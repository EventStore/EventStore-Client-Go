package esdb

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gossipApi "github.com/EventStore/EventStore-Client-Go/v2/protos/gossip"
	server_features "github.com/EventStore/EventStore-Client-Go/v2/protos/serverfeatures"
	"github.com/EventStore/EventStore-Client-Go/v2/protos/shared"
	"github.com/gofrs/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcClient struct {
	channel   chan msg
	closeFlag *int32
	once      *sync.Once
	logger    *logger
}

func (client *grpcClient) handleError(handle *connectionHandle, headers metadata.MD, trailers metadata.MD, err error) error {
	values := trailers.Get("exception")

	if values != nil && values[0] == "not-leader" {
		hostValues := trailers.Get("leader-endpoint-host")
		portValues := trailers.Get("leader-endpoint-port")

		if hostValues != nil && portValues != nil {
			host := hostValues[0]
			port, err := strconv.Atoi(portValues[0])

			if err == nil {
				endpoint := EndPoint{
					Host: host,
					Port: uint16(port),
				}

				msg := reconnect{
					correlation: handle.Id(),
					endpoint:    &endpoint,
				}

				client.channel <- msg
				client.logger.error("not leader exception, reconnecting to %v", endpoint)
				return &Error{code: ErrorNotLeader}
			}
		}
	}

	if values != nil && values[0] == "stream-deleted" {
		streamName := trailers.Get("stream-name")[0]
		return &Error{code: ErrorStreamDeleted, err: fmt.Errorf("stream '%s' is deleted", streamName)}
	}

	code := errToCode(err)

	if code != ErrorUnknown {
		return &Error{code: code}
	}

	client.logger.error("unexpected exception: %v", err)

	msg := reconnect{
		correlation: handle.Id(),
	}

	client.channel <- msg

	return err
}

func (client *grpcClient) getConnectionHandle() (*connectionHandle, error) {
	if atomic.LoadInt32(client.closeFlag) != 0 {
		return nil, &Error{
			code: ErrorConnectionClosed,
			err:  fmt.Errorf("connection is closed"),
		}
	}

	msg := newGetConnectionMsg()
	client.channel <- msg

	resp := <-msg.channel

	return &resp, resp.err
}

func (client *grpcClient) close() {
	client.once.Do(func() {
		atomic.StoreInt32(client.closeFlag, 1)
		close(client.channel)
	})
}

type getConnection struct {
	channel chan connectionHandle
}

func (msg getConnection) isMsg() {}

func newGetConnectionMsg() getConnection {
	return getConnection{
		channel: make(chan connectionHandle),
	}
}

type connectionState struct {
	correlation uuid.UUID
	connection  *grpc.ClientConn
	serverInfo  *ServerInfo
	config      Configuration
	lastError   error
}

func newConnectionState(config Configuration) connectionState {
	return connectionState{
		correlation: uuid.Nil,
		connection:  nil,
		config:      config,
		lastError:   nil,
	}
}

type msg interface {
	isMsg()
}

type connectionHandle struct {
	id         uuid.UUID
	connection *grpc.ClientConn
	serverInfo *ServerInfo
	err        error
}

func (handle *connectionHandle) Id() uuid.UUID {
	return handle.id
}

func (handle *connectionHandle) Connection() *grpc.ClientConn {
	return handle.connection
}

func (handle *connectionHandle) SupportsFeature(feature int) bool {
	if handle.serverInfo != nil {
		return handle.serverInfo.FeatureFlags&feature != 0
	}

	return false
}

func newErroredConnectionHandle(err error) connectionHandle {
	return connectionHandle{
		id:  uuid.Nil,
		err: err,
	}
}

func newConnectionHandle(id uuid.UUID, serverInfo *ServerInfo, connection *grpc.ClientConn) connectionHandle {
	return connectionHandle{
		id:         id,
		connection: connection,
		serverInfo: serverInfo,
	}
}

func connectionStateMachine(config Configuration, closeFlag *int32, channel chan msg, logger *logger) {
	state := newConnectionState(config)

	for {
		msg, ok := <-channel

		if !ok {
			if state.connection != nil {
				err := state.connection.Close()

				if err != nil {
					logger.warn("error when closing gRPC connection. %v", err)
				}
			}

			return
		}

		switch evt := msg.(type) {
		case getConnection:
			{
				// Means we need to create a grpc connection.
				if state.correlation == uuid.Nil {
					conn, serverInfo, err := discoverNode(state.config, logger)

					if err != nil {
						atomic.StoreInt32(closeFlag, 1)
						state.lastError = err
						resp := newErroredConnectionHandle(err)
						evt.channel <- resp
						close(evt.channel)
						return
					}

					state.correlation = uuid.Must(uuid.NewV4())
					state.connection = conn
					state.serverInfo = serverInfo

					resp := newConnectionHandle(state.correlation, serverInfo, conn)
					evt.channel <- resp
					close(evt.channel)
				} else {
					handle := connectionHandle{
						id:         state.correlation,
						connection: state.connection,
						serverInfo: state.serverInfo,
						err:        nil,
					}

					evt.channel <- handle
					close(evt.channel)
				}
			}
		case reconnect:
			if evt.correlation == state.correlation {
				if evt.endpoint == nil {
					// Means that in the next iteration cycle, the discovery process will start.
					state.correlation = uuid.Nil
					logger.info("starting a new discovery process")
					continue
				}

				if state.connection != nil {
					state.connection.Close()
					state.connection = nil
				}

				logger.info("Connecting to leader node %s ...", evt.endpoint.String())
				conn, err := createGrpcConnection(&state.config, evt.endpoint.String())

				if err != nil {
					logger.error("exception when connecting to suggested node %s", evt.endpoint.String())
					state.correlation = uuid.Nil
					continue
				}

				serverInfo, err := getSupportedMethods(context.Background(), &state.config, conn)
				if err != nil {
					logger.error("exception when fetching server features from suggested node %s: %v", evt.endpoint.String(), err)
					state.correlation = uuid.Nil
					continue
				}

				state.correlation = uuid.Must(uuid.NewV4())
				state.connection = conn
				state.serverInfo = serverInfo

				logger.info("successfully connected to leader node %s", evt.endpoint.String())
			}
		}
	}
}

type reconnect struct {
	correlation uuid.UUID
	endpoint    *EndPoint
}

func (msg reconnect) isMsg() {}

func createGrpcConnection(conf *Configuration, address string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if conf.DisableTLS {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts,
			grpc.WithTransportCredentials(credentials.NewTLS(
				&tls.Config{
					InsecureSkipVerify: conf.SkipCertificateVerification,
					RootCAs:            conf.RootCAs,
				})))
	}

	opts = append(opts, grpc.WithPerRPCCredentials(basicAuth{
		username: conf.Username,
		password: conf.Password,
	}))

	if conf.KeepAliveInterval >= 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                conf.KeepAliveInterval,
			Timeout:             conf.KeepAliveTimeout,
			PermitWithoutStream: true,
		}))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize connection to %s. Reason: %w", address, err)
	}

	return conn, nil
}

type ServerVersion struct {
	Major int
	Minor int
	Patch int
}

const (
	FEATURE_NOTHING                                   = 0
	FEATURE_BATCH_APPEND                              = 1
	FEATURE_PERSISTENT_SUBSCRIPTION_LIST              = 2
	FEATURE_PERSISTENT_SUBSCRIPTION_REPLAY            = 4
	FEATURE_PERSISTENT_SUBSCRIPTION_RESTART_SUBSYSTEM = 8
	FEATURE_PERSISTENT_SUBSCRIPTION_GET_INFO          = 16
	FEATURE_PERSISTENT_SUBSCRIPTION_TO_ALL            = 32
	FEATURE_PERSISTENT_SUBSCRIPTION_MANAGEMENT        = FEATURE_PERSISTENT_SUBSCRIPTION_LIST | FEATURE_PERSISTENT_SUBSCRIPTION_GET_INFO | FEATURE_PERSISTENT_SUBSCRIPTION_RESTART_SUBSYSTEM | FEATURE_PERSISTENT_SUBSCRIPTION_REPLAY
)

type ServerInfo struct {
	Version      ServerVersion
	FeatureFlags int
}

func getSupportedMethods(ctx context.Context, conf *Configuration, conn *grpc.ClientConn) (*ServerInfo, error) {
	client := server_features.NewServerFeaturesClient(conn)
	newCtx, cancel := context.WithTimeout(ctx, time.Duration(conf.GossipTimeout)*time.Second)
	defer cancel()
	methods, err := client.GetSupportedMethods(newCtx, &shared.Empty{})

	s, ok := status.FromError(err)

	if !ok || (s != nil && s.Code() != codes.OK) {
		if s.Code() == codes.Unimplemented || s.Code() == codes.NotFound {
			return nil, nil
		}

		return nil, err
	}

	info := ServerInfo{
		Version:      ServerVersion{},
		FeatureFlags: FEATURE_NOTHING,
	}

	for idx, value := range strings.Split(methods.EventStoreServerVersion, ".") {
		if idx > 2 {
			break
		}

		num, err := strconv.Atoi(value)

		if err != nil {
			return nil, fmt.Errorf("invalid EventStoreDB server version format: %w", err)
		}

		switch idx {
		case 0:
			info.Version.Major = num
		case 1:
			info.Version.Minor = num
		default:
			info.Version.Patch = num
		}
	}

	for _, method := range methods.Methods {
		switch method.ServiceName {
		case "event_store.client.streams.streams":
			if method.MethodName == "batchappend" {
				info.FeatureFlags |= FEATURE_BATCH_APPEND
			}
		case "event_store.client.persistent_subscriptions.persistentsubscriptions":
			switch method.MethodName {
			case "create":
				for _, feat := range method.Features {
					if feat == "all" {
						info.FeatureFlags |= FEATURE_PERSISTENT_SUBSCRIPTION_TO_ALL
					}
				}
			case "getinfo":
				info.FeatureFlags |= FEATURE_PERSISTENT_SUBSCRIPTION_GET_INFO
			case "replayparked":
				info.FeatureFlags |= FEATURE_PERSISTENT_SUBSCRIPTION_REPLAY
			case "list":
				info.FeatureFlags |= FEATURE_PERSISTENT_SUBSCRIPTION_LIST
			case "restartsubsystem":
				info.FeatureFlags |= FEATURE_PERSISTENT_SUBSCRIPTION_RESTART_SUBSYSTEM
			default:
			}
		default:
		}
	}

	return &info, nil
}

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(tx context.Context, in ...string) (map[string]string, error) {
	auth := b.username + ":" + b.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{
		"Authorization": "Basic " + enc,
	}, nil
}

func (basicAuth) RequireTransportSecurity() bool {
	return false
}
func allowedNodeState() []gossipApi.MemberInfo_VNodeState {
	return []gossipApi.MemberInfo_VNodeState{
		gossipApi.MemberInfo_Follower,
		gossipApi.MemberInfo_Leader,
		gossipApi.MemberInfo_ReadOnlyLeaderless,
		gossipApi.MemberInfo_PreReadOnlyReplica,
		gossipApi.MemberInfo_ReadOnlyReplica,
	}
}

func discoverNode(conf Configuration, logger *logger) (*grpc.ClientConn, *ServerInfo, error) {
	var connection *grpc.ClientConn = nil
	var serverInfo *ServerInfo = nil
	var err error
	var candidates []string

	attempt := 0
	// We still need to keep tracking that state until 20.10 end of life, as GossipOnSingleNode is still present in that
	// version.
	clusterMode := false

	if conf.DnsDiscover {
		clusterMode = true
		candidates = append(candidates, conf.Address)
	} else if len(conf.GossipSeeds) > 0 {
		clusterMode = true
		for _, seed := range conf.GossipSeeds {
			candidates = append(candidates, seed.String())
		}
	} else {
		candidates = append(candidates, conf.Address)
	}

	if clusterMode {
		shuffleCandidates(candidates)
	}

	for attempt < conf.MaxDiscoverAttempts {
		attempt += 1
		logger.info("discovery attempt %v/%v", attempt, conf.MaxDiscoverAttempts)
		for _, candidate := range candidates {
			logger.debug("trying candidate '%s'...", candidate)
			connection, err = createGrpcConnection(&conf, candidate)
			if err != nil {
				logger.warn("error when creating a grpc connection for candidate %s: %v", candidate, err)

				continue
			}

			if clusterMode {
				client := gossipApi.NewGossipClient(connection)
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(conf.GossipTimeout)*time.Second)
				info, err := client.Read(ctx, &shared.Empty{})

				s, ok := status.FromError(err)
				if !ok || (s != nil && s.Code() != codes.OK) {
					logger.warn("error when reading gossip from candidate %s: %v", candidate, err)
					cancel()
					continue
				}

				cancel()
				info.Members = shuffleMembers(info.Members)
				selected, err := pickBestCandidate(info, conf.NodePreference)

				if err != nil {
					logger.warn("error when picking best candidate out of %s gossip response: %v", candidate, err)
					continue
				}

				selectedAddress := fmt.Sprintf("%s:%d", selected.GetHttpEndPoint().GetAddress(), selected.GetHttpEndPoint().GetPort())
				logger.info("best candidate found. %s (%s)", selectedAddress, selected.State.String())
				if candidate != selectedAddress {
					candidate = selectedAddress
					_ = connection.Close()
					connection, err = createGrpcConnection(&conf, selectedAddress)

					if err != nil {
						logger.warn("error when creating gRPC connection for the selected candidate '%s': %v", selectedAddress, err)
						continue
					}
				}
			}

			logger.debug("attempting node supported features retrieval on '%s'...", candidate)
			serverInfo, err = getSupportedMethods(context.Background(), &conf, connection)
			if err != nil {
				logger.warn("error when creating reading server features from the best candidate '%s': %v", candidate, err)
				_ = connection.Close()
				connection = nil
				continue
			}

			if serverInfo != nil {
				logger.debug("retrieved supported features on node '%s' successfully", candidate)
			} else {
				logger.debug("selected node '%s' doesn't support a supported features endpoint", candidate)
			}

			break
		}

		if connection != nil {
			break
		}

		time.Sleep(time.Duration(conf.DiscoveryInterval) * time.Millisecond)
	}

	if connection == nil {
		return nil, nil, &Error{
			code: errToCode(err),
			err:  fmt.Errorf("maximum discovery attempt count reached: %v. Last Error: %w", conf.MaxDiscoverAttempts, err),
		}
	}

	return connection, serverInfo, nil
}

// In that case, `err` is always != nil
func errToCode(err error) ErrorCode {
	var code ErrorCode
	switch status.Code(err) {
	case codes.Unauthenticated:
		code = ErrorUnauthenticated
	case codes.Unimplemented:
		code = ErrorUnsupportedFeature
	case codes.NotFound:
		code = ErrorResourceNotFound
	case codes.PermissionDenied:
		code = ErrorAccessDenied
	case codes.DeadlineExceeded:
		code = ErrorDeadlineExceeded
	case codes.AlreadyExists:
		code = ErrorResourceAlreadyExists
	default:
		code = ErrorUnknown
	}

	return code
}

func shuffleCandidates(src []string) []string {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(src), func(i, j int) {
		src[i], src[j] = src[j], src[i]
	})

	return src
}

func shuffleMembers(src []*gossipApi.MemberInfo) []*gossipApi.MemberInfo {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(src), func(i, j int) {
		src[i], src[j] = src[j], src[i]
	})

	return src
}

func sortByState(members []*gossipApi.MemberInfo, nodeState gossipApi.MemberInfo_VNodeState) []*gossipApi.MemberInfo {
	sorted := make([]*gossipApi.MemberInfo, 0)
	for _, member := range members {
		if member.State == nodeState {
			sorted = append(sorted, member)
		}
	}
	for _, member := range members {
		if member.State != nodeState {
			sorted = append(sorted, member)
		}
	}
	return sorted
}

func pickBestCandidate(response *gossipApi.ClusterInfo, nodePreference NodePreference) (*gossipApi.MemberInfo, error) {
	if len(response.Members) == 0 {
		return nil, fmt.Errorf("there are no members to determine the best candidate from")
	}
	allowedMembers := make([]*gossipApi.MemberInfo, 0)
	for _, member := range response.Members {
		for _, allowedState := range allowedNodeState() {
			if member.State == allowedState && member.GetIsAlive() {
				allowedMembers = append(allowedMembers, member)
			}
		}
	}
	if len(allowedMembers) == 0 {
		return nil, fmt.Errorf("no nodes are eligable to be a candidate")
	}
	switch nodePreference {
	case NodePreference_Leader:
		{
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_Leader)
		}
	case NodePreference_Follower:
		{
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_Follower)
		}
	case NodePreference_ReadOnlyReplica:
		{
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_ReadOnlyReplica)
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_PreReadOnlyReplica)
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_ReadOnlyLeaderless)
		}
	}
	return allowedMembers[0], nil
}
