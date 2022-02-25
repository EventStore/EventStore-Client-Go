package esdb

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	gossipApi "github.com/EventStore/EventStore-Client-Go/protos/gossip"
	server_features "github.com/EventStore/EventStore-Client-Go/protos/serverfeatures"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/gofrs/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcClient struct {
	channel chan msg
}

func (client *grpcClient) handleError(handle connectionHandle, headers metadata.MD, trailers metadata.MD, err error) error {
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
				log.Printf("[error] Not leader exception, reconnecting to %v", endpoint)
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

	log.Printf("[error] unexpected exception: %v", err)

	msg := reconnect{
		correlation: handle.Id(),
	}

	client.channel <- msg

	return err
}

func (client *grpcClient) getConnectionHandle() (connectionHandle, error) {
	msg := newGetConnectionMsg()
	client.channel <- msg

	resp := <-msg.channel

	return resp, resp.err
}

func (client *grpcClient) close() {
	channel := make(chan bool)
	client.channel <- closeConnection{channel}
	<-channel
}

type getConnection struct {
	channel chan connectionHandle
}

func newGetConnectionMsg() getConnection {
	return getConnection{
		channel: make(chan connectionHandle),
	}
}

func (msg getConnection) handle(state *connectionState) {
	// Means we need to create a grpc connection.
	if state.correlation == uuid.Nil {
		conn, serverInfo, err := discoverNode(state.config)

		if err != nil {
			state.lastError = err
			resp := newErroredConnectionHandle(err)
			msg.channel <- resp
			return
		}

		id, err := uuid.NewV4()

		if err != nil {
			state.lastError = fmt.Errorf("error when trying to generate a random UUID: %v", err)
			return
		}

		state.correlation = id
		state.connection = conn
		state.serverInfo = serverInfo

		resp := newConnectionHandle(id, serverInfo, conn)
		msg.channel <- resp
	} else {
		handle := connectionHandle{
			id:         state.correlation,
			connection: state.connection,
			serverInfo: state.serverInfo,
			err:        nil,
		}

		msg.channel <- handle
	}
}

type connectionState struct {
	correlation uuid.UUID
	connection  *grpc.ClientConn
	serverInfo  *ServerInfo
	config      Configuration
	lastError   error
	closed      bool
}

func newConnectionState(config Configuration) connectionState {
	return connectionState{
		correlation: uuid.Nil,
		connection:  nil,
		config:      config,
		lastError:   nil,
		closed:      false,
	}
}

type msg interface {
	handle(*connectionState)
}

type connectionHandle struct {
	id         uuid.UUID
	connection *grpc.ClientConn
	serverInfo *ServerInfo
	err        error
}

func (handle connectionHandle) Id() uuid.UUID {
	return handle.id
}

func (handle connectionHandle) Connection() *grpc.ClientConn {
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

func connectionStateMachine(config Configuration, channel chan msg) {
	state := newConnectionState(config)

	for {
		msg := <-channel

		if state.closed {
			switch evt := msg.(type) {
			case getConnection:
				{
					evt.channel <- connectionHandle{
						err: &Error{
							code: ErrorConnectionClosed,
						},
					}
				}
			case closeConnection:
				{
					evt.channel <- true
				}
			default:
				// No-op
			}
			continue
		}

		msg.handle(&state)
	}
}

type reconnect struct {
	correlation uuid.UUID
	endpoint    *EndPoint
}

func (msg reconnect) handle(state *connectionState) {
	if msg.correlation == state.correlation {
		if msg.endpoint == nil {
			// Means that in the next iteration cycle, the discovery process will start.
			state.correlation = uuid.Nil
			log.Printf("[info] Starting a new discovery process")
			return
		}

		log.Printf("[info] Connecting to leader node %s ...", msg.endpoint.String())
		conn, err := createGrpcConnection(&state.config, msg.endpoint.String())

		if err != nil {
			log.Printf("[error] exception when connecting to suggested node %s", msg.endpoint.String())
			state.correlation = uuid.Nil
			return
		}

		serverInfo, err := getSupportedMethods(context.Background(), &state.config, conn)
		if err != nil {
			log.Printf("[error] exception when fetching server features from suggested node %s: %v", msg.endpoint.String(), err)
			state.correlation = uuid.Nil
			return
		}

		id, err := uuid.NewV4()

		if err != nil {
			log.Printf("[error] exception when generating a correlation id after reconnected to %s : %v", msg.endpoint.String(), err)
			state.correlation = uuid.Nil
			return
		}

		state.correlation = id
		state.connection = conn
		state.serverInfo = serverInfo

		log.Printf("[info] Successfully connected to leader node %s", msg.endpoint.String())
	}
}

type closeConnection struct {
	channel chan bool
}

func (msg closeConnection) handle(state *connectionState) {
	state.closed = true
	if state.connection != nil {
		defer func() {
			_ = state.connection.Close()
			state.connection = nil
		}()
	}

	msg.channel <- true
}

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

func discoverNode(conf Configuration) (*grpc.ClientConn, *ServerInfo, error) {
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
		log.Printf("[info] discovery attempt %v/%v", attempt, conf.MaxDiscoverAttempts)
		for _, candidate := range candidates {
			log.Printf("[debug] trying candidate '%s'...", candidate)
			connection, err = createGrpcConnection(&conf, candidate)
			if err != nil {
				log.Printf("[warn] Error when creating a grpc connection for candidate %s: %v", candidate, err)

				continue
			}

			if clusterMode {
				client := gossipApi.NewGossipClient(connection)
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(conf.GossipTimeout)*time.Second)
				info, err := client.Read(ctx, &shared.Empty{})

				s, ok := status.FromError(err)
				if !ok || (s != nil && s.Code() != codes.OK) {
					log.Printf("[warn] Error when reading gossip from candidate %s: %v", candidate, err)
					cancel()
					continue
				}

				cancel()
				info.Members = shuffleMembers(info.Members)
				selected, err := pickBestCandidate(info, conf.NodePreference)

				if err != nil {
					log.Printf("[warn] Eror when picking best candidate out of %s gossip response: %v", candidate, err)
					continue
				}

				selectedAddress := fmt.Sprintf("%s:%d", selected.GetHttpEndPoint().GetAddress(), selected.GetHttpEndPoint().GetPort())
				log.Printf("[info] Best candidate found. %s (%s)", selectedAddress, selected.State.String())
				if candidate != selectedAddress {
					candidate = selectedAddress
					_ = connection.Close()
					connection, err = createGrpcConnection(&conf, selectedAddress)

					if err != nil {
						log.Printf("[warn] Error when creating gRPC connection for the selected candidate '%s': %v", selectedAddress, err)
						continue
					}
				}
			}

			log.Printf("[debug] Attempting node supported features retrieval on '%s'...", candidate)
			serverInfo, err = getSupportedMethods(context.Background(), &conf, connection)
			if err != nil {
				log.Printf("[warn] Error when creating reading server features from the best candidate '%s': %v", candidate, err)
				_ = connection.Close()
				connection = nil
				continue
			}

			if serverInfo != nil {
				log.Printf("[debug] Retrieved supported features on node '%s' successfully", candidate)
			} else {
				log.Printf("[debug] Selected node '%s' doesn't support a supported features endpoint", candidate)
			}

			break
		}

		if connection != nil {
			break
		}
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
