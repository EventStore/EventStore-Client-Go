package connection

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/EventStore/EventStore-Client-Go/errors"
	gossipApi "github.com/EventStore/EventStore-Client-Go/protos/gossip"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/gofrs/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcClientImpl struct {
	channel chan msg
}

func (client grpcClientImpl) HandleError(handle ConnectionHandle, headers metadata.MD, trailers metadata.MD, err error) error {
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
				log.Printf("[error] Not leader exception occurred")
				return fmt.Errorf("not leader exception")
			}
		}
	}

	log.Printf("[error] unexpected exception: %v", err)

	status, _ := status.FromError(err)
	if status.Code() == codes.FailedPrecondition { // Precondition -> ErrWrongExpectedStremRevision
		return fmt.Errorf("%w, reason: %s", errors.ErrWrongExpectedStreamRevision, err.Error())
	}
	if status.Code() == codes.PermissionDenied { // PermissionDenied -> ErrPemissionDenied
		return fmt.Errorf("%w", errors.ErrPermissionDenied)
	}
	if status.Code() == codes.Unauthenticated { // PermissionDenied -> ErrUnauthenticated
		return fmt.Errorf("%w", errors.ErrUnauthenticated)
	}

	msg := reconnect{
		correlation: handle.Id(),
	}

	client.channel <- msg

	return err
}

func (client grpcClientImpl) GetConnectionHandle() (ConnectionHandle, error) {
	msg := newGetConnectionMsg()
	client.channel <- msg

	resp := <-msg.channel

	return resp, resp.err
}

func (client grpcClientImpl) Close() {
	channel := make(chan bool)
	client.channel <- close{channel}
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
		conn, err := discoverNode(state.config)

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

		resp := newConnectionHandle(id, conn)
		msg.channel <- resp
	} else {
		handle := connectionHandle{
			id:         state.correlation,
			connection: state.connection,
			err:        nil,
		}

		msg.channel <- handle
	}
}

type connectionState struct {
	correlation uuid.UUID
	connection  *grpc.ClientConn
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
	err        error
}

func (handle connectionHandle) Id() uuid.UUID {
	return handle.id
}

func (handle connectionHandle) Connection() *grpc.ClientConn {
	return handle.connection
}

func newErroredConnectionHandle(err error) connectionHandle {
	return connectionHandle{
		id:         uuid.Nil,
		connection: nil,
		err:        err,
	}
}

func newConnectionHandle(id uuid.UUID, connection *grpc.ClientConn) connectionHandle {
	return connectionHandle{
		id:         id,
		connection: connection,
		err:        nil,
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
						err: fmt.Errorf("esdb connection is closed"),
					}
				}
			case close:
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

		id, err := uuid.NewV4()

		if err != nil {
			log.Printf("[error] exception when generating a correlation id after reconnected to %s : %v", msg.endpoint.String(), err)
			state.correlation = uuid.Nil
			return
		}

		state.correlation = id
		state.connection = conn

		log.Printf("[info] Successfully connected to leader node %s", msg.endpoint.String())
	}
}

type close struct {
	channel chan bool
}

func (msg close) handle(state *connectionState) {
	state.closed = true
	if state.connection != nil {
		defer func() {
			state.connection.Close()
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
		return nil, fmt.Errorf("Failed to initialize connection to %+v. Reason: %v", conf, err)
	}

	return conn, nil
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

func discoverNode(conf Configuration) (*grpc.ClientConn, error) {
	var connection *grpc.ClientConn = nil
	attempt := 1

	if conf.DnsDiscover || len(conf.GossipSeeds) > 0 {
		var candidates []string

		if conf.DnsDiscover {
			candidates = append(candidates, conf.Address)
		} else {
			for _, seed := range conf.GossipSeeds {
				candidates = append(candidates, seed.String())
			}
		}

		shuffleCandidates(candidates)

		for attempt <= conf.MaxDiscoverAttempts {
			log.Printf("[info] discovery attempt %v/%v", attempt, conf.MaxDiscoverAttempts)
			for _, candidate := range candidates {
				log.Printf("[info] Attempting to gossip via %s", candidate)
				connection, err := createGrpcConnection(&conf, candidate)
				if err != nil {
					log.Printf("[warn] Error when creating a grpc connection for candidate %s", candidate)

					continue
				}

				client := gossipApi.NewGossipClient(connection)
				context, cancel := context.WithTimeout(context.Background(), time.Duration(conf.GossipTimeout)*time.Second)
				defer cancel()
				info, err := client.Read(context, &shared.Empty{})

				if err != nil {
					log.Printf("[warn] Error when reading gossip from candidate %s: %v", candidate, err)
					continue
				}

				info.Members = shuffleMembers(info.Members)
				selected, err := pickBestCandidate(info, conf.NodePreference)

				if err != nil {
					log.Printf("[warn] Eror when picking best candidate out of %s gossip response: %v", candidate, err)
					continue
				}

				selectedAddress := fmt.Sprintf("%s:%d", selected.GetHttpEndPoint().GetAddress(), selected.GetHttpEndPoint().GetPort())
				log.Printf("[info] Best candidate found. %s (%s)", selectedAddress, selected.State.String())

				if candidate != selectedAddress {
					connection, err = createGrpcConnection(&conf, selectedAddress)

					if err != nil {
						log.Printf("[warn] Error when creating gRPC connection for best candidate %v", err)
						continue
					}
				}

				log.Printf("[info] Successfully connected to best candidate %s (%s)", selectedAddress, selected.State.String())

				return connection, nil
			}

			attempt += 1
			time.Sleep(time.Duration(conf.DiscoveryInterval))
		}

		return nil, fmt.Errorf("maximum discovery attempt count reached")

	} else {
		for attempt <= conf.MaxDiscoverAttempts {
			grpcConn, err := createGrpcConnection(&conf, conf.Address)

			if err == nil {
				connection = grpcConn
				break
			}

			log.Printf("[warn] error when creating a single node connection to %s", conf.Address)

			attempt += 1
			time.Sleep(time.Duration(conf.DiscoveryInterval))
		}

		if connection == nil {
			return nil, fmt.Errorf("unable to connect to single node %s", conf.Address)
		}
	}

	return connection, nil
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
