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

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	gossipApi "github.com/pivonroll/EventStore-Client-Go/protos/gossip"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
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

const (
	protoStreamDeleted             = "stream-deleted"
	protoStreamNotFound            = "stream-not-found"
	protoMaximumAppendSizeExceeded = "maximum-append-size-exceeded"
	protoWrongExpectedVersion      = "wrong-expected-version"
	protoNotLeader                 = "not-leader"
	protoUserNotFound              = "user-not-found"
)

func isProtoException(trailers metadata.MD, protoException string) bool {
	values := trailers.Get("exception")
	return values != nil && values[0] == protoException
}

func ErrorFromStdErrorByStatus(err error) errors.Error {
	protoStatus, _ := status.FromError(err)
	if protoStatus.Code() == codes.PermissionDenied {
		return errors.NewError(errors.PermissionDeniedErr, err)
	} else if protoStatus.Code() == codes.Unauthenticated {
		return errors.NewError(errors.UnauthenticatedErr, err)
	} else if protoStatus.Code() == codes.DeadlineExceeded {
		return errors.NewError(errors.DeadlineExceededErr, err)
	} else if protoStatus.Code() == codes.Canceled {
		return errors.NewError(errors.CanceledErr, err)
	}
	return nil
}

func GetErrorFromProtoException(trailers metadata.MD, stdErr error) errors.Error {
	if isProtoException(trailers, protoStreamDeleted) {
		return errors.NewError(errors.StreamDeletedErr, stdErr)
	} else if isProtoException(trailers, protoMaximumAppendSizeExceeded) {
		return errors.NewError(errors.MaximumAppendSizeExceededErr, stdErr)
	} else if isProtoException(trailers, protoStreamNotFound) {
		return errors.NewError(errors.StreamNotFoundErr, stdErr)
	} else if isProtoException(trailers, protoWrongExpectedVersion) {
		return errors.NewError(errors.WrongExpectedStreamRevisionErr, stdErr)
	} else if isProtoException(trailers, protoNotLeader) {
		return errors.NewError(errors.NotLeaderErr, stdErr)
	} else if isProtoException(trailers, protoUserNotFound) {
		return errors.NewError(errors.UserNotFoundErr, stdErr)
	}

	err := ErrorFromStdErrorByStatus(stdErr)
	if err != nil {
		return err
	}

	return nil
}

func (client grpcClientImpl) HandleError(
	handle ConnectionHandle,
	_header metadata.MD,
	trailers metadata.MD,
	stdErr error,
	mapUnknownErrorToOtherError ...errors.ErrorCode) errors.Error {

	err := GetErrorFromProtoException(trailers, stdErr)
	if err != nil {
		if err.Code() == errors.NotLeaderErr {
			hostValues := trailers.Get("leader-endpoint-host")
			portValues := trailers.Get("leader-endpoint-port")

			if hostValues != nil && portValues != nil {
				host := hostValues[0]
				port, aToIErr := strconv.Atoi(portValues[0])

				if aToIErr == nil {
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
					return err
				}
			}
		} else {
			return err
		}
	}

	log.Printf("[error] unexpected exception: %v", stdErr)

	msg := reconnect{
		correlation: handle.Id(),
	}

	client.channel <- msg

	if mapUnknownErrorToOtherError != nil && len(mapUnknownErrorToOtherError) == 1 {
		return errors.NewError(mapUnknownErrorToOtherError[0], stdErr)
	}

	return errors.NewError(errors.UnknownErr, stdErr)
}

func (client grpcClientImpl) GetConnectionHandle() (ConnectionHandle, errors.Error) {
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

const UUIDGeneratingError errors.ErrorCode = "UUIDGeneratingError"

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

		id, stdErr := uuid.NewV4()
		if stdErr != nil {
			state.lastError = errors.NewErrorCodeMsg(UUIDGeneratingError,
				fmt.Sprintf("error when trying to generate a random UUID: %v", err))
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
	lastError   errors.Error
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
	err        errors.Error
}

func (handle connectionHandle) Id() uuid.UUID {
	return handle.id
}

func (handle connectionHandle) Connection() *grpc.ClientConn {
	return handle.connection
}

func newErroredConnectionHandle(err errors.Error) connectionHandle {
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

const EsdbConnectionIsClosed errors.ErrorCode = "EsdbConnectionIsClosed"

func connectionStateMachine(config Configuration, channel chan msg) {
	state := newConnectionState(config)

	for {
		msg := <-channel

		if state.closed {
			switch evt := msg.(type) {
			case getConnection:
				{
					evt.channel <- connectionHandle{
						err: errors.NewErrorCode(EsdbConnectionIsClosed),
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

const (
	MaximumDiscoveryAttemptCountReached errors.ErrorCode = "MaximumDiscoveryAttemptCountReached"
	UnableToConnectToSingleNode         errors.ErrorCode = "UnableToConnectToSingleNode"
)

func discoverNode(conf Configuration) (*grpc.ClientConn, errors.Error) {
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

		return nil, errors.NewErrorCode(MaximumDiscoveryAttemptCountReached)

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
			return nil, errors.NewErrorCodeMsg(UnableToConnectToSingleNode,
				fmt.Sprintf("unable to connect to single node %s", conf.Address))
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
