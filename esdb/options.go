package esdb

import (
	"context"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type operationKind int

const (
	regularOperation operationKind = iota
	streamingOperation
)

type options interface {
	kind() operationKind
	credentials() *Credentials
	deadline() *time.Duration
	requiresLeader() bool
}

func configureGrpcCall(ctx context.Context, conf *Configuration, options options, grpcOptions []grpc.CallOption, perRPCCredentials credentials.PerRPCCredentials) ([]grpc.CallOption, context.Context, context.CancelFunc) {
	var duration time.Duration

	if options.deadline() != nil {
		duration = *options.deadline()
	} else if options.kind() != streamingOperation && conf.DefaultDeadline != nil {
		duration = *conf.DefaultDeadline
	} else if options.kind() == streamingOperation {
		duration = time.Duration(math.MaxInt64)
	} else {
		duration = 10 * time.Second
	}

	deadline := time.Now().Add(duration)
	newCtx, cancel := context.WithDeadline(ctx, deadline)

	if options.credentials() != nil {
		perRPCCredentials = NewBasicPerCallAuth(options.credentials().Login, options.credentials().Password)
	}
	if perRPCCredentials != nil {
		grpcOptions = append(grpcOptions, grpc.PerRPCCredsCallOption{Creds: perRPCCredentials})
	}

	if options.requiresLeader() || conf.NodePreference == NodePreferenceLeader {
		md := metadata.New(map[string]string{"requires-leader": "true"})
		newCtx = metadata.NewIncomingContext(newCtx, md)
	}

	return grpcOptions, newCtx, cancel
}
