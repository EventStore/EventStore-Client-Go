package esdb

import (
	"context"
	"google.golang.org/grpc/metadata"
	"math"
	"time"

	"google.golang.org/grpc"
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

func configureGrpcCall(ctx context.Context, conf *Configuration, options options, grpcOptions []grpc.CallOption) ([]grpc.CallOption, context.Context, context.CancelFunc) {
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
		grpcOptions = append(grpcOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.credentials().Login,
			password: options.credentials().Password,
		}))
	}

	if options.requiresLeader() || conf.NodePreference == NodePreferenceLeader {
		md := metadata.New(map[string]string{"requires-leader": "true"})
		newCtx = metadata.NewIncomingContext(newCtx, md)
	}

	return grpcOptions, newCtx, cancel
}
