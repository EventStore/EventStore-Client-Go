package esdb

import (
	"context"
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

	return grpcOptions, newCtx, cancel
}
