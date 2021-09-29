package operations

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type Client interface {
	Admin
	Scavenger
}

type Scavenger interface {
	StartScavenge(ctx context.Context, request StartScavengeRequest) (ScavengeResponse, errors.Error)
	StopScavenge(ctx context.Context, scavengeId string) (ScavengeResponse, errors.Error)
}

type Admin interface {
	Shutdown(ctx context.Context) errors.Error
	MergeIndexes(ctx context.Context) errors.Error
	ResignNode(ctx context.Context) errors.Error
	SetNodePriority(ctx context.Context, priority int32) errors.Error
	RestartPersistentSubscriptions(ctx context.Context) errors.Error
}
