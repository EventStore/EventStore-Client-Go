package client

import api "github.com/eventstore/EventStore-Client-Go/protos"

// WriteResult ...
type WriteResult struct {
	CommitPosition      uint64
	PreparePosition     uint64
	NextExpectedVersion uint64
}

// WriteResultFromAppendResp ...
func WriteResultFromAppendResp(appendResponse *api.AppendResp) *WriteResult {
	position := appendResponse.GetPosition()
	if position != nil {
		return &WriteResult{
			CommitPosition:      position.GetCommitPosition(),
			PreparePosition:     position.GetPreparePosition(),
			NextExpectedVersion: appendResponse.GetCurrentRevision(),
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: appendResponse.GetCurrentRevision(),
	}
}
