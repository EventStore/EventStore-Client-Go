package stream_position

import pos "github.com/EventStore/EventStore-Client-Go/position"

type Revision struct {
	Value uint64
}

type Position struct {
	Value pos.Position
}

type Start struct {
}

type End struct {
}

type StreamPosition interface {
	AcceptRegularVisitor(visitor RegularStreamVisitor)
}

type AllStreamPosition interface {
	AcceptAllVisitor(visitor AllStreamVisitor)
}

type RegularStreamVisitor interface {
	VisitRevision(value uint64)
	VisitStart()
	VisitEnd()
}

type AllStreamVisitor interface {
	VisitPosition(value pos.Position)
	VisitStart()
	VisitEnd()
}

func (r Revision) AcceptRegularVisitor(visitor RegularStreamVisitor) {
	visitor.VisitRevision(r.Value)
}

func (r Position) AcceptAllVisitor(visitor AllStreamVisitor) {
	visitor.VisitPosition(r.Value)
}

func (r Start) AcceptRegularVisitor(visitor RegularStreamVisitor) {
	visitor.VisitStart()
}

func (r End) AcceptRegularVisitor(visitor RegularStreamVisitor) {
	visitor.VisitEnd()
}

func (r Start) AcceptAllVisitor(visitor AllStreamVisitor) {
	visitor.VisitStart()
}

func (r End) AcceptAllVisitor(visitor AllStreamVisitor) {
	visitor.VisitEnd()
}
