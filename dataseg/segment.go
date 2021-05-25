package main

type SegmentID int

type Segment interface {
	ID() SegmentID     // returns a unique id for this segment used for storage
	Encoded() []byte   // produces encoded value of this segment for storage
	Load([]byte) error // loads the segment using the byte slice provided
}

type SegmentProvider interface {
	GetSegment(id SegmentID) Segment
	AddSegment(seg Segment)
	RemoveSegment(seg Segment)
}

// think of it as ledger
type BasicSegmentProvider struct {
	segments map[SegmentID]Segment
}

func NewBasicSegmentProvider() *BasicSegmentProvider {
	return &BasicSegmentProvider{segments: make(map[SegmentID]Segment)}
}

func (s *BasicSegmentProvider) GetSegment(id SegmentID) Segment {
	return s.segments[id]
}

func (s *BasicSegmentProvider) AddSegment(seg Segment) {
	s.segments[seg.ID()] = seg
}

func (s *BasicSegmentProvider) RemoveSegment(seg Segment) {
	delete(s.segments, seg.ID())
}
