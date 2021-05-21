package main

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
