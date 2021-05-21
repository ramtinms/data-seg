package main

import (
	"bytes"
	"fmt"
	"math"
)

const minThreshold = 10
const maxThreshold = 20

var counter int = 0

func generateUUID() SegmentID {
	counter++
	return SegmentID(counter)
}

type SegmentID int

type Segment interface {
	ID() SegmentID
}

// think of it as ledger
type SegmentProvider struct {
	segments map[SegmentID]Segment
}

func NewSegmentProvider() *SegmentProvider {
	return &SegmentProvider{segments: make(map[SegmentID]Segment, 0)}
}

func (s *SegmentProvider) GetSegment(id SegmentID) Segment {
	return s.segments[id]
}

func (s *SegmentProvider) AddSegment(seg Segment) {
	s.segments[seg.ID()] = seg
}

func (s *SegmentProvider) RemoveSegment(seg Segment) {
	delete(s.segments, seg.ID())
}

// TODO change this later to generic
type Storable struct {
	index uint32
	value byte
}

// TODO make this dynamic
func (e Storable) Size() uint32 {
	return 4 + 1
}

type ArraySegment struct {
	id        SegmentID
	totalSize uint32
	elements  []Storable
}

func NewArraySegment(id SegmentID) *ArraySegment {
	return &ArraySegment{
		id:        id,
		totalSize: 0,
		elements:  make([]Storable, 0),
	}
}

func (a *ArraySegment) StartIndex() uint32 {
	if len(a.elements) < 1 {
		return 0
	}
	return a.elements[0].index
}

func (a *ArraySegment) LastIndex() uint32 {
	if len(a.elements) < 1 {
		return 0
	}
	return a.elements[len(a.elements)-1].index
}

func (a *ArraySegment) GetStorable(index uint32) (data Storable, found bool) {
	if index < a.StartIndex() {
		return Storable{}, false
	}
	// TODO we might not need this at all
	if index > a.LastIndex() {
		return Storable{}, false
	}
	for _, e := range a.elements {
		// if already exist replace
		if index == e.index {
			return e, true
		}
	}
	return Storable{}, false
}

func (a *ArraySegment) AddStorable(s Storable) {
	if s.index < a.StartIndex() {
		// prepend
		newElms := make([]Storable, 1)
		newElms[0] = s
		a.elements = append(newElms, a.elements...)
		a.totalSize += s.Size()
		return
	}
	if s.index > a.LastIndex() {
		// append
		a.elements = append(a.elements, s)
		a.totalSize += s.Size()
		return
	}
	for i, e := range a.elements {
		// if already exist replace
		if s.index == e.index {
			a.elements[i] = s
			a.totalSize = a.totalSize - e.Size() + s.Size()
			break
		}
		// we already passed it, insert it
		if s.index < e.index {
			newElems := make([]Storable, len(a.elements)+1)
			copy(newElems[:i], a.elements[:i])
			newElems[i] = s
			copy(newElems[i+1:], a.elements[i:])
			a.elements = newElems
			a.totalSize += s.Size()
			break
		}
	}
}

func (a *ArraySegment) RemoveStorable(index uint32) {
	for i, e := range a.elements {
		if index < e.index {
			// not found, early break
			break
		}
		if index == e.index {
			newElems := make([]Storable, len(a.elements)-1)
			if i == 0 {
				copy(newElems, a.elements[1:])
			} else {
				copy(newElems, a.elements[:i])
				if i < len(a.elements) {
					copy(newElems[i:], a.elements[i+1:])
				}
			}
			a.elements = newElems
			a.totalSize = a.totalSize - e.Size()
			break
		}
	}
}

func (a *ArraySegment) Split() (seg2 *ArraySegment) {
	// TODO change this logic to act based on the size of values (if hetro)
	// this compute the ceil of split keep the first part with more members (optimized for aditions)
	d := float64(len(a.elements)) / float64(2)
	breakPoint := int(math.Ceil(d))

	newSeg := NewArraySegment(generateUUID())
	newSeg.elements = a.elements[breakPoint:len(a.elements)]
	newSegSize := uint32(0)
	for _, e := range newSeg.elements {
		newSegSize += e.Size()
	}
	newSeg.totalSize = newSegSize
	a.elements = a.elements[:breakPoint]
	a.totalSize = a.totalSize - newSegSize
	return newSeg
}

func (a *ArraySegment) Merge(seg2 *ArraySegment) {
	a.elements = append(a.elements, seg2.elements...)
	a.totalSize = a.totalSize + seg2.totalSize
}

func (a ArraySegment) ID() SegmentID {
	return a.id
}

func (a *ArraySegment) Value() []Storable {
	return a.elements
}

func (a *ArraySegment) Header() ArraySegmentHeader {
	return ArraySegmentHeader{
		startIndex: a.StartIndex(),
		size:       a.totalSize,
		segID:      a.id,
	}
}

type ArraySegmentHeader struct {
	startIndex uint32
	size       uint32
	segID      SegmentID
}

type ArrayMetaSegment struct {
	id               SegmentID
	sortedSegHeaders []ArraySegmentHeader
	size             uint32
}

func (a ArrayMetaSegment) ID() SegmentID {
	return a.id
}

type Array struct {
	metaSegmentID SegmentID
	sp            *SegmentProvider
}

// Print is intended for debugging purpose only
func (a *Array) Print() {
	fmt.Println("============= array ================")
	mseg := a.ArrayMetaSegment()
	for _, segH := range mseg.sortedSegHeaders {
		seg := a.sp.GetSegment(segH.segID)
		fmt.Println(seg)
	}
	fmt.Println("====================================")
}

func (a *Array) ValidateCorrectness(expectedValues []byte) bool {
	allValues := make([]byte, 0)
	mseg := a.ArrayMetaSegment()
	previousIndex := uint32(0)
	for _, segH := range mseg.sortedSegHeaders {
		segValues := make([]byte, 0)
		totalSegSize := uint32(0)
		seg := a.sp.GetSegment(segH.segID).(*ArraySegment)

		for _, elem := range seg.elements {
			if elem.index < previousIndex {
				fmt.Println("index sequence is wrong")
				return false
			}

			segValues = append(segValues, elem.value)
			totalSegSize += elem.Size()
			previousIndex = elem.index
		}
		if totalSegSize != seg.totalSize {
			fmt.Println("total size is wrong")
			return false
		}

		allValues = append(allValues, segValues...)
	}

	if !bytes.Equal(allValues, expectedValues) {
		fmt.Println("bytes not equal")
		return false
	}

	return true
}

func FetchArray(metaSegmentID SegmentID, sp *SegmentProvider) *Array {
	return &Array{
		sp:            sp,
		metaSegmentID: metaSegmentID,
	}
}

func NewArray(sp *SegmentProvider) *Array {
	sp1 := NewArraySegment(generateUUID())
	sp.AddSegment(sp1)
	metaSegID := generateUUID()
	metaSeg := &ArrayMetaSegment{
		id:               metaSegID,
		sortedSegHeaders: []ArraySegmentHeader{sp1.Header()},
		size:             0,
	}
	sp.AddSegment(metaSeg)
	return &Array{metaSegID, sp}
}

func (a *Array) ArrayMetaSegment() *ArrayMetaSegment {
	return a.sp.GetSegment(a.metaSegmentID).(*ArrayMetaSegment)
}

func (a *Array) FindSegmentIndex(inpIndex uint32) int {
	// TODO optimize this read and pass it as param
	mseg := a.ArrayMetaSegment()
	for i, segH := range mseg.sortedSegHeaders {
		if inpIndex < segH.startIndex {
			if i == 0 {
				return 0
			}
			// TODO look at sizes and decide (optimize)
			return i - 1
		}
	}
	return len(mseg.sortedSegHeaders) - 1
}

func (a *Array) Insert(inp Storable) {
	// TODO handle insert if size of storable is bigger than threshold
	mseg := a.ArrayMetaSegment()
	segIndex := a.FindSegmentIndex(inp.index)
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	aseg := seg.(*ArraySegment)
	oldSize := aseg.totalSize
	aseg.AddStorable(inp)

	if aseg.totalSize > maxThreshold {
		s2 := aseg.Split()
		mseg.sortedSegHeaders[segIndex] = aseg.Header()
		// last one
		if segIndex == len(mseg.sortedSegHeaders)-1 {
			mseg.sortedSegHeaders = append(mseg.sortedSegHeaders, s2.Header())
		} else {
			newSortedHeaders := mseg.sortedSegHeaders[:segIndex]
			newSortedHeaders = append(newSortedHeaders, s2.Header())
			mseg.sortedSegHeaders = append(newSortedHeaders, mseg.sortedSegHeaders[segIndex:len(mseg.sortedSegHeaders)]...)
		}
		a.sp.AddSegment(s2)
	}
	mseg.size = mseg.size - oldSize + aseg.totalSize
	a.sp.AddSegment(mseg)
	a.sp.AddSegment(aseg)
}

func (a *Array) Remove(index uint32) {
	mseg := a.ArrayMetaSegment()
	segIndex := a.FindSegmentIndex(index)
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	aseg := seg.(*ArraySegment)
	oldSize := aseg.totalSize
	aseg.RemoveStorable(index)
	mseg.size = mseg.size - oldSize + aseg.totalSize
	if aseg.totalSize < minThreshold {
		if len(mseg.sortedSegHeaders) > 1 { // if only on segment don't merge
			if segIndex == 0 { // first segment
				nextSeg := a.sp.GetSegment(mseg.sortedSegHeaders[1].segID).(*ArraySegment)
				if nextSeg.totalSize+aseg.totalSize <= maxThreshold {
					aseg.Merge(nextSeg)
					newSortedHeaders := mseg.sortedSegHeaders[:1]
					newSortedHeaders[0] = aseg.Header()
					if len(mseg.sortedSegHeaders) > 2 {
						newSortedHeaders = append(newSortedHeaders, mseg.sortedSegHeaders[2:]...)
					}
					mseg.sortedSegHeaders = newSortedHeaders
					a.sp.RemoveSegment(nextSeg)
					a.sp.AddSegment(aseg)
					a.sp.AddSegment(mseg)
				}
			} else if segIndex == len(mseg.sortedSegHeaders)-1 { // last segment
				prevSeg := a.sp.GetSegment(mseg.sortedSegHeaders[segIndex-1].segID).(*ArraySegment)
				if prevSeg.totalSize+aseg.totalSize <= maxThreshold {
					prevSeg.Merge(aseg)
					newSortedHeaders := mseg.sortedSegHeaders[:segIndex]
					newSortedHeaders[segIndex-1] = prevSeg.Header()
					mseg.sortedSegHeaders = newSortedHeaders
					a.sp.RemoveSegment(aseg)
					a.sp.AddSegment(prevSeg)
					a.sp.AddSegment(mseg)
				}
			} else if mseg.sortedSegHeaders[segIndex-1].size > mseg.sortedSegHeaders[segIndex+1].size {
				nextSeg := a.sp.GetSegment(mseg.sortedSegHeaders[1].segID).(*ArraySegment)
				if nextSeg.totalSize+aseg.totalSize <= maxThreshold {
					aseg.Merge(nextSeg)
					newSortedHeaders := mseg.sortedSegHeaders[:segIndex+1]
					newSortedHeaders[segIndex] = aseg.Header()
					if len(mseg.sortedSegHeaders) > segIndex+2 {
						newSortedHeaders = append(newSortedHeaders, mseg.sortedSegHeaders[segIndex+2:]...)
					}
					mseg.sortedSegHeaders = newSortedHeaders
					a.sp.RemoveSegment(nextSeg)
					a.sp.AddSegment(aseg)
					a.sp.AddSegment(mseg)
				}
			} else {
				prevSeg := a.sp.GetSegment(mseg.sortedSegHeaders[1].segID).(*ArraySegment)
				if prevSeg.totalSize+aseg.totalSize <= maxThreshold {
					prevSeg.Merge(aseg)
					newSortedHeaders := mseg.sortedSegHeaders[:segIndex]
					newSortedHeaders[segIndex-1] = prevSeg.Header()
					if len(mseg.sortedSegHeaders) > segIndex+1 {
						newSortedHeaders = append(newSortedHeaders, mseg.sortedSegHeaders[segIndex+1:]...)
					}
					mseg.sortedSegHeaders = newSortedHeaders
					a.sp.RemoveSegment(aseg)
					a.sp.AddSegment(prevSeg)
					a.sp.AddSegment(mseg)
				}
			}
		}
	}
}

func (a *Array) Append(v uint8) {
	mseg := a.ArrayMetaSegment()
	segIndex := len(mseg.sortedSegHeaders) - 1 // get last header
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	aseg := seg.(*ArraySegment)
	inp := Storable{aseg.LastIndex() + 1, byte(v)}
	oldSize := aseg.totalSize
	aseg.AddStorable(inp)
	mseg.size = mseg.size - oldSize + aseg.totalSize

	if aseg.totalSize > maxThreshold {
		s2 := aseg.Split()
		mseg.sortedSegHeaders[segIndex] = aseg.Header()
		mseg.sortedSegHeaders = append(mseg.sortedSegHeaders, s2.Header())
		a.sp.AddSegment(s2)
	}

	a.sp.AddSegment(mseg)
	a.sp.AddSegment(aseg)
}

func main() {
	sp := NewSegmentProvider()
	aa := NewArray(sp)
	fmt.Println("append to loc 0")
	aa.Append(1)
	aa.Print()
	fmt.Println("append to loc 1")
	aa.Append(2)
	aa.Print()
	fmt.Println("replace loc 0")
	aa.Insert(Storable{uint32(1), byte(4)}) // index is 1
	aa.Print()
	fmt.Println("append to loc 2")
	aa.Append(5)
	aa.Print()
	fmt.Println("append to loc 3")
	aa.Append(7)
	aa.Print()
	fmt.Println("append to loc 4 and split")
	aa.Append(9)
	aa.Print()
	fmt.Println("replace loc 2")
	aa.Insert(Storable{uint32(3), byte(0)}) // index is 3
	aa.Print()
	fmt.Println("replace loc 4")
	aa.Insert(Storable{uint32(5), byte(0)})
	aa.Print()
	fmt.Println("remove item at loc 3, and merge")
	aa.Remove(4) // index 2
	aa.Print()
	fmt.Println("no op")
	aa.Remove(4)
	aa.Print()
	fmt.Println("add item to index 4 (loc 3) and split")
	aa.Insert(Storable{uint32(4), byte(5)})
	aa.Print()
	fmt.Println(aa.ValidateCorrectness([]byte{4, 2, 0, 5, 0}))
	fmt.Println("remove several")
	aa.Remove(1)
	aa.Remove(2)
	aa.Remove(3)
	aa.Print()
	fmt.Println("remove rest")
	aa.Remove(4)
	aa.Remove(5)
	aa.Print()
	fmt.Println("add some values")
	aa.Insert(Storable{uint32(2), byte(2)})
	aa.Insert(Storable{uint32(4), byte(4)})
	aa.Insert(Storable{uint32(6), byte(6)})
	aa.Insert(Storable{uint32(8), byte(8)})
	aa.Insert(Storable{uint32(10), byte(10)})
	aa.Print()
	arrayID := aa.metaSegmentID
	bb := FetchArray(arrayID, sp)
	bb.Print()
}

// TODO add equal functionaity to create a list of values and compare it to an array
// so we can have test with randomize updates

// TODO add benchmarking on delays
// add proper testing to each componenet
