package main

import (
	"bytes"
	"fmt"
	"math"
)

// ArrayItem holds anything that has to be stored in array
type ArrayItem interface {
	Index() uint32
	Encoded() []byte
	Size() uint32
}

// EmptyArrayItem is returned when value not found
type EmptyArrayItem struct{}

func (EmptyArrayItem) Index() uint32   { return 0 }
func (EmptyArrayItem) Encoded() []byte { return nil }
func (EmptyArrayItem) Size() uint32    { return 0 }

// ByteArrayItem is an array item that holds only a single byte value, mostly used for testing
type ByteArrayItem struct {
	index uint32
	value byte
}

func (b ByteArrayItem) Index() uint32   { return b.index }
func (b ByteArrayItem) Encoded() []byte { return []byte{} } // TODO implement this
func (b ByteArrayItem) Size() uint32    { return 4 + 1 }

type ArraySegment struct {
	id        SegmentID
	totalSize uint32
	elements  []ArrayItem
}

func NewArraySegment(id SegmentID) *ArraySegment {
	return &ArraySegment{
		id:        id,
		totalSize: 0,
		elements:  make([]ArrayItem, 0),
	}
}

func (a *ArraySegment) StartIndex() uint32 {
	if len(a.elements) < 1 {
		return 0
	}
	return a.elements[0].Index()
}

func (a *ArraySegment) LastIndex() uint32 {
	if len(a.elements) < 1 {
		return 0
	}
	return a.elements[len(a.elements)-1].Index()
}

func (a *ArraySegment) GetItem(index uint32) (data ArrayItem, found bool) {
	if index < a.StartIndex() {
		return EmptyArrayItem{}, false
	}
	// TODO we might not need this at all
	if index > a.LastIndex() {
		return EmptyArrayItem{}, false
	}
	for _, e := range a.elements {
		// if already exist replace
		if index == e.Index() {
			return e, true
		}
	}
	return EmptyArrayItem{}, false
}

func (a *ArraySegment) AddItem(s ArrayItem) {
	// we can insert more than max size
	if s.Size() > maxItemSize {
		return
	}
	if s.Index() < a.StartIndex() {
		// prepend
		newElms := make([]ArrayItem, 1)
		newElms[0] = s
		a.elements = append(newElms, a.elements...)
		a.totalSize += s.Size()
		return
	}
	if s.Index() > a.LastIndex() {
		// append
		a.elements = append(a.elements, s)
		a.totalSize += s.Size()
		return
	}
	for i, e := range a.elements {
		// if already exist replace
		if s.Index() == e.Index() {
			a.elements[i] = s
			a.totalSize = a.totalSize - e.Size() + s.Size()
			break
		}
		// we already passed it, insert it
		if s.Index() < e.Index() {
			newElems := make([]ArrayItem, len(a.elements)+1)
			copy(newElems[:i], a.elements[:i])
			newElems[i] = s
			copy(newElems[i+1:], a.elements[i:])
			a.elements = newElems
			a.totalSize += s.Size()
			break
		}
	}
}

func (a *ArraySegment) RemoveItem(index uint32) {
	for i, e := range a.elements {
		if index < e.Index() {
			// not found, early break
			break
		}
		if index == e.Index() {
			newElems := make([]ArrayItem, len(a.elements)-1)
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
	// this compute the ceil of split keep the first part with more members (optimized for append operations)
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

func (a ArraySegment) Encoded() []byte {
	// TODO update me and properly implement me
	// right now we just append bytes
	res := make([]byte, 0)
	for _, e := range a.elements {
		res = append(res, e.Encoded()...)
	}
	return res
}

func (a ArraySegment) Load([]byte) error {
	// TODO implement me
	return nil
}

func (a *ArraySegment) Value() []ArrayItem {
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

func (a ArrayMetaSegment) Encoded() []byte {
	// TODO implement me
	return []byte{}
}

func (a ArrayMetaSegment) Load([]byte) error {
	// TODO implement me
	return nil
}

type Array struct {
	metaSegmentID SegmentID
	sp            SegmentProvider
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

func FetchArray(metaSegmentID SegmentID, sp SegmentProvider) *Array {
	return &Array{
		sp:            sp,
		metaSegmentID: metaSegmentID,
	}
}

func NewArray(sp SegmentProvider) *Array {
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
		if inpIndex == segH.startIndex {
			return i
		}
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

func (a *Array) Insert(inp ArrayItem) {
	// TODO handle insert if size of storable is bigger than threshold
	mseg := a.ArrayMetaSegment()
	segIndex := a.FindSegmentIndex(inp.Index())
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	aseg := seg.(*ArraySegment)
	oldSize := aseg.totalSize
	aseg.AddItem(inp)

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
	aseg.RemoveItem(index)
	mseg.size = mseg.size - oldSize + aseg.totalSize
	if aseg.totalSize < minThreshold {
		if len(mseg.sortedSegHeaders) > 1 { // if only one segment don't merge
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
			return
		}
	}
	a.sp.AddSegment(mseg)
	a.sp.AddSegment(aseg)
}

func (a *Array) AppendByteArrayItem(v uint8) {
	mseg := a.ArrayMetaSegment()
	segIndex := len(mseg.sortedSegHeaders) - 1 // get last header
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	aseg := seg.(*ArraySegment)
	inp := ByteArrayItem{aseg.LastIndex() + 1, byte(v)}
	oldSize := aseg.totalSize
	aseg.AddItem(inp)
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

func (a *Array) ValidateCorrectness(expectedValues []byte) bool {
	allValues := make([]byte, 0)
	mseg := a.ArrayMetaSegment()
	previousIndex := uint32(0)
	for _, segH := range mseg.sortedSegHeaders {
		segValues := make([]byte, 0)
		totalSegSize := uint32(0)
		seg := a.sp.GetSegment(segH.segID).(*ArraySegment)

		for _, elem := range seg.elements {
			if elem.Index() < previousIndex {
				fmt.Println("index sequence is wrong")
				return false
			}

			segValues = append(segValues, elem.Encoded()...)
			totalSegSize += elem.Size()
			previousIndex = elem.Index()
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
