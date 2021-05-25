package main

import (
	"fmt"
	"math"
)

// MapItem holds anything that has to be stored in a map
type MapItem interface {
	Key() string
	Encoded() []byte
	Size() uint32 // including key and value
}

// EmptyMapItem is returned when value not found
type EmptyMapItem struct{}

func (EmptyMapItem) Key() string     { return "" }
func (EmptyMapItem) Encoded() []byte { return nil }
func (EmptyMapItem) Size() uint32    { return 0 }

// StringMapItem is a map item that holds only a single string value
type StringMapItem struct {
	key   string
	value string
}

func (s StringMapItem) Key() string     { return s.key }
func (s StringMapItem) Encoded() []byte { return []byte{} } // TODO implement this
func (s StringMapItem) Size() uint32    { return uint32(len(s.key) + len(s.value)) }

// TODO encode should do sorted keys, we might need to keep sorted keys

type MapSegment struct {
	id        SegmentID
	totalSize uint32
	mask      Mask               // Mask captures valid prefixes for this segment
	keys      []string           // keeps ordered keys
	lookup    map[string]MapItem // lookup is used for fast lookup for get
}

func NewMapSegment(id SegmentID) *MapSegment {
	return &MapSegment{
		id:        id,
		totalSize: 0,
		mask:      NewAcceptAllMask(),
		keys:      make([]string, 0),
		lookup:    make(map[string]MapItem, 0),
	}
}

func (a *MapSegment) FirstKey() string {
	if len(a.keys) < 1 {
		return ""
	}
	return a.keys[0]
}

func (a *MapSegment) LastKey() string {
	if len(a.keys) < 1 {
		return ""
	}
	return a.keys[len(a.keys)-1]
}

func (a *MapSegment) GetItem(key string) (data MapItem, found bool) {
	data, ok := a.lookup[key]
	return data, ok
}

func (a *MapSegment) AddItem(s MapItem) {
	// we can insert more than max size
	// TODO return error
	if s.Size() > maxItemSize {
		return
	}
	// this should never happen but lets keep it for sanity check for now
	if !a.mask.IsMember(s.Key()) {
		fmt.Println("NOT A MEMBER !!!!")
		return
	}

	if s.Key() < a.FirstKey() {
		// prepend
		newKeys := make([]string, 1)
		newKeys[0] = s.Key()
		a.keys = append(newKeys, a.keys...)
		a.totalSize += s.Size()
		a.lookup[string(s.Key())] = s
		return
	}
	if s.Key() > a.LastKey() {
		// append
		a.keys = append(a.keys, s.Key())
		a.totalSize += s.Size()
		a.lookup[string(s.Key())] = s
		return
	}
	for i, e := range a.keys {
		// if already exist replace
		if s.Key() == e {
			oldSize := a.lookup[string(s.Key())].Size()
			a.totalSize = a.totalSize - oldSize + s.Size()
			a.lookup[string(s.Key())] = s
			break
		}
		// we already passed it, insert it
		if s.Key() < e {
			newKeys := make([]string, len(a.keys)+1)
			copy(newKeys[:i], a.keys[:i])
			newKeys[i] = s.Key()
			copy(newKeys[i+1:], a.keys[i:])
			a.keys = newKeys
			a.totalSize += s.Size()
			a.lookup[string(s.Key())] = s
			break
		}
	}

}

func (a *MapSegment) RemoveItem(key string) {
	for i, e := range a.keys {
		if key < e {
			// not found, early break
			break
		}
		if key == e {
			newKeys := make([]string, len(a.keys)-1)
			if i == 0 {
				copy(newKeys, a.keys[1:])
			} else {
				copy(newKeys, a.keys[:i])
				if i < len(a.keys) {
					copy(newKeys[i:], a.keys[i+1:])
				}
			}
			a.keys = newKeys
			oldSize := a.lookup[string(key)].Size()
			a.totalSize = a.totalSize - oldSize
			break
		}
	}
	delete(a.lookup, key)
}

func (a *MapSegment) Split() (seg2 *MapSegment) {
	// TODO change this logic to act based on the size of values (if hetro)
	// TODO deal with very large values
	d := float64(len(a.lookup)) / float64(2)
	breakPoint := int(math.Ceil(d))

	newSeg := NewMapSegment(generateUUID())
	newSeg.keys = a.keys[breakPoint:len(a.keys)]
	for _, e := range newSeg.keys {
		item := a.lookup[e]
		newSeg.lookup[e] = item
		delete(a.lookup, e)
		a.totalSize = a.totalSize - item.Size()
		newSeg.totalSize += item.Size()

	}

	m1, m2 := NewSplitMasks(a.mask, a.LastKey(), newSeg.keys[0])
	newSeg.mask = m2
	a.mask = m1
	a.keys = a.keys[:breakPoint]
	return newSeg
}

func (a *MapSegment) Merge(seg2 *MapSegment) {
	if a.mask.index != seg2.mask.index {
		fmt.Println("Warning merging segments that are not parallel")
		return
	}
	a.mask.index = uint32(findLastCommonBit(a.FirstKey(), a.LastKey()))
	a.keys = append(a.keys, seg2.keys...)
	for k, v := range seg2.lookup {
		a.lookup[k] = v
	}
	a.totalSize = a.totalSize + seg2.totalSize
}

func (a MapSegment) ID() SegmentID {
	return a.id
}

func (a MapSegment) Encoded() []byte {
	// TODO implement me
	return []byte{}
}

func (a MapSegment) Load([]byte) error {
	// TODO implement me
	return nil
}

func (a *MapSegment) Header() MapSegmentHeader {
	return MapSegmentHeader{
		mask:  a.mask,
		size:  a.totalSize,
		segID: a.id,
	}
}

type MapSegmentHeader struct {
	mask  Mask
	size  uint32
	segID SegmentID
}

type MapMetaSegment struct {
	id               SegmentID
	sortedSegHeaders []MapSegmentHeader
	size             uint32
}

func (a MapMetaSegment) ID() SegmentID {
	return a.id
}

func (a MapMetaSegment) Encoded() []byte {
	// TODO implement me
	return []byte{}
}

func (a MapMetaSegment) Load([]byte) error {
	// TODO implement me
	return nil
}

type Map struct {
	metaSegmentID SegmentID
	sp            SegmentProvider
}

// TODO add keys method and back it up with an array, has functionality should be part of map
// arraySegments

// Print is intended for debugging purpose only
func (a *Map) Print() {
	fmt.Println("============= array ================")
	mseg := a.MapMetaSegment()
	for _, segH := range mseg.sortedSegHeaders {
		seg := a.sp.GetSegment(segH.segID)
		fmt.Println(seg)
	}
	fmt.Println("====================================")
}

func FetchMap(metaSegmentID SegmentID, sp SegmentProvider) *Map {
	return &Map{
		sp:            sp,
		metaSegmentID: metaSegmentID,
	}
}

func NewMap(sp SegmentProvider) *Map {
	sp1 := NewMapSegment(generateUUID())
	sp.AddSegment(sp1)
	metaSegID := generateUUID()
	metaSeg := &MapMetaSegment{
		id:               metaSegID,
		sortedSegHeaders: []MapSegmentHeader{sp1.Header()},
		size:             0,
	}
	sp.AddSegment(metaSeg)
	return &Map{metaSegID, sp}
}

func (a *Map) MapMetaSegment() *MapMetaSegment {
	return a.sp.GetSegment(a.metaSegmentID).(*MapMetaSegment)
}

func (a *Map) FindSegmentIndex(key string) int {
	// TODO optimize this read and pass it as param
	mseg := a.MapMetaSegment()
	for i, segH := range mseg.sortedSegHeaders {
		if segH.mask.IsMember(key) {
			return i
		}
	}
	// this should never happen
	return 0
}

func (a *Map) Insert(inp MapItem) {
	// TODO handle insert if size of storable is bigger than threshold
	mseg := a.MapMetaSegment()
	segIndex := a.FindSegmentIndex(inp.Key())
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	// todo rename aseg
	aseg := seg.(*MapSegment)
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

func (a *Map) Get(key string) (res MapItem, found bool) {
	mseg := a.MapMetaSegment()
	segIndex := a.FindSegmentIndex(key)
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID).(*MapSegment)
	return seg.GetItem(key)
}

func (a *Map) Remove(key string) {
	mseg := a.MapMetaSegment()
	segIndex := a.FindSegmentIndex(key)
	segH := mseg.sortedSegHeaders[segIndex]
	seg := a.sp.GetSegment(segH.segID)
	aseg := seg.(*MapSegment)
	oldSize := aseg.totalSize
	aseg.RemoveItem(key)
	mseg.size = mseg.size - oldSize + aseg.totalSize
	if aseg.totalSize < minThreshold {
		if len(mseg.sortedSegHeaders) > 1 { // if only one segment don't merge
			if segIndex%2 == 0 { // even segment
				nextSeg := a.sp.GetSegment(mseg.sortedSegHeaders[segIndex+1].segID).(*MapSegment)
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
			} else { // odd segment
				prevSeg := a.sp.GetSegment(mseg.sortedSegHeaders[segIndex-1].segID).(*MapSegment)
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
