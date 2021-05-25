package main

import (
	"fmt"
)

const minThreshold = 10
const maxThreshold = 20
const maxItemSize = 6

func arrayExample() {
	sp := NewBasicSegmentProvider()
	aa := NewArray(sp)
	fmt.Println("append to loc 0")
	aa.AppendByteArrayItem(1)
	aa.Print()
	fmt.Println("append to loc 1")
	aa.AppendByteArrayItem(2)
	aa.Print()
	fmt.Println("replace loc 0")
	aa.Insert(ByteArrayItem{uint32(1), byte(4)}) // index is 1
	aa.Print()
	fmt.Println("append to loc 2")
	aa.AppendByteArrayItem(5)
	aa.Print()
	fmt.Println("append to loc 3")
	aa.AppendByteArrayItem(7)
	aa.Print()
	fmt.Println("append to loc 4 and split")
	aa.AppendByteArrayItem(9)
	aa.Print()
	fmt.Println("replace loc 2")
	aa.Insert(ByteArrayItem{uint32(3), byte(0)}) // index is 3
	aa.Print()
	fmt.Println("replace loc 4")
	aa.Insert(ByteArrayItem{uint32(5), byte(0)})
	aa.Print()
	fmt.Println("remove item at loc 3, and merge")
	aa.Remove(4) // index 2
	aa.Print()
	fmt.Println("no op")
	aa.Remove(4)
	aa.Print()
	fmt.Println("add item to index 4 (loc 3) and split")
	aa.Insert(ByteArrayItem{uint32(4), byte(5)})
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
	aa.Insert(ByteArrayItem{uint32(2), byte(2)})
	aa.Insert(ByteArrayItem{uint32(4), byte(4)})
	aa.Insert(ByteArrayItem{uint32(6), byte(6)})
	aa.Insert(ByteArrayItem{uint32(8), byte(8)})
	aa.Insert(ByteArrayItem{uint32(10), byte(10)})
	aa.Print()
	arrayID := aa.metaSegmentID
	bb := FetchArray(arrayID, sp)
	bb.Print()
}

func mapExample() {
	sp := NewBasicSegmentProvider()
	mm := NewMap(sp)
	mm.Insert(StringMapItem{"A", "AAAA"})
	mm.Print()
	mm.Insert(StringMapItem{"B", "BBB"})
	mm.Print()
	mm.Insert(StringMapItem{"D", "DDDD"})
	mm.Print()
	mm.Insert(StringMapItem{"A", "AAAAA"})
	mm.Print()
	mm.Insert(StringMapItem{"C", "CC"})
	mm.Print()
	mm.Insert(StringMapItem{"F", "FFFF"})
	mm.Print()
	mm.Get("H")
	mm.Get("A")
}

func main() {
	// arrayExample()
	mapExample()
}

// TODO add equal functionaity to create a list of values and compare it to an array
// so we can have test with randomize updates

// TODO add benchmarking on delays
// add proper testing to each componenet
