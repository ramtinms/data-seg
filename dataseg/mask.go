package main

import "fmt"

// TODO handle large size keys and expansion
// maybe enforce max keys size

type Mask struct {
	index uint32 // number of bits active as mask
	bytes []byte // TODO replace me with a bitset or a fixed size bytes
}

func NewAcceptAllMask() Mask {
	return Mask{
		index: 0,
		bytes: make([]byte, 32),
	}
}

func NewSplitMasks(parent Mask, inp1 string, inp2 string) (Mask, Mask) {
	// find first bit diff between inp1 and inp2

	index := uint32(findLastCommonBit(inp1, inp2))
	leftMask := Mask{index: index + 1,
		bytes: make([]byte, 32)}
	copy(leftMask.bytes, parent.bytes)

	rightMask := Mask{index: index + 1,
		bytes: make([]byte, 32)}

	copy(rightMask.bytes, parent.bytes)
	SetBit(rightMask.bytes, int(parent.index))
	return leftMask, rightMask
}

func (m Mask) IsMember(inp string) bool {
	// if index is zero all values are member
	if m.index == 0 {
		return true
	}
	for i := 0; i < int(m.index); i++ {
		if Bit(m.bytes, i) != Bit([]byte(inp), i) {
			return false
		}
	}
	return true
}

func (m Mask) Print() {
	fmt.Printf("mask: ")
	for i := 0; i < int(m.index); i++ {
		if Bit(m.bytes, i) == 1 {
			fmt.Printf("1")
		} else {
			fmt.Printf("0")
		}
	}
	fmt.Printf("\n")

}

func findLastCommonBit(inp1 string, inp2 string) int {
	// TODO deal with variable sizes
	for i := 0; i < len(inp1)*8; i++ {
		if Bit([]byte(inp1), i) != Bit([]byte(inp2), i) {
			// TODO i == 0 should never happen
			return i - 1
		}
	}
	return len(inp1) * 8
}
