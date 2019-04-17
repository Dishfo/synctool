package syncfile

import (
	"bytes"
	"fmt"
)


var pc [256]byte

const (
	//OsBit is os arch
	OsBit = 32 << (^uint(0) >> 63)
)

//!+intset

// An IntSet is a set of small non-negative integers.
// Its zero value represents the empty set.
type IntSet struct {
	words []uint
}

func init() {
	for i := range pc {
		pc[i] = pc[i/2] + byte(i&1)
	}
}

func newIntSet() *IntSet{
	iset := new(IntSet)
	iset.words = make([]uint,0)
	return iset
}

func popCount(x uint) int {
	if OsBit == 32 {
		return popCount32(uint32(x))
	} else {
		return popCount64(uint64(x))
	}
}

func popCount64(x uint64) int {
	return int(
		pc[byte(x>>(0*8))] +
			pc[byte(x>>(1*8))] +
			pc[byte(x>>(2*8))] +
			pc[byte(x>>(3*8))] +
			pc[byte(x>>(4*8))] +
			pc[byte(x>>(5*8))] +
			pc[byte(x>>(6*8))] +
			pc[byte(x>>(7*8))])
}

func popCount32(x uint32) int {
	return int(
		pc[byte(x>>(0*8))] +
			pc[byte(x>>(1*8))] +
			pc[byte(x>>(2*8))] +
			pc[byte(x>>(3*8))])
}

// Has reports whether the set contains the non-negative value x.
func (s *IntSet) Has(x int) bool {
	word, bit := x/OsBit, uint(x%64)
	return word < len(s.words) && s.words[word]&(1<<bit) != 0
}

// Add adds the non-negative value x to the set.
func (s *IntSet) Add(x int) {
	word, bit := x/OsBit, uint(x%64)
	for word >= len(s.words) {
		s.words = append(s.words, 0)
	}
	s.words[word] |= 1 << bit
}

// UnionWith sets s to the union of s and t.
func (s *IntSet) UnionWith(t *IntSet) {
	for i, tword := range t.words {
		if i < len(s.words) {
			s.words[i] |= tword
		} else {
			s.words = append(s.words, tword)
		}
	}
}

//!-intset

//!+string

// String returns the set as a string of the form "{1 2 3}".
func (s *IntSet) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, word := range s.words {
		if word == 0 {
			continue
		}
		for j := 0; j < 64; j++ {
			if word&(1<<uint(j)) != 0 {
				if buf.Len() > len("{") {
					buf.WriteByte(' ')
				}
				_, _ = fmt.Fprintf(&buf, "%d", 64*i+j)
			}
		}
	}
	buf.WriteByte('}')
	return buf.String()
}

//Len return the number of intset 's element
func (s *IntSet) Len() int {
	var slen = 0
	for _, v := range s.words {
		slen += popCount(v)
	}
	return slen
}

//Remove will remove on x bit
func (s *IntSet) Remove(x int) {
	word, bit := x/64, uint(x%64)

	if word < len(s.words) {
		s.words[word] &= ^(1 << bit)
	}
}

//Clear will reset all bit
func (s *IntSet) Clear() {
	for i := range s.words {
		s.words[i] = 0
	}
}

//Copy will copy all bits  to a new set
func (s *IntSet) Copy() *IntSet {
	var intSet = new(IntSet)
	if s.words != nil {
		intSet.words = append(intSet.words, s.words...)
	}
	return intSet
}

//AddAll will add a int array
func (s *IntSet) AddAll(vars ...int) {
	for _, v := range vars {
		s.Add(v)
	}
}

//ELem return a uint array contain all coping of element
func (s *IntSet) ELem() []uint {
	var words = make([]uint, len(s.words))
	for _, v := range s.words {
		words = append(words, v)
	}
	return words
}


































































