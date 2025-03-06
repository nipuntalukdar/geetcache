package main

import (
	"math"
	"sync"
)

type hyperlog struct {
	slot    []uint8
	numslot uint32
	lock    *sync.RWMutex
}

const (
	SLOT     uint32  = 256
	SLOTF    float64 = float64(SLOT)
	twopo32  uint64  = 0x00000100000000
	twopo32f float64 = float64(twopo32)
	fval     float64 = (0.7213 / (1 + 1.079/SLOTF)) * SLOTF * SLOTF
	cmp1     float64 = 2.5 * SLOTF
	cmp2     float64 = twopo32f / 30.0
)

func newHyperLog() *hyperlog {
	slot := make([]uint8, SLOT)
	return &hyperlog{slot: slot, numslot: SLOT, lock: &sync.RWMutex{}}
}

func (hpl *hyperlog) addhash(val uint32) {
	idx := uint8(val >> 24)
	var andwth uint32 = 0x00800000
	var leadzs uint32 = 0
	i := 23
	for {
		if (val & andwth) != 0 {
			break
		}
		leadzs += 1
		if i == 0 {
			break
		}
		i--
		andwth = andwth >> 1
	}
	leadzs += 1
	hpl.lock.Lock()
	if hpl.slot[idx] < uint8(leadzs) {
		hpl.slot[idx] = uint8(leadzs)
	}
	hpl.lock.Unlock()
}

func (hpl *hyperlog) count_cardinality() uint64 {
	var i uint32 = 0
	sum := 0.0
	hpl.lock.RLock()
	defer hpl.lock.RUnlock()
	for i < SLOT {
		x := 1 << hpl.slot[i]
		sum += 1.0 / float64(x)
		i++
	}
	ret := fval / sum
	if ret <= cmp1 {
		i = 0
		var v float64 = 0
		for i < SLOT {
			if hpl.slot[i] == 0 {
				v++
			}
			i++
		}
		if v != 0 {
			return uint64(SLOTF * math.Log(SLOTF/v))
		} else {
			return uint64(ret)
		}
	} else if ret <= cmp2 {
		return uint64(ret)
	}
	ret = -(twopo32f * math.Log(1.0-ret/twopo32f))
	return uint64(ret)
}
