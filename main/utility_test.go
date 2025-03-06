package main

import (
	"testing"
)

func TestMurmurHash(t *testing.T) {
	x := []string{"hello the world how are you", "hey out", "o", "anybody1", "anybody12"}
	y := []uint64{1535210816201750818, 13424779833380308823, 13210364986222294696,
		3340334878881431361, 16830039030203079439}
	for i, data := range x {
		hash := murmur3_64([]byte(data), 0)
		if hash != y[i] {
			t.Fatalf("Failed calculating murmur3 hash for %s", data)
		}
	}
	t.Log("Murmur3 has calculation impl passed")
}
