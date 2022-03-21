package sst

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func TestBasic(t *testing.T) {
	b := newBloom()
	b.Add([]byte("abc"))
	b.Add([]byte("bcd"))

	if got := b.Test([]byte("abc")); !got {
		t.Errorf("b.Test(abc)=%v, wanted true", got)
	}
	if got := b.Test([]byte("bcd")); !got {
		t.Errorf("b.Test(bcd)=%v, wanted true", got)
	}
	if got := b.Test([]byte("d")); got {
		t.Errorf("b.Test(d)=%v, wanted false", got)
	}
}

func TestPercentile(t *testing.T) {
	var total, bad int
	threshold := 0.01
	for i := 0; i < 50; i++ {
		fp := runTrial(16000, t)
		if fp > 2*threshold {
			t.Errorf("Exceedingly bad FP rate: %v", fp)
		}
		if fp > threshold {
			t.Logf("FP rate: %v", fp)
			bad++
		}
		total++
	}
	if float64(bad)/float64(total) > 0.995 {
		t.Errorf("Bloom filters did not have expected false positive rate. "+
			"%v out of %v had rate > %v", bad, total, threshold)
	}
}

// runTrial returns the false positive ratio
func runTrial(n int, t *testing.T) float64 {
	b := newBloom()
	keys := make(map[string]struct{})

	for i := 0; i < n; i++ {
		key := fmt.Sprint(rand.Int31())
		keys[key] = struct{}{}
		b.Add([]byte(key))
	}

	// Validate all added keys still test ok.
	for k := range keys {
		if !b.Test([]byte(k)) {
			t.Fatalf("b.Test(%v)=false, expected true for added key", string(k))
		}
	}

	// Validate random keys
	var hits, total int
	for i := 0; i < 5000; i++ {
		key := fmt.Sprint(rand.Int31())
		if _, found := keys[key]; !found {
			if b.Test([]byte(key)) {
				hits++
			}
		}
		total++
	}

	return float64(hits) / float64(total)
}

func TestBloomCollisions(t *testing.T) {

	const loopSize = 100000

	bl := newBloom()

	var keys []string // real keys in the bloomFilter

	t.Run("populating", func(t *testing.T) {
		for i := 0; i < loopSize; i++ {
			key := randomString(500) // 500 is the max size of a key

			bl.Add([]byte(key))
			keys = append(keys, key)

			if !bl.Test([]byte(key)) {
				t.Error("expected key to be found")
			}
		}
		sort.Strings(keys) // sort for easier searching
	})

	var collisions int
	t.Run("checking", func(t *testing.T) {
		// loop until a collision is found
		for i := 0; i < loopSize; i++ {
			// generate a new random key
			key := randomString(500)

			if idx := sort.SearchStrings(keys, key); idx != len(keys) && keys[idx] == key {
				continue // if existing key has been generated, skip it
			}

			// check if the string is in the bloom filter
			if bl.Test([]byte(key)) {
				// if it is, check if it's in the real keys
				i := sort.SearchStrings(keys, key)
				if i == len(keys) || keys[i] != key {
					collisions++
				}
			} else {
				// if it's not, check if it's in the real keys
				i := sort.SearchStrings(keys, key)
				if i != len(keys) && keys[i] == key {
					t.Errorf("expected %s to not be in the bloom filter but it was", key)
					break
				}
			}
		}

		if collisions > 0 {
			t.Logf("found %d collisions", collisions)
		} else {
			t.Log("no collisions found")
		}
	})
}

func randomString(upToSize int32) string {
	length := rand.Int31n(upToSize)

	var randomString string
	for j := 0; j < int(length); j++ {
		randomString += string(letters[rand.Intn(len(letters))])
	}

	return randomString
}
