package heavykeeper

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTopkList(t *testing.T) {
	// zipfan distribution
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().Unix())), 2, 2, 1000)
	topk := New(10, 1000, 5, 0.9)
	for i := 0; i < 10000; i++ {
		topk.Add(strconv.FormatUint(zipf.Uint64(), 10), 1)
	}
	for i, node := range topk.List() {
		assert.Equal(t, strconv.FormatInt(int64(i), 10), node.Item)
		t.Logf("%s: %d", node.Item, node.Count)
	}
}

func BenchmarkAdd(b *testing.B) {
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().Unix())), 2, 2, 1000)
	var data []string = make([]string, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = strconv.FormatUint(zipf.Uint64(), 10)
	}
	topk := New(10, 1000, 5, 0.9)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topk.Add(data[i%1000], 1)
	}
}

func BenchmarkAddwithLock(b *testing.B) {
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().Unix())), 2, 2, 1000)
	var data []string = make([]string, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = strconv.FormatUint(zipf.Uint64(), 10)
	}
	mutex := sync.Mutex{}
	topk := New(10, 1000, 5, 0.9)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			mutex.Lock()
			topk.Add(data[i%1000], 1)
			mutex.Unlock()
		}
	})
}
