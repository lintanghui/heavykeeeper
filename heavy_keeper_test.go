package heavykeeper

import (
	"math"
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
	topk := New(10, 10000, 5, 0.925)
	dataMap := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := strconv.FormatUint(zipf.Uint64(), 10)
		dataMap[key] = dataMap[key] + 1
		topk.Add(key, 1)
	}
	var rate float64
	for _, node := range topk.List() {
		rate += math.Abs(float64(node.Count)-float64(dataMap[node.Item])) / float64(dataMap[node.Item])
		t.Logf("item %s, count %d, expect %d", node.Item, node.Count, dataMap[node.Item])
	}
	t.Logf("err rate avg:%f", rate)
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

func BenchmarkPow(b *testing.B) {
	var value float64
	for i := 0; i < b.N; i++ {
		_ = math.Pow(0.925, float64(i))
	}
	_ = value
}

func BenchmarkPowLookup(b *testing.B) {
	tables := make([]float64, 256)
	for i := 0; i < 256; i++ {
		tables[i] = math.Pow(0.925, float64(i))
	}
	b.ResetTimer()
	var value float64
	for j := 0; j < b.N; j++ {
		if j < 256 {
			value = tables[j]
		} else {
			value = math.Pow(tables[256], float64(j/256)) * tables[j%256]
		}
	}
	_ = value
}

func TestPow(t *testing.T) {
	tables := make([]float64, 256)
	for i := 0; i < 256; i++ {
		tables[i] = math.Pow(2, float64(i))
	}
	for j := 0; j < 1000; j++ {
		var lvalue float64
		var value float64
		if j < 256 {
			lvalue = tables[j]
		} else {
			lvalue = math.Pow(tables[255], float64(j/255)) * tables[j%255]
		}
		value = math.Pow(2, float64(j))
		assert.Equal(t, lvalue, value)
		t.Log("j:", j, "lvalue:", lvalue, "value:", value)
	}
}
