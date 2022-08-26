package heavykeeper

import (
	"math"
	"math/rand"
	"unsafe"

	"github.com/lintanghui/heavykeeper/pkg/minheap"
	"github.com/spaolacci/murmur3"
)

type TopK struct {
	k     uint32
	width uint32
	depth uint32
	decay float64

	r       *rand.Rand
	buckets [][]bucket
	minHeap *minheap.Heap
}

func New(k, width, depth uint32, decay float64) *TopK {
	arrays := make([][]bucket, depth)
	for i := range arrays {
		arrays[i] = make([]bucket, width)
	}

	topk := TopK{
		k:       k,
		width:   width,
		depth:   depth,
		decay:   decay,
		buckets: arrays,
		r:       rand.New(rand.NewSource(0)),
		minHeap: minheap.NewHeap(k),
	}

	return &topk
}

func (topk *TopK) Query(item string) (exist bool) {
	_, exist = topk.minHeap.Find(item)
	return
}

func (topk *TopK) Count(item string) (uint32, bool) {
	if id, exist := topk.minHeap.Find(item); exist {
		return topk.minHeap.Nodes[id].Count, true
	}
	return 0, false
}

func (topk *TopK) List() []minheap.Node {
	return topk.minHeap.Sorted()
}

// Add add item into heavykeeper and return if item had beend add into minheap.
// if item had been add into minheap and some item was expelled, return the expelled item.
func (topk *TopK) Add(item string, incr uint32) (string, bool) {
	bs := StringToBytes(item)
	itemFingerprint := murmur3.Sum32(bs)
	var maxCount uint32

	// compute d hashes
	for i, row := range topk.buckets {

		bucketNumber := murmur3.Sum32WithSeed(bs, uint32(i)) % uint32(topk.width)

		fingerprint := row[bucketNumber].fingerprint
		count := row[bucketNumber].count

		if count == 0 {
			row[bucketNumber].fingerprint = itemFingerprint
			row[bucketNumber].count = incr
			maxCount = max(maxCount, incr)

		} else if fingerprint == itemFingerprint {
			row[bucketNumber].count += incr
			maxCount = max(maxCount, row[bucketNumber].count)

		} else {
			for local_incr := incr; local_incr > 0; local_incr-- {
				decay := math.Pow(topk.decay, float64(count))
				if topk.r.Float64() < decay {
					row[bucketNumber].count--
					if row[bucketNumber].count == 0 {
						row[bucketNumber].fingerprint = itemFingerprint
						row[bucketNumber].count = local_incr
						maxCount = max(maxCount, local_incr)
						break
					}
				}
			}
		}
	}
	minHeap := topk.minHeap.Min()
	if len(topk.minHeap.Nodes) == int(topk.k) && maxCount < minHeap {
		return "", false
	}
	// update minheap
	itemHeapIdx, itemHeapExist := topk.minHeap.Find(item)
	if itemHeapExist {
		topk.minHeap.Fix(itemHeapIdx, maxCount)
		return "", true
	}
	expelled := topk.minHeap.Add(minheap.Node{Item: item, Count: maxCount})
	return expelled, true
}

type bucket struct {
	fingerprint uint32
	count       uint32
}

func (b *bucket) Get() (uint32, uint32) {
	return b.fingerprint, b.count
}

func (b *bucket) Set(fingerprint, count uint32) {
	b.fingerprint = fingerprint
	b.count = count
}

func (b *bucket) Inc(val uint32) uint32 {
	b.count += val
	return b.count
}

func max(x, y uint32) uint32 {
	if x > y {
		return x
	}
	return y
}

func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
