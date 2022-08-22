package heavykeeper

import (
	"math"
	"math/rand"

	"github.com/lintanghui/heavykeeper/pkg/minheap"
	"github.com/spaolacci/murmur3"
)

type TopK struct {
	k     uint32
	width uint32
	depth uint32
	decay float64

	buckets [][]bucket
	minHeap *minheap.Heap
}

func New(workers int, k, width, depth uint32, decay float64) *TopK {
	// err check...

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

func (topk *TopK) Add(item string, incr uint32) string {
	itemFingerprint := murmur3.Sum32([]byte(item))

	var maxCount uint32
	heapMin := topk.minHeap.Min()

	// compute d hashes
	for i := uint32(0); i < topk.depth; i++ {

		bucketNumber := murmur3.Sum32WithSeed([]byte(item), uint32(i)) % uint32(topk.width)

		fingerprint := topk.buckets[i][bucketNumber].fingerprint
		count := topk.buckets[i][bucketNumber].count

		if count == 0 {
			topk.buckets[i][bucketNumber].fingerprint = itemFingerprint
			topk.buckets[i][bucketNumber].count = incr
			maxCount = max(maxCount, incr)

		} else if fingerprint == itemFingerprint {
			topk.buckets[i][bucketNumber].count += incr
			maxCount = max(maxCount, topk.buckets[i][bucketNumber].count)

		} else {
			for local_incr := incr; local_incr > 0; local_incr-- {
				decay := math.Pow(topk.decay, float64(count))
				if rand.Float64() < decay {
					topk.buckets[i][bucketNumber].count--
					if topk.buckets[i][bucketNumber].count == 0 {
						topk.buckets[i][bucketNumber].fingerprint = itemFingerprint
						topk.buckets[i][bucketNumber].count = local_incr
						maxCount = max(maxCount, local_incr)
						break
					}
				}
			}
		}
	}

	if maxCount > heapMin {
		itemHeapIdx, itemHeapExist := topk.minHeap.Find(item)
		if itemHeapExist {
			topk.minHeap.Fix(itemHeapIdx, maxCount)
		} else {
			expelled := topk.minHeap.Pop()
			topk.minHeap.Add(minheap.Node{Item: item, Count: maxCount})
			return expelled.Item
		}
	}
	return ""
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
