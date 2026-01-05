package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSpecializedOperations tests MergeSorted, ZipLongest, Cartesian, etc.
func TestSpecializedOperations(t *testing.T) {
	t.Parallel()
	t.Run("MergeSorted", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 3, 5, 7)
		s2 := Of(2, 4, 6, 8)
		result := MergeSorted(s1, s2, func(a, b int) int { return a - b }).Collect()
		assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, result, "MergeSorted should merge two sorted streams")
	})

	t.Run("MergeSortedUnequal", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 5)
		s2 := Of(2, 3, 4)
		result := MergeSorted(s1, s2, func(a, b int) int { return a - b }).Collect()
		assert.Equal(t, []int{1, 2, 3, 4, 5}, result, "MergeSorted should handle unequal lengths")
	})

	t.Run("MergeSortedEmptyLeft", func(t *testing.T) {
		t.Parallel()
		s1 := Empty[int]()
		s2 := Of(1, 2, 3)
		result := MergeSorted(s1, s2, func(a, b int) int { return a - b }).Collect()
		assert.Equal(t, []int{1, 2, 3}, result, "MergeSorted with empty left should yield right")
	})

	t.Run("MergeSortedEmptyRight", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2, 3)
		s2 := Empty[int]()
		result := MergeSorted(s1, s2, func(a, b int) int { return a - b }).Collect()
		assert.Equal(t, []int{1, 2, 3}, result, "MergeSorted with empty right should yield left")
	})

	t.Run("MergeSortedBothEmpty", func(t *testing.T) {
		t.Parallel()
		s1 := Empty[int]()
		s2 := Empty[int]()
		result := MergeSorted(s1, s2, func(a, b int) int { return a - b }).Collect()
		assert.Empty(t, result, "MergeSorted with both empty should be empty")
	})

	t.Run("MergeSortedEarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 3, 5, 7, 9)
		s2 := Of(2, 4, 6, 8, 10)
		result := MergeSorted(s1, s2, func(a, b int) int { return a - b }).Limit(4).Collect()
		assert.Equal(t, []int{1, 2, 3, 4}, result, "MergeSorted should respect Limit")
	})

	t.Run("MergeSortedNHeap", func(t *testing.T) {
		t.Parallel()
		// Test with multiple streams to trigger heap-based merge
		s1 := Of(1, 6, 11)
		s2 := Of(2, 7, 12)
		s3 := Of(3, 8, 13)
		s4 := Of(4, 9, 14)
		s5 := Of(5, 10, 15)
		cmp := func(a, b int) int { return a - b }
		result := MergeSortedNHeap(cmp, s1, s2, s3, s4, s5).Collect()
		assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, result, "MergeSortedNHeap should merge multiple sorted streams")
	})

	t.Run("MergeSortedNHeapEmptyStreams", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 3)
		s2 := Empty[int]()
		s3 := Of(2, 4)
		cmp := func(a, b int) int { return a - b }
		result := MergeSortedNHeap(cmp, s1, s2, s3).Collect()
		assert.Equal(t, []int{1, 2, 3, 4}, result, "MergeSortedNHeap should ignore empty streams")
	})

	t.Run("MergeSortedNHeapAllEmpty", func(t *testing.T) {
		t.Parallel()
		cmp := func(a, b int) int { return a - b }
		result := MergeSortedNHeap(cmp, Empty[int](), Empty[int]()).Collect()
		assert.Empty(t, result, "MergeSortedNHeap with all empty streams should be empty")
	})

	t.Run("MergeSortedNHeapSingleStream", func(t *testing.T) {
		t.Parallel()
		cmp := func(a, b int) int { return a - b }
		result := MergeSortedNHeap(cmp, Of(1, 2, 3)).Collect()
		assert.Equal(t, []int{1, 2, 3}, result, "MergeSortedNHeap with single stream should return same elements")
	})

	t.Run("MergeSortedNHeapEarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 4, 7)
		s2 := Of(2, 5, 8)
		s3 := Of(3, 6, 9)
		cmp := func(a, b int) int { return a - b }
		result := MergeSortedNHeap(cmp, s1, s2, s3).Limit(5).Collect()
		assert.Equal(t, []int{1, 2, 3, 4, 5}, result, "MergeSortedNHeap should respect Limit")
	})

	t.Run("ZipLongest", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2, 3)
		s2 := Of("a", "b")
		result := ZipLongest(s1, s2).Collect()

		assert.Len(t, result, 3, "ZipLongest should size to longer stream")
		assert.True(t, result[0].First.IsPresent(), "ZipLongest should wrap first element")
		assert.True(t, result[0].Second.IsPresent(), "ZipLongest should wrap second element where present")
		assert.True(t, result[2].First.IsPresent(), "ZipLongest should fill tail with default for shorter side")
		assert.False(t, result[2].Second.IsPresent(), "ZipLongest second should be None when exhausted")
	})

	t.Run("ZipLongestRightLonger", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1)
		s2 := Of("a", "b", "c")
		result := ZipLongest(s1, s2).Collect()

		assert.Len(t, result, 3, "ZipLongestRightLonger should include all right elements")
		assert.False(t, result[1].First.IsPresent(), "ZipLongest should set left None when left exhausted")
		assert.True(t, result[1].Second.IsPresent(), "ZipLongest should set right Some when available")
	})

	t.Run("ZipLongestEqualLength", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2)
		s2 := Of("a", "b")
		result := ZipLongest(s1, s2).Collect()

		assert.Len(t, result, 2, "ZipLongestEqualLength should have length equal to inputs")
		for _, r := range result {
			assert.True(t, r.First.IsPresent(), "ZipLongestEqualLength left should be present")
			assert.True(t, r.Second.IsPresent(), "ZipLongestEqualLength right should be present")
		}
	})

	t.Run("ZipLongestBothEmpty", func(t *testing.T) {
		t.Parallel()
		result := ZipLongest(Empty[int](), Empty[string]()).Collect()
		assert.Empty(t, result, "ZipLongest with both empty should be empty")
	})

	t.Run("ZipLongestWith", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2, 3)
		s2 := Of("a")
		result := ZipLongestWith(s1, s2, 0, "").Collect()

		assert.Len(t, result, 3, "ZipLongestWith should size to longer stream")
		assert.Equal(t, 1, result[0].First, "ZipLongestWith should combine first elements")
		assert.Equal(t, "a", result[0].Second, "ZipLongestWith should combine first elements")
		assert.Equal(t, 2, result[1].First, "ZipLongestWith should use default for missing")
		assert.Equal(t, "", result[1].Second, "ZipLongestWith should use default for missing")
		assert.Equal(t, 3, result[2].First, "ZipLongestWith should use default at tail")
		assert.Equal(t, "", result[2].Second, "ZipLongestWith should use default at tail")
	})

	t.Run("ZipLongestWithDefaults", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1)
		s2 := Of("a", "b", "c")
		result := ZipLongestWith(s1, s2, -99, "default").Collect()

		assert.Len(t, result, 3, "ZipLongestWithDefaults should fill with defaults")
		assert.Equal(t, -99, result[1].First, "ZipLongestWithDefaults left default should apply")
		assert.Equal(t, -99, result[2].First, "ZipLongestWithDefaults left default should apply at tail")
	})

	t.Run("Cartesian", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2)
		s2 := Of("a", "b")
		result := Cartesian(s1, s2).Collect()

		assert.Len(t, result, 4, "Cartesian should produce all pairs")
		assert.Equal(t, NewPair(1, "a"), result[0], "Cartesian pair should match")
		assert.Equal(t, NewPair(1, "b"), result[1], "Cartesian pair should match")
		assert.Equal(t, NewPair(2, "a"), result[2], "Cartesian pair should match")
		assert.Equal(t, NewPair(2, "b"), result[3], "Cartesian pair should match")
	})

	t.Run("CartesianEmpty", func(t *testing.T) {
		t.Parallel()
		result := Cartesian(Empty[int](), Of("a", "b")).Collect()
		assert.Empty(t, result, "Cartesian with empty left should be empty")
	})

	t.Run("CartesianEmptySecond", func(t *testing.T) {
		t.Parallel()
		result := Cartesian(Of(1, 2), Empty[string]()).Collect()
		assert.Empty(t, result, "Cartesian with empty right should be empty")
	})

	t.Run("CartesianEarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2, 3)
		s2 := Of("a", "b", "c")
		result := Cartesian(s1, s2).Limit(5).Collect()
		assert.Len(t, result, 5, "Cartesian should respect Limit")
	})

	t.Run("Combinations", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Of(1, 2, 3), 2).Collect()
		assert.Len(t, result, 3, "Combinations of 3 pick 2 should have 3 results")
		assert.Equal(t, []int{1, 2}, result[0], "Combinations result should match")
		assert.Equal(t, []int{1, 3}, result[1], "Combinations result should match")
		assert.Equal(t, []int{2, 3}, result[2], "Combinations result should match")
	})

	t.Run("CombinationsK1", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Of(1, 2, 3), 1).Collect()
		assert.Len(t, result, 3, "Combinations k=1 should have 3 results")
		assert.Equal(t, []int{1}, result[0], "Combinations element should match")
		assert.Equal(t, []int{2}, result[1], "Combinations element should match")
		assert.Equal(t, []int{3}, result[2], "Combinations element should match")
	})

	t.Run("CombinationsFull", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Of(1, 2, 3), 3).Collect()
		assert.Len(t, result, 1, "Combinations full should return single combination")
		assert.Equal(t, []int{1, 2, 3}, result[0], "Combinations full should match input")
	})

	t.Run("CombinationsKGreaterThanN", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Of(1, 2), 5).Collect()
		assert.Empty(t, result, "Combinations with k>n should be empty")
	})

	t.Run("CombinationsK0", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Of(1, 2, 3), 0).Collect()
		assert.Empty(t, result, "Combinations with k=0 should be empty")
	})

	t.Run("CombinationsEmpty", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Empty[int](), 2).Collect()
		assert.Empty(t, result, "Combinations on empty should be empty")
	})

	t.Run("CombinationsEarlyTermination", func(t *testing.T) {
		t.Parallel()
		result := Combinations(Of(1, 2, 3, 4, 5), 3).Limit(3).Collect()
		assert.Len(t, result, 3, "Combinations should respect Limit")
	})

	t.Run("Permutations", func(t *testing.T) {
		t.Parallel()
		result := Permutations(Of(1, 2, 3)).Collect()
		assert.Len(t, result, 6, "Permutations of 3 elements should be 6") // 3! = 6
	})

	t.Run("PermutationsSingle", func(t *testing.T) {
		t.Parallel()
		result := Permutations(Of(1)).Collect()
		assert.Len(t, result, 1, "Permutations of single element should be 1")
		assert.Equal(t, []int{1}, result[0], "Permutations single element should match")
	})

	t.Run("PermutationsTwo", func(t *testing.T) {
		t.Parallel()
		result := Permutations(Of(1, 2)).Collect()
		assert.Len(t, result, 2, "Permutations of 2 elements should be 2")
		assert.Equal(t, []int{1, 2}, result[0], "Permutations should include [1,2]")
		assert.Equal(t, []int{2, 1}, result[1], "Permutations should include [2,1]")
	})

	t.Run("PermutationsEmpty", func(t *testing.T) {
		t.Parallel()
		result := Permutations(Empty[int]()).Collect()
		assert.Empty(t, result, "Permutations of empty stream should be empty")
	})

	t.Run("PermutationsEarlyTermination", func(t *testing.T) {
		t.Parallel()
		result := Permutations(Of(1, 2, 3, 4)).Limit(5).Collect()
		assert.Len(t, result, 5, "Permutations should respect Limit")
	})
}

// TestTopKCollectors tests TopK, BottomK, and Quantile collectors.
func TestTopKCollectors(t *testing.T) {
	t.Parallel()
	t.Run("TopK", func(t *testing.T) {
		t.Parallel()
		result := TopK(Of(3, 1, 4, 1, 5, 9, 2, 6), 3, func(a, b int) bool { return a < b })
		assert.Len(t, result, 3, "TopK should return k elements")
		assert.Equal(t, 9, result[0], "TopK should be sorted descending")
		assert.Equal(t, 6, result[1], "TopK should be sorted descending")
		assert.Equal(t, 5, result[2], "TopK should be sorted descending")
	})

	t.Run("TopKFewerThanK", func(t *testing.T) {
		t.Parallel()
		result := TopK(Of(3, 1), 5, func(a, b int) bool { return a < b })
		assert.Len(t, result, 2, "TopK with fewer than k should return all elements")
	})

	t.Run("TopKEmpty", func(t *testing.T) {
		t.Parallel()
		result := TopK(Empty[int](), 3, func(a, b int) bool { return a < b })
		assert.Empty(t, result, "TopK on empty should return empty result")
	})

	t.Run("BottomK", func(t *testing.T) {
		t.Parallel()
		result := BottomK(Of(3, 1, 4, 1, 5, 9, 2, 6), 3, func(a, b int) bool { return a < b })
		assert.Len(t, result, 3, "BottomK should return k elements")
		assert.Equal(t, 1, result[0], "BottomK should be sorted ascending")
		assert.Equal(t, 1, result[1], "BottomK should be sorted ascending")
		assert.Equal(t, 2, result[2], "BottomK should be sorted ascending")
	})

	t.Run("BottomKFewerThanK", func(t *testing.T) {
		t.Parallel()
		result := BottomK(Of(5, 3), 5, func(a, b int) bool { return a < b })
		assert.Len(t, result, 2, "BottomK with fewer than k should return all elements")
	})

	t.Run("BottomKEmpty", func(t *testing.T) {
		t.Parallel()
		result := BottomK(Empty[int](), 3, func(a, b int) bool { return a < b })
		assert.Empty(t, result, "BottomK on empty should return empty result")
	})

	t.Run("Median", func(t *testing.T) {
		t.Parallel()
		result := Median(Of(1, 2, 3, 4, 5), func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Median should return Some for non-empty")
		assert.Equal(t, 3, result.Get(), "Median of 1..5 should be 3")
	})

	t.Run("MedianEvenCount", func(t *testing.T) {
		t.Parallel()
		result := Median(Of(1, 2, 3, 4), func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Median should return Some for non-empty")
		// Index 1 (middle of 4 elements = 4/2 - 1 = 1)
		assert.Equal(t, 2, result.Get(), "Median of 1..4 should be 2")
	})

	t.Run("MedianSingle", func(t *testing.T) {
		t.Parallel()
		result := Median(Of(42), func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Median should return Some for single element")
		assert.Equal(t, 42, result.Get(), "Median of single element should be that element")
	})

	t.Run("MedianEmpty", func(t *testing.T) {
		t.Parallel()
		result := Median(Empty[int](), func(a, b int) bool { return a < b })
		assert.False(t, result.IsPresent(), "Median on empty should be None")
	})

	t.Run("Quantile", func(t *testing.T) {
		t.Parallel()
		result := Quantile(Of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 0.75, func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Quantile should return Some")
		// 0.75 * 9 = 6.75, so index 6 or 7
		assert.True(t, result.Get() >= 7, "75th percentile should be >= 7")
	})

	t.Run("Quantile0", func(t *testing.T) {
		t.Parallel()
		result := Quantile(Of(1, 2, 3, 4, 5), 0.0, func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Quantile(0) should return Some")
		assert.Equal(t, 1, result.Get(), "Quantile(0) should be minimum")
	})

	t.Run("Quantile1", func(t *testing.T) {
		t.Parallel()
		result := Quantile(Of(1, 2, 3, 4, 5), 1.0, func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Quantile(1) should return Some")
		assert.Equal(t, 5, result.Get(), "Quantile(1) should be maximum")
	})

	t.Run("QuantileEmpty", func(t *testing.T) {
		t.Parallel()
		result := Quantile(Empty[int](), 0.5, func(a, b int) bool { return a < b })
		assert.False(t, result.IsPresent(), "Quantile on empty should be None")
	})

	t.Run("Frequency", func(t *testing.T) {
		t.Parallel()
		freq := Frequency(Of(1, 2, 2, 3, 3, 3))
		assert.Equal(t, 1, freq[1], "Frequency of 1 should be 1")
		assert.Equal(t, 2, freq[2], "Frequency of 2 should be 2")
		assert.Equal(t, 3, freq[3], "Frequency of 3 should be 3")
	})

	t.Run("FrequencyEmpty", func(t *testing.T) {
		t.Parallel()
		freq := Frequency(Empty[int]())
		assert.Empty(t, freq, "Frequency on empty should be empty map")
	})

	t.Run("FrequencyAllSame", func(t *testing.T) {
		t.Parallel()
		freq := Frequency(Of(5, 5, 5, 5))
		assert.Len(t, freq, 1, "Frequency on all same should have one key")
		assert.Equal(t, 4, freq[5], "Frequency count should equal occurrences")
	})

	t.Run("MostCommon", func(t *testing.T) {
		t.Parallel()
		result := MostCommon(Of("a", "b", "b", "c", "c", "c"), 2)
		assert.Len(t, result, 2, "MostCommon should return k elements")
		assert.Equal(t, "c", result[0].First, "MostCommon should be sorted by frequency desc")
		assert.Equal(t, 3, result[0].Second, "MostCommon should include frequency")
	})

	t.Run("MostCommonFewerThanK", func(t *testing.T) {
		t.Parallel()
		result := MostCommon(Of("a", "b"), 5)
		assert.Len(t, result, 2, "MostCommon with fewer than k should return all")
	})

	t.Run("MostCommonEmpty", func(t *testing.T) {
		t.Parallel()
		result := MostCommon(Empty[string](), 3)
		assert.Empty(t, result, "MostCommon on empty should be empty")
	})

	// Tests for equal elements to cover return 0 path in sort comparison
	t.Run("TopKAllEqual", func(t *testing.T) {
		t.Parallel()
		// All equal elements should trigger return 0 in sort comparison
		result := TopK(Of(5, 5, 5, 5, 5), 3, func(a, b int) bool { return a < b })
		assert.Len(t, result, 3, "TopKAllEqual should return k elements")
		assert.Equal(t, 5, result[0], "TopKAllEqual values should be equal")
		assert.Equal(t, 5, result[1], "TopKAllEqual values should be equal")
		assert.Equal(t, 5, result[2], "TopKAllEqual values should be equal")
	})

	t.Run("TopKWithDuplicates", func(t *testing.T) {
		t.Parallel()
		// Duplicates in top K to trigger return 0 path when comparing equal elements
		result := TopK(Of(3, 3, 1, 1, 2, 2), 4, func(a, b int) bool { return a < b })
		assert.Len(t, result, 4, "TopKWithDuplicates should return k elements")
		// Top 4 should be 3, 3, 2, 2 in descending order
		assert.Equal(t, 3, result[0], "TopKWithDuplicates should keep highest values first")
		assert.Equal(t, 3, result[1], "TopKWithDuplicates should keep highest values first")
		assert.Equal(t, 2, result[2], "TopKWithDuplicates should keep next values after highs")
		assert.Equal(t, 2, result[3], "TopKWithDuplicates should keep next values after highs")
	})

	t.Run("BottomKAllEqual", func(t *testing.T) {
		t.Parallel()
		// All equal elements should trigger return 0 in sort comparison
		result := BottomK(Of(7, 7, 7, 7, 7), 3, func(a, b int) bool { return a < b })
		assert.Len(t, result, 3, "BottomKAllEqual should return k elements")
		assert.Equal(t, 7, result[0], "BottomKAllEqual should preserve equal values")
		assert.Equal(t, 7, result[1], "BottomKAllEqual should preserve equal values")
		assert.Equal(t, 7, result[2], "BottomKAllEqual should preserve equal values")
	})

	t.Run("BottomKWithDuplicates", func(t *testing.T) {
		t.Parallel()
		// Duplicates in bottom K to trigger return 0 path when comparing equal elements
		result := BottomK(Of(3, 3, 1, 1, 2, 2), 4, func(a, b int) bool { return a < b })
		assert.Len(t, result, 4, "BottomKWithDuplicates should return k elements")
		// Bottom 4 should be 1, 1, 2, 2 in ascending order
		assert.Equal(t, 1, result[0], "BottomKWithDuplicates should keep smallest values first")
		assert.Equal(t, 1, result[1], "BottomKWithDuplicates should keep smallest values first")
		assert.Equal(t, 2, result[2], "BottomKWithDuplicates should keep next values after lows")
		assert.Equal(t, 2, result[3], "BottomKWithDuplicates should keep next values after lows")
	})

	t.Run("BottomKLargeK", func(t *testing.T) {
		t.Parallel()
		// More elements to exercise heapifyDown left child path
		result := BottomK(Of(10, 9, 8, 7, 6, 5, 4, 3, 2, 1), 5, func(a, b int) bool { return a < b })
		assert.Len(t, result, 5, "BottomKLargeK should return k elements")
		assert.Equal(t, []int{1, 2, 3, 4, 5}, result, "BottomKLargeK should return lowest values in order")
	})

	t.Run("QuantileWithDuplicates", func(t *testing.T) {
		t.Parallel()
		// Duplicates to trigger equal element comparison paths
		result := Quantile(Of(5, 5, 5, 5, 5), 0.5, func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Quantile with duplicates should return Some")
		assert.Equal(t, 5, result.Get(), "Median/quantile among duplicates should be 5")
	})

	t.Run("Quantile25", func(t *testing.T) {
		t.Parallel()
		// Test 25th percentile
		result := Quantile(Of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 0.25, func(a, b int) bool { return a < b })
		assert.True(t, result.IsPresent(), "Quantile25 should return Some")
		assert.True(t, result.Get() >= 2 && result.Get() <= 3, "25th percentile should be in [2,3]")
	})
}
