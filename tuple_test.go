package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPair tests Pair operations.
func TestPair(t *testing.T) {
	t.Parallel()
	t.Run("NewPair", func(t *testing.T) {
		t.Parallel()
		p := NewPair(1, "hello")
		assert.Equal(t, 1, p.First, "NewPair should set First")
		assert.Equal(t, "hello", p.Second, "NewPair should set Second")
	})

	t.Run("Swap", func(t *testing.T) {
		t.Parallel()
		p := NewPair(1, "hello")
		swapped := p.Swap()
		assert.Equal(t, "hello", swapped.First, "Swap should exchange First and Second")
		assert.Equal(t, 1, swapped.Second, "Swap should exchange First and Second")
	})

	t.Run("MapFirst", func(t *testing.T) {
		t.Parallel()
		p := NewPair(1, "hello")
		mapped := p.MapFirst(func(n int) int { return n * 2 })
		assert.Equal(t, 2, mapped.First, "MapFirst should transform First")
		assert.Equal(t, "hello", mapped.Second, "MapFirst should not change Second")
	})

	t.Run("MapSecond", func(t *testing.T) {
		t.Parallel()
		p := NewPair(1, "hello")
		mapped := p.MapSecond(func(s string) string { return s + "!" })
		assert.Equal(t, 1, mapped.First, "MapSecond should not change First")
		assert.Equal(t, "hello!", mapped.Second, "MapSecond should transform Second")
	})

	t.Run("Unpack", func(t *testing.T) {
		t.Parallel()
		p := NewPair(1, "hello")
		first, second := p.Unpack()
		assert.Equal(t, 1, first, "Unpack should return First")
		assert.Equal(t, "hello", second, "Unpack should return Second")
	})
}

// TestTriple tests Triple operations.
func TestTriple(t *testing.T) {
	t.Parallel()
	t.Run("NewTriple", func(t *testing.T) {
		t.Parallel()
		tr := NewTriple(1, "hello", 3.14)
		assert.Equal(t, 1, tr.First, "NewTriple should set First")
		assert.Equal(t, "hello", tr.Second, "NewTriple should set Second")
		assert.Equal(t, 3.14, tr.Third, "NewTriple should set Third")
	})

	t.Run("ToPair", func(t *testing.T) {
		t.Parallel()
		tr := NewTriple(1, "hello", 3.14)
		p := tr.ToPair()
		assert.Equal(t, 1, p.First, "ToPair should preserve First")
		assert.Equal(t, "hello", p.Second, "ToPair should preserve Second")
	})

	t.Run("Unpack", func(t *testing.T) {
		t.Parallel()
		tr := NewTriple(1, "hello", 3.14)
		first, second, third := tr.Unpack()
		assert.Equal(t, 1, first, "Unpack should return First")
		assert.Equal(t, "hello", second, "Unpack should return Second")
		assert.Equal(t, 3.14, third, "Unpack should return Third")
	})

	t.Run("MapFirst", func(t *testing.T) {
		t.Parallel()
		tr := NewTriple(1, "hello", 3.14)
		mapped := tr.MapFirst(func(n int) int { return n * 2 })
		assert.Equal(t, 2, mapped.First, "MapFirst should transform First")
		assert.Equal(t, "hello", mapped.Second, "MapFirst should not change Second")
		assert.Equal(t, 3.14, mapped.Third, "MapFirst should not change Third")
	})

	t.Run("MapSecond", func(t *testing.T) {
		t.Parallel()
		tr := NewTriple(1, "hello", 3.14)
		mapped := tr.MapSecond(func(s string) string { return s + "!" })
		assert.Equal(t, 1, mapped.First, "MapSecond should not change First")
		assert.Equal(t, "hello!", mapped.Second, "MapSecond should transform Second")
		assert.Equal(t, 3.14, mapped.Third, "MapSecond should not change Third")
	})

	t.Run("MapThird", func(t *testing.T) {
		t.Parallel()
		tr := NewTriple(1, "hello", 3.14)
		mapped := tr.MapThird(func(f float64) float64 { return f * 2 })
		assert.Equal(t, 1, mapped.First, "MapThird should not change First")
		assert.Equal(t, "hello", mapped.Second, "MapThird should not change Second")
		assert.Equal(t, 6.28, mapped.Third, "MapThird should transform Third")
	})
}

// TestZip3Operations tests Zip3 operations.
func TestZip3Operations(t *testing.T) {
	t.Parallel()
	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := Of(1, 2, 3, 4, 5)
		s2 := Of("a", "b", "c", "d", "e")
		s3 := Of(1.0, 2.0, 3.0, 4.0, 5.0)
		result := Zip3(s1, s2, s3).Limit(2).Collect()
		assert.Len(t, result, 2, "Zip3 with Limit should stop early")
		assert.Equal(t, 1, result[0].First, "First element of zipped triple should match")
		assert.Equal(t, "a", result[0].Second, "Second element of zipped triple should match")
		assert.Equal(t, 1.0, result[0].Third, "Third element of zipped triple should match")
	})
}

// TestQuad tests Quad operations.
func TestQuad(t *testing.T) {
	t.Parallel()
	t.Run("NewQuad", func(t *testing.T) {
		t.Parallel()
		q := NewQuad(1, "hello", 3.14, true)
		assert.Equal(t, 1, q.First, "NewQuad should set First")
		assert.Equal(t, "hello", q.Second, "NewQuad should set Second")
		assert.Equal(t, 3.14, q.Third, "NewQuad should set Third")
		assert.Equal(t, true, q.Fourth, "NewQuad should set Fourth")
	})

	t.Run("ToTriple", func(t *testing.T) {
		t.Parallel()
		q := NewQuad(1, "hello", 3.14, true)
		tr := q.ToTriple()
		assert.Equal(t, 1, tr.First, "ToTriple should preserve First")
		assert.Equal(t, "hello", tr.Second, "ToTriple should preserve Second")
		assert.Equal(t, 3.14, tr.Third, "ToTriple should preserve Third")
	})

	t.Run("ToPair", func(t *testing.T) {
		t.Parallel()
		q := NewQuad(1, "hello", 3.14, true)
		p := q.ToPair()
		assert.Equal(t, 1, p.First, "ToPair should preserve First")
		assert.Equal(t, "hello", p.Second, "ToPair should preserve Second")
	})

	t.Run("Unpack", func(t *testing.T) {
		t.Parallel()
		q := NewQuad(1, "hello", 3.14, true)
		first, second, third, fourth := q.Unpack()
		assert.Equal(t, 1, first, "Unpack should return First")
		assert.Equal(t, "hello", second, "Unpack should return Second")
		assert.Equal(t, 3.14, third, "Unpack should return Third")
		assert.Equal(t, true, fourth, "Unpack should return Fourth")
	})
}
