package streams

import "iter"

// Pair represents a tuple of two values.
type Pair[T, U any] struct {
	First  T
	Second U
}

// NewPair creates a new Pair.
func NewPair[T, U any](first T, second U) Pair[T, U] {
	return Pair[T, U]{First: first, Second: second}
}

// Swap returns a new Pair with First and Second swapped.
func (p Pair[T, U]) Swap() Pair[U, T] {
	return Pair[U, T]{First: p.Second, Second: p.First}
}

// MapFirst transforms the First element.
func (p Pair[T, U]) MapFirst(fn func(T) T) Pair[T, U] {
	return Pair[T, U]{First: fn(p.First), Second: p.Second}
}

// MapSecond transforms the Second element.
func (p Pair[T, U]) MapSecond(fn func(U) U) Pair[T, U] {
	return Pair[T, U]{First: p.First, Second: fn(p.Second)}
}

// Unpack returns the pair's elements separately.
func (p Pair[T, U]) Unpack() (T, U) {
	return p.First, p.Second
}

// Triple represents a tuple of three values.
type Triple[A, B, C any] struct {
	First  A
	Second B
	Third  C
}

// NewTriple creates a new Triple.
func NewTriple[A, B, C any](first A, second B, third C) Triple[A, B, C] {
	return Triple[A, B, C]{First: first, Second: second, Third: third}
}

// ToPair converts Triple to Pair by dropping the third element.
func (t Triple[A, B, C]) ToPair() Pair[A, B] {
	return Pair[A, B]{First: t.First, Second: t.Second}
}

// Unpack returns the triple's elements separately.
func (t Triple[A, B, C]) Unpack() (A, B, C) {
	return t.First, t.Second, t.Third
}

// MapFirst transforms the First element.
func (t Triple[A, B, C]) MapFirst(fn func(A) A) Triple[A, B, C] {
	return Triple[A, B, C]{First: fn(t.First), Second: t.Second, Third: t.Third}
}

// MapSecond transforms the Second element.
func (t Triple[A, B, C]) MapSecond(fn func(B) B) Triple[A, B, C] {
	return Triple[A, B, C]{First: t.First, Second: fn(t.Second), Third: t.Third}
}

// MapThird transforms the Third element.
func (t Triple[A, B, C]) MapThird(fn func(C) C) Triple[A, B, C] {
	return Triple[A, B, C]{First: t.First, Second: t.Second, Third: fn(t.Third)}
}

// Quad represents a tuple of four values.
type Quad[A, B, C, D any] struct {
	First  A
	Second B
	Third  C
	Fourth D
}

// NewQuad creates a new Quad.
func NewQuad[A, B, C, D any](first A, second B, third C, fourth D) Quad[A, B, C, D] {
	return Quad[A, B, C, D]{First: first, Second: second, Third: third, Fourth: fourth}
}

// ToTriple converts Quad to Triple by dropping the fourth element.
func (q Quad[A, B, C, D]) ToTriple() Triple[A, B, C] {
	return Triple[A, B, C]{First: q.First, Second: q.Second, Third: q.Third}
}

// ToPair converts Quad to Pair by dropping the third and fourth elements.
func (q Quad[A, B, C, D]) ToPair() Pair[A, B] {
	return Pair[A, B]{First: q.First, Second: q.Second}
}

// Unpack returns the quad's elements separately.
func (q Quad[A, B, C, D]) Unpack() (A, B, C, D) {
	return q.First, q.Second, q.Third, q.Fourth
}

// --- Zip variants for tuples ---

// Zip3 combines three Streams into a Stream of Triples.
// The resulting stream ends when any input stream ends.
func Zip3[A, B, C any](s1 Stream[A], s2 Stream[B], s3 Stream[C]) Stream[Triple[A, B, C]] {
	return Stream[Triple[A, B, C]]{
		seq: func(yield func(Triple[A, B, C]) bool) {
			next1, stop1 := iter.Pull(s1.seq)
			defer stop1()
			next2, stop2 := iter.Pull(s2.seq)
			defer stop2()
			next3, stop3 := iter.Pull(s3.seq)
			defer stop3()

			for {
				v1, ok1 := next1()
				v2, ok2 := next2()
				v3, ok3 := next3()
				if !ok1 || !ok2 || !ok3 {
					return
				}
				if !yield(Triple[A, B, C]{First: v1, Second: v2, Third: v3}) {
					return
				}
			}
		},
	}
}

// Unzip splits a Stream of Pairs into two separate slices.
func Unzip[T, U any](s Stream[Pair[T, U]]) ([]T, []U) {
	var firsts []T
	var seconds []U
	for p := range s.seq {
		firsts = append(firsts, p.First)
		seconds = append(seconds, p.Second)
	}
	return firsts, seconds
}
