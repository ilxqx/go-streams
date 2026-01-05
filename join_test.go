package streams

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- JoinResult and JoinResultOptional Tests ---

func TestJoinResultTypes(t *testing.T) {
	t.Parallel()
	t.Run("JoinResultFields", func(t *testing.T) {
		t.Parallel()
		result := JoinResult[string, int, string]{
			Key:   "a",
			Left:  1,
			Right: "x",
		}
		assert.Equal(t, "a", result.Key, "JoinResult.Key should match")
		assert.Equal(t, 1, result.Left, "JoinResult.Left should match")
		assert.Equal(t, "x", result.Right, "JoinResult.Right should match")
	})

	t.Run("JoinResultOptionalFields", func(t *testing.T) {
		t.Parallel()
		result := JoinResultOptional[string, int, string]{
			Key:   "a",
			Left:  Some(1),
			Right: None[string](),
		}
		assert.Equal(t, "a", result.Key, "JoinResultOptional.Key should match")
		assert.True(t, result.Left.IsPresent(), "JoinResultOptional.Left should be present")
		assert.False(t, result.Right.IsPresent(), "JoinResultOptional.Right should be empty")
	})
}

// --- InnerJoin Tests ---

func TestInnerJoin(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("d", "z"))

		result := InnerJoin(s1, s2).Collect()

		assert.Len(t, result, 2, "InnerJoin should join by matching keys")
		keys := make([]string, len(result))
		for i, r := range result {
			keys[i] = r.Key
		}
		sort.Strings(keys)
		assert.Equal(t, []string{"a", "b"}, keys, "InnerJoin keys should be [a b]")
	})

	t.Run("EmptyLeftStream", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"))

		result := InnerJoin(s1, s2).Collect()
		assert.Empty(t, result, "InnerJoin with empty left should be empty")
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := Empty2[string, string]()

		result := InnerJoin(s1, s2).Collect()
		assert.Empty(t, result, "InnerJoin with empty right should be empty")
	})

	t.Run("NoMatchingKeys", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("c", "x"), NewPair("d", "y"))

		result := InnerJoin(s1, s2).Collect()
		assert.Empty(t, result, "InnerJoin with no matching keys should be empty")
	})

	t.Run("MultipleValuesPerKey", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("a", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("a", "y"))

		result := InnerJoin(s1, s2).Collect()
		assert.Len(t, result, 4, "InnerJoin with 2x2 combinations should produce 4")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := InnerJoin(s1, s2).Limit(1).Collect()
		assert.Len(t, result, 1, "InnerJoin should respect Limit")
	})
}

// --- LeftJoin Tests ---

func TestLeftJoin(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := LeftJoin(s1, s2).Collect()

		assert.Len(t, result, 3, "LeftJoin should produce all left rows")
		// Find the "c" result which should have None for right
		for _, r := range result {
			if r.Key == "c" {
				assert.True(t, r.Left.IsPresent(), "LeftJoin Left should be present")
				assert.False(t, r.Right.IsPresent(), "LeftJoin Right should be None when unmatched")
			}
		}
	})

	t.Run("EmptyLeftStream", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"))

		result := LeftJoin(s1, s2).Collect()
		assert.Empty(t, result, "LeftJoin with empty left should be empty")
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := Empty2[string, string]()

		result := LeftJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "LeftJoin with empty right should return len(left)")
		for _, r := range result {
			assert.True(t, r.Left.IsPresent(), "LeftJoin Left should be present")
			assert.False(t, r.Right.IsPresent(), "LeftJoin Right should be None")
		}
	})

	t.Run("AllMatched", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := LeftJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "LeftJoin all matched should return all")
		for _, r := range result {
			assert.True(t, r.Left.IsPresent(), "LeftJoin Left should be present")
			assert.True(t, r.Right.IsPresent(), "LeftJoin Right should be present when matched")
		}
	})

	t.Run("MultipleValuesPerKey", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("a", "x"), NewPair("a", "y"))

		result := LeftJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "LeftJoin with 1x2 combinations should be 2")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"))

		result := LeftJoin(s1, s2).Limit(1).Collect()
		assert.Len(t, result, 1, "LeftJoin should respect Limit")
	})

	t.Run("EarlyTerminationInInnerLoop", func(t *testing.T) {
		t.Parallel()
		// s2 has multiple values for the same key "a" - this creates a cartesian product
		// and Limit(1) should terminate inside the inner loop (join.go line 65-66)
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("a", "x"), NewPair("a", "y"), NewPair("a", "z"))

		result := LeftJoin(s1, s2).Limit(1).Collect()
		assert.Len(t, result, 1, "LeftJoin early termination in inner loop should return 1")
		assert.Equal(t, "a", result[0].Key, "LeftJoin key should be 'a'")
		assert.True(t, result[0].Left.IsPresent(), "LeftJoin Left should be present")
		assert.True(t, result[0].Right.IsPresent(), "LeftJoin Right should be present")
	})
}

// --- RightJoin Tests ---

func TestRightJoin(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := RightJoin(s1, s2).Collect()

		assert.Len(t, result, 3, "RightJoin should produce all right rows")
		// Find the "c" result which should have None for left
		for _, r := range result {
			if r.Key == "c" {
				assert.False(t, r.Left.IsPresent(), "RightJoin Left should be None when unmatched")
				assert.True(t, r.Right.IsPresent(), "RightJoin Right should be present")
			}
		}
	})

	t.Run("EmptyLeftStream", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := RightJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "RightJoin with empty left should return len(right)")
		for _, r := range result {
			assert.False(t, r.Left.IsPresent(), "RightJoin Left should be None")
			assert.True(t, r.Right.IsPresent(), "RightJoin Right should be present")
		}
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := Empty2[string, string]()

		result := RightJoin(s1, s2).Collect()
		assert.Empty(t, result, "RightJoin with empty right should be empty")
	})

	t.Run("MultipleValuesPerKey", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("a", 2))
		s2 := PairsOf(NewPair("a", "x"))

		result := RightJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "RightJoin with 2x1 combinations should be 2")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := RightJoin(s1, s2).Limit(1).Collect()
		assert.Len(t, result, 1, "RightJoin should respect Limit")
	})

	t.Run("EarlyTerminationInInnerLoop", func(t *testing.T) {
		t.Parallel()
		// s1 has multiple values for the same key "a" - this creates a cartesian product
		// and Limit(1) should terminate inside the inner loop (join.go line 103-104)
		s1 := PairsOf(NewPair("a", 1), NewPair("a", 2), NewPair("a", 3))
		s2 := PairsOf(NewPair("a", "x"))

		result := RightJoin(s1, s2).Limit(1).Collect()
		assert.Len(t, result, 1, "RightJoin inner loop early termination should return 1")
		assert.Equal(t, "a", result[0].Key, "RightJoin key should be 'a'")
		assert.True(t, result[0].Left.IsPresent(), "RightJoin Left should be present")
		assert.True(t, result[0].Right.IsPresent(), "RightJoin Right should be present")
	})
}

// --- FullJoin Tests ---

func TestFullJoin(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("b", "y"), NewPair("c", "z"))

		result := FullJoin(s1, s2).Collect()

		assert.Len(t, result, 3, "FullJoin should contain union of keys (a,b,c)") // a, b, c
	})

	t.Run("EmptyBothStreams", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := Empty2[string, string]()

		result := FullJoin(s1, s2).Collect()
		assert.Empty(t, result, "FullJoin with both empty should be empty")
	})

	t.Run("EmptyLeftStream", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"))

		result := FullJoin(s1, s2).Collect()
		assert.Len(t, result, 1, "FullJoin with empty left should return right rows with Left=None")
		assert.False(t, result[0].Left.IsPresent(), "FullJoin Left should be None")
		assert.True(t, result[0].Right.IsPresent(), "FullJoin Right should be present")
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := Empty2[string, string]()

		result := FullJoin(s1, s2).Collect()
		assert.Len(t, result, 1, "FullJoin with empty right should return left rows with Right=None")
		assert.True(t, result[0].Left.IsPresent(), "FullJoin Left should be present")
		assert.False(t, result[0].Right.IsPresent(), "FullJoin Right should be None")
	})

	t.Run("NoMatchingKeys", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("b", "x"))

		result := FullJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "FullJoin with no matching keys should include both sides")
	})

	t.Run("AllMatchingKeys", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := FullJoin(s1, s2).Collect()
		assert.Len(t, result, 2, "FullJoin with all matched should return all")
		for _, r := range result {
			assert.True(t, r.Left.IsPresent(), "FullJoin Left should be present")
			assert.True(t, r.Right.IsPresent(), "FullJoin Right should be present")
		}
	})

	t.Run("MultipleValuesPerKey", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("a", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("a", "y"))

		result := FullJoin(s1, s2).Collect()
		assert.Len(t, result, 4, "FullJoin with 2x2 combinations should be 4")
	})

	t.Run("EarlyTerminationMatched", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := FullJoin(s1, s2).Limit(1).Collect()
		assert.Len(t, result, 1, "FullJoin should respect Limit")
	})

	t.Run("EarlyTerminationUnmatchedLeft", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := Empty2[string, string]()

		result := FullJoin(s1, s2).Limit(2).Collect()
		assert.Len(t, result, 2, "FullJoin should respect Limit on unmatched-left")
	})

	t.Run("EarlyTerminationUnmatchedRight", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := FullJoin(s1, s2).Limit(2).Collect()
		assert.Len(t, result, 2, "FullJoin should respect Limit on unmatched-right")
	})
}

// --- LeftJoinWith Tests ---

func TestLeftJoinWith(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := LeftJoinWith(s1, s2, "default").Collect()

		assert.Len(t, result, 3, "LeftJoinWith should return one row per left key")
		for _, r := range result {
			if r.Key == "c" {
				assert.Equal(t, "default", r.Right, "LeftJoinWith should use default for missing right value")
			}
		}
	})

	t.Run("EmptyRightStreamUsesDefault", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := Empty2[string, int]()

		result := LeftJoinWith(s1, s2, 0).Collect()
		assert.Len(t, result, 2, "LeftJoinBy should produce one row per left element")
		for _, r := range result {
			assert.Equal(t, 0, r.Right, "LeftJoinWith should use default for empty right stream")
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"))

		result := LeftJoinWith(s1, s2, "default").Limit(1).Collect()
		assert.Len(t, result, 1, "LeftJoinWith should respect Limit")
	})

	t.Run("EarlyTerminationInInnerLoop", func(t *testing.T) {
		t.Parallel()
		// s2 has multiple values for the same key - this creates inner loop iterations
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("a", "x"), NewPair("a", "y"), NewPair("a", "z"))

		result := LeftJoinWith(s1, s2, "default").Limit(1).Collect()
		assert.Len(t, result, 1, "LeftJoinWith should respect Limit in inner loop")
		assert.Equal(t, "a", result[0].Key, "LeftJoinWith should preserve key")
		assert.Equal(t, 1, result[0].Left, "LeftJoinWith should preserve left value")
	})
}

// --- RightJoinWith Tests ---

func TestRightJoinWith(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := RightJoinWith(s1, s2, 0).Collect()

		assert.Len(t, result, 3, "RightJoinWith should return one row per right key")
		for _, r := range result {
			if r.Key == "c" {
				assert.Equal(t, 0, r.Left, "RightJoinWith should use default for missing left value")
			}
		}
	})

	t.Run("EmptyLeftStreamUsesDefault", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := RightJoinWith(s1, s2, -1).Collect()
		assert.Len(t, result, 2, "SemiJoin should keep keys present in right")
		for _, r := range result {
			assert.Equal(t, -1, r.Left, "RightJoinWith should use default for empty left stream")
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := RightJoinWith(s1, s2, 0).Limit(1).Collect()
		assert.Len(t, result, 1, "RightJoinWith should respect Limit")
	})

	t.Run("EarlyTerminationInInnerLoop", func(t *testing.T) {
		t.Parallel()
		// s1 has multiple values for the same key - this creates inner loop iterations
		s1 := PairsOf(NewPair("a", 1), NewPair("a", 2), NewPair("a", 3))
		s2 := PairsOf(NewPair("a", "x"))

		result := RightJoinWith(s1, s2, 0).Limit(1).Collect()
		assert.Len(t, result, 1, "RightJoinWith should respect Limit in inner loop")
		assert.Equal(t, "a", result[0].Key, "RightJoinWith should preserve key")
		assert.Equal(t, "x", result[0].Right, "RightJoinWith should preserve right value")
	})
}

// --- CoGroup Tests ---

func TestCoGroup(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("b", "y"), NewPair("c", "z"))

		result := CoGroup(s1, s2).Collect()

		assert.Len(t, result, 3, "CoGroup should return one row per distinct key") // a, b, c

		resultMap := make(map[string]CoGrouped[string, int, string])
		for _, r := range result {
			resultMap[r.Key] = r
		}

		assert.Equal(t, []int{1}, resultMap["a"].Left, "CoGroup should keep left values for key a")
		assert.Empty(t, resultMap["a"].Right, "CoGroup should have empty right for key a")

		assert.Equal(t, []int{2}, resultMap["b"].Left, "CoGroup should keep left values for key b")
		assert.Equal(t, []string{"y"}, resultMap["b"].Right, "CoGroup should keep right values for key b")

		assert.Empty(t, resultMap["c"].Left, "CoGroup should have empty left for key c")
		assert.Equal(t, []string{"z"}, resultMap["c"].Right, "CoGroup should keep right values for key c")
	})

	t.Run("EmptyBothStreams", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := Empty2[string, string]()

		result := CoGroup(s1, s2).Collect()
		assert.Empty(t, result, "SemiJoin with empty left should be empty")
	})

	t.Run("MultipleValuesPerKey", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("a", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("a", "y"))

		result := CoGroup(s1, s2).Collect()
		assert.Len(t, result, 1, "CoGroup should return one row for shared key")
		assert.Len(t, result[0].Left, 2, "CoGroup should include both left values")
		assert.Len(t, result[0].Right, 2, "CoGroup should include both right values")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("d", "x"), NewPair("e", "y"), NewPair("f", "z"))

		result := CoGroup(s1, s2).Limit(2).Collect()
		assert.Len(t, result, 2, "CoGroup should respect Limit")
	})
}

// --- JoinBy Tests ---

func TestJoinBy(t *testing.T) {
	t.Parallel()
	type Person struct {
		ID   int
		Name string
	}
	type Order struct {
		ID       int
		PersonID int
		Amount   float64
	}

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
		)
		orders := Of(
			Order{ID: 1, PersonID: 1, Amount: 100.0},
			Order{ID: 2, PersonID: 1, Amount: 50.0},
			Order{ID: 3, PersonID: 3, Amount: 75.0}, // No matching person
		)

		result := JoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()

		assert.Len(t, result, 2, "JoinBy should match orders for Alice") // Alice has 2 orders
		for _, r := range result {
			assert.Equal(t, "Alice", r.First.Name, "JoinBy left should be person Alice")
		}
	})

	t.Run("EmptyStreams", func(t *testing.T) {
		t.Parallel()
		people := Empty[Person]()
		orders := Of(Order{ID: 1, PersonID: 1, Amount: 100.0})

		result := JoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Empty(t, result, "JoinBy with empty people should be empty")
	})

	t.Run("NoMatches", func(t *testing.T) {
		t.Parallel()
		people := Of(Person{ID: 1, Name: "Alice"})
		orders := Of(Order{ID: 1, PersonID: 99, Amount: 100.0})

		result := JoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Empty(t, result, "JoinBy with no matching person ids should be empty")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
		)
		orders := Of(
			Order{ID: 1, PersonID: 1, Amount: 100.0},
			Order{ID: 2, PersonID: 1, Amount: 50.0},
			Order{ID: 3, PersonID: 2, Amount: 75.0},
		)

		result := JoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Limit(1).Collect()
		assert.Len(t, result, 1, "JoinBy should respect Limit")
	})
}

// --- LeftJoinBy Tests ---

func TestLeftJoinBy(t *testing.T) {
	t.Parallel()
	type Person struct {
		ID   int
		Name string
	}
	type Order struct {
		ID       int
		PersonID int
		Amount   float64
	}

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
		)
		orders := Of(
			Order{ID: 1, PersonID: 1, Amount: 100.0},
		)

		result := LeftJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()

		assert.Len(t, result, 2, "LeftJoinBy should return one row per left element")
		for _, r := range result {
			if r.First.Name == "Alice" {
				assert.True(t, r.Second.IsPresent(), "LeftJoinBy right should be present for Alice")
			} else {
				assert.False(t, r.Second.IsPresent(), "LeftJoinBy right should be None for Bob")
			}
		}
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		people := Of(Person{ID: 1, Name: "Alice"})
		orders := Empty[Order]()

		result := LeftJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Len(t, result, 1, "LeftJoinBy with empty orders should still return left rows")
		assert.False(t, result[0].Second.IsPresent(), "LeftJoinBy right should be None with empty orders")
	})

	t.Run("AllMatched", func(t *testing.T) {
		t.Parallel()
		people := Of(Person{ID: 1, Name: "Alice"})
		orders := Of(Order{ID: 1, PersonID: 1, Amount: 100.0})

		result := LeftJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Len(t, result, 1, "LeftJoinBy with all matched should return one row")
		assert.True(t, result[0].Second.IsPresent(), "LeftJoinBy right should be present")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
			Person{ID: 3, Name: "Charlie"},
		)
		orders := Empty[Order]()

		result := LeftJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Limit(2).Collect()
		assert.Len(t, result, 2, "LeftJoinBy should respect Limit")
	})

	t.Run("EarlyTerminationInInnerLoop", func(t *testing.T) {
		t.Parallel()
		// Multiple orders for the same person creates inner loop iterations
		people := Of(Person{ID: 1, Name: "Alice"})
		orders := Of(
			Order{ID: 1, PersonID: 1, Amount: 100.0},
			Order{ID: 2, PersonID: 1, Amount: 50.0},
			Order{ID: 3, PersonID: 1, Amount: 25.0},
		)

		result := LeftJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Limit(1).Collect()
		assert.Len(t, result, 1, "LeftJoinBy early termination in inner loop should return 1")
		assert.Equal(t, "Alice", result[0].First.Name, "LeftJoinBy left should be Alice")
		assert.True(t, result[0].Second.IsPresent(), "LeftJoinBy right should be present")
	})
}

// --- SemiJoin Tests ---

func TestSemiJoin(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("c", "z"))

		result := SemiJoin(s1, s2).CollectPairs()

		assert.Len(t, result, 2, "SemiJoin should keep only matching left keys")
		keys := make([]string, len(result))
		for i, p := range result {
			keys[i] = p.First
		}
		sort.Strings(keys)
		assert.Equal(t, []string{"a", "c"}, keys, "SemiJoin should return keys present in right")
	})

	t.Run("EmptyLeftStream", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"))

		result := SemiJoin(s1, s2).CollectPairs()
		assert.Empty(t, result, "SemiJoin with empty right should be empty")
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := Empty2[string, string]()

		result := SemiJoin(s1, s2).CollectPairs()
		assert.Empty(t, result, "SemiJoin with no overlaps should be empty")
	})

	t.Run("NoMatchingKeys", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1))
		s2 := PairsOf(NewPair("b", "x"))

		result := SemiJoin(s1, s2).CollectPairs()
		assert.Empty(t, result, "SemiJoin with no matching keys should be empty")
	})

	t.Run("AllMatchingKeys", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := SemiJoin(s1, s2).CollectPairs()
		assert.Len(t, result, 2, "SemiJoin with all matches should return all")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"), NewPair("c", "z"))

		result := SemiJoin(s1, s2).Limit(1).CollectPairs()
		assert.Len(t, result, 1, "SemiJoin should respect Limit")
	})
}

// --- AntiJoin Tests ---

func TestAntiJoin(t *testing.T) {
	t.Parallel()
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := PairsOf(NewPair("a", "x"), NewPair("c", "z"))

		result := AntiJoin(s1, s2).CollectPairs()

		assert.Len(t, result, 1, "AntiJoin should keep non-matching left keys only one")
		assert.Equal(t, "b", result[0].First, "AntiJoin kept key should be 'b'")
		assert.Equal(t, 2, result[0].Second, "AntiJoin kept value should be 2")
	})

	t.Run("EmptyLeftStream", func(t *testing.T) {
		t.Parallel()
		s1 := Empty2[string, int]()
		s2 := PairsOf(NewPair("a", "x"))

		result := AntiJoin(s1, s2).CollectPairs()
		assert.Empty(t, result, "AntiJoin with empty left should be empty")
	})

	t.Run("EmptyRightStream", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := Empty2[string, string]()

		result := AntiJoin(s1, s2).CollectPairs()
		assert.Len(t, result, 2, "AntiJoin with empty right should return all left rows")
	})

	t.Run("AllMatchingKeysReturnsEmpty", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("a", "x"), NewPair("b", "y"))

		result := AntiJoin(s1, s2).CollectPairs()
		assert.Empty(t, result, "AntiJoin with all matches should be empty")
	})

	t.Run("NoMatchingKeysReturnsAll", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2))
		s2 := PairsOf(NewPair("c", "x"), NewPair("d", "y"))

		result := AntiJoin(s1, s2).CollectPairs()
		assert.Len(t, result, 2, "AntiJoin with no matches should return all")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		s1 := PairsOf(NewPair("a", 1), NewPair("b", 2), NewPair("c", 3))
		s2 := Empty2[string, string]()

		result := AntiJoin(s1, s2).Limit(1).CollectPairs()
		assert.Len(t, result, 1, "AntiJoin should respect Limit")
	})
}

// --- SemiJoinBy Tests ---

func TestSemiJoinBy(t *testing.T) {
	t.Parallel()
	type Person struct {
		ID   int
		Name string
	}
	type Order struct {
		ID       int
		PersonID int
	}

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
			Person{ID: 3, Name: "Charlie"},
		)
		orders := Of(
			Order{ID: 1, PersonID: 1},
			Order{ID: 2, PersonID: 3},
		)

		result := SemiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()

		assert.Len(t, result, 2, "SemiJoinBy should return only people with matching orders")
		names := make([]string, len(result))
		for i, p := range result {
			names[i] = p.Name
		}
		sort.Strings(names)
		assert.Equal(t, []string{"Alice", "Charlie"}, names, "SemiJoinBy names should be sorted [Alice Charlie]")
	})

	t.Run("EmptyStreams", func(t *testing.T) {
		t.Parallel()
		people := Empty[Person]()
		orders := Of(Order{ID: 1, PersonID: 1})

		result := SemiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Empty(t, result, "SemiJoinBy with empty left should be empty")
	})

	t.Run("NoMatches", func(t *testing.T) {
		t.Parallel()
		people := Of(Person{ID: 1, Name: "Alice"})
		orders := Of(Order{ID: 1, PersonID: 99})

		result := SemiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Empty(t, result, "SemiJoinBy with no matches should be empty")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
		)
		orders := Of(Order{ID: 1, PersonID: 1}, Order{ID: 2, PersonID: 2})

		result := SemiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Limit(1).Collect()
		assert.Len(t, result, 1, "SemiJoinBy should respect Limit")
	})
}

// --- AntiJoinBy Tests ---

func TestAntiJoinBy(t *testing.T) {
	t.Parallel()
	type Person struct {
		ID   int
		Name string
	}
	type Order struct {
		ID       int
		PersonID int
	}

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
			Person{ID: 3, Name: "Charlie"},
		)
		orders := Of(
			Order{ID: 1, PersonID: 1},
			Order{ID: 2, PersonID: 3},
		)

		result := AntiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()

		assert.Len(t, result, 1, "AntiJoinBy should keep only people with no orders")
		assert.Equal(t, "Bob", result[0].Name, "AntiJoinBy remaining person should be Bob")
	})

	t.Run("EmptyRightStreamReturnsAll", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
		)
		orders := Empty[Order]()

		result := AntiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Len(t, result, 2, "AntiJoinBy with empty right should return all left")
	})

	t.Run("AllMatchedReturnsEmpty", func(t *testing.T) {
		t.Parallel()
		people := Of(Person{ID: 1, Name: "Alice"})
		orders := Of(Order{ID: 1, PersonID: 1})

		result := AntiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Collect()
		assert.Empty(t, result, "AntiJoinBy with all matched should be empty")
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		t.Parallel()
		people := Of(
			Person{ID: 1, Name: "Alice"},
			Person{ID: 2, Name: "Bob"},
			Person{ID: 3, Name: "Charlie"},
		)
		orders := Empty[Order]()

		result := AntiJoinBy(people, orders, func(p Person) int { return p.ID }, func(o Order) int { return o.PersonID }).Limit(1).Collect()
		assert.Len(t, result, 1, "AntiJoinBy should respect Limit")
	})
}

// --- CoGrouped Type Tests ---

func TestCoGroupedType(t *testing.T) {
	t.Parallel()
	t.Run("CoGroupedFields", func(t *testing.T) {
		t.Parallel()
		grouped := CoGrouped[string, int, string]{
			Key:   "test",
			Left:  []int{1, 2, 3},
			Right: []string{"a", "b"},
		}
		assert.Equal(t, "test", grouped.Key, "CoGrouped.Key should match")
		assert.Equal(t, []int{1, 2, 3}, grouped.Left, "CoGrouped.Left should match")
		assert.Equal(t, []string{"a", "b"}, grouped.Right, "CoGrouped.Right should match")
	})
}
