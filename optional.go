package streams

// Optional represents a value that may or may not exist.
// It provides a type-safe alternative to using nil pointers or zero values.
type Optional[T any] struct {
	value   T
	present bool
}

// Some creates an Optional containing the given value.
func Some[T any](value T) Optional[T] {
	return Optional[T]{value: value, present: true}
}

// None creates an empty Optional.
func None[T any]() Optional[T] {
	return Optional[T]{}
}

// OptionalOf creates an Optional from a pointer.
// If the pointer is nil, returns None; otherwise returns Some(*ptr).
func OptionalOf[T any](ptr *T) Optional[T] {
	if ptr == nil {
		return None[T]()
	}
	return Some(*ptr)
}

// OptionalFromCondition creates an Optional based on a condition.
// If the condition is true, returns Some(value); otherwise returns None.
func OptionalFromCondition[T any](condition bool, value T) Optional[T] {
	if condition {
		return Some(value)
	}
	return None[T]()
}

// IsPresent returns true if the Optional contains a value.
func (o Optional[T]) IsPresent() bool {
	return o.present
}

// IsEmpty returns true if the Optional is empty (no value).
func (o Optional[T]) IsEmpty() bool {
	return !o.present
}

// Get returns the value if present, or panics if empty.
// Use GetOrElse or GetOrElseGet for safe access.
func (o Optional[T]) Get() T {
	if !o.present {
		panic("Optional.Get: no value present")
	}
	return o.value
}

// GetOrElse returns the value if present, or the given default value.
func (o Optional[T]) GetOrElse(defaultVal T) T {
	if o.present {
		return o.value
	}
	return defaultVal
}

// GetOrElseGet returns the value if present, or computes a default using the supplier.
func (o Optional[T]) GetOrElseGet(supplier func() T) T {
	if o.present {
		return o.value
	}
	return supplier()
}

// GetOrZero returns the value if present, or the zero value of T.
func (o Optional[T]) GetOrZero() T {
	return o.value
}

// IfPresent calls the action with the value if present.
func (o Optional[T]) IfPresent(action func(T)) {
	if o.present {
		action(o.value)
	}
}

// IfPresentOrElse calls the action with the value if present, or calls emptyAction.
func (o Optional[T]) IfPresentOrElse(action func(T), emptyAction func()) {
	if o.present {
		action(o.value)
	} else {
		emptyAction()
	}
}

// Filter returns the Optional if present and the predicate returns true; otherwise None.
func (o Optional[T]) Filter(pred func(T) bool) Optional[T] {
	if o.present && pred(o.value) {
		return o
	}
	return None[T]()
}

// Map transforms the value if present.
// For type-changing transformations, use OptionalMap function instead.
func (o Optional[T]) Map(fn func(T) T) Optional[T] {
	if o.present {
		return Some(fn(o.value))
	}
	return None[T]()
}

// OrElse returns this Optional if present, or the other Optional.
func (o Optional[T]) OrElse(other Optional[T]) Optional[T] {
	if o.present {
		return o
	}
	return other
}

// OrElseGet returns this Optional if present, or computes another Optional using the supplier.
func (o Optional[T]) OrElseGet(supplier func() Optional[T]) Optional[T] {
	if o.present {
		return o
	}
	return supplier()
}

// ToSlice returns a slice containing the value if present, or an empty slice.
func (o Optional[T]) ToSlice() []T {
	if o.present {
		return []T{o.value}
	}
	return nil
}

// ToPointer returns a pointer to the value if present, or nil.
func (o Optional[T]) ToPointer() *T {
	if o.present {
		return &o.value
	}
	return nil
}

// ToStream returns a Stream containing the value if present, or an empty Stream.
func (o Optional[T]) ToStream() Stream[T] {
	if o.present {
		return Of(o.value)
	}
	return Empty[T]()
}

// String returns a string representation of the Optional.
func (o Optional[T]) String() string {
	if o.present {
		return "Some(...)"
	}
	return "None"
}

// --- Free functions for Optional type transformation ---

// OptionalMap transforms Optional[T] to Optional[U].
// Use this when the transformation changes the type.
func OptionalMap[T, U any](o Optional[T], fn func(T) U) Optional[U] {
	if o.present {
		return Some(fn(o.value))
	}
	return None[U]()
}

// OptionalFlatMap transforms Optional[T] to Optional[U] where the function returns an Optional.
func OptionalFlatMap[T, U any](o Optional[T], fn func(T) Optional[U]) Optional[U] {
	if o.present {
		return fn(o.value)
	}
	return None[U]()
}

// OptionalZip combines two Optionals into an Optional of Pair.
// Returns None if either Optional is empty.
func OptionalZip[T, U any](o1 Optional[T], o2 Optional[U]) Optional[Pair[T, U]] {
	if o1.present && o2.present {
		return Some(Pair[T, U]{First: o1.value, Second: o2.value})
	}
	return None[Pair[T, U]]()
}

// OptionalEquals checks if two Optionals are equal.
// Two Optionals are equal if both are empty, or both are present with equal values.
func OptionalEquals[T comparable](o1, o2 Optional[T]) bool {
	if o1.present != o2.present {
		return false
	}
	if !o1.present {
		return true
	}
	return o1.value == o2.value
}
