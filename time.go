package streams

import (
	"context"
	"sync"
	"time"
)

// --- Time-Based Windowing ---

// TimestampedValue holds a value with its timestamp.
type TimestampedValue[T any] struct {
	Value     T
	Timestamp time.Time
}

// NewTimestamped creates a TimestampedValue with the given value and current time.
func NewTimestamped[T any](value T) TimestampedValue[T] {
	return TimestampedValue[T]{Value: value, Timestamp: time.Now()}
}

// NewTimestampedAt creates a TimestampedValue with the given value and timestamp.
func NewTimestampedAt[T any](value T, ts time.Time) TimestampedValue[T] {
	return TimestampedValue[T]{Value: value, Timestamp: ts}
}

// WithTimestamp adds the current timestamp to each element.
func WithTimestamp[T any](s Stream[T]) Stream[TimestampedValue[T]] {
	return Stream[TimestampedValue[T]]{
		seq: func(yield func(TimestampedValue[T]) bool) {
			for v := range s.seq {
				if !yield(NewTimestamped(v)) {
					return
				}
			}
		},
	}
}

// TumblingTimeWindow groups elements into fixed-duration, non-overlapping windows.
// Elements are collected based on their arrival time (wall clock).
// This is a blocking operation that runs until the context is cancelled or timeout.
func TumblingTimeWindow[T any](ctx context.Context, s Stream[T], windowSize time.Duration) Stream[[]T] {
	return Stream[[]T]{
		seq: func(yield func([]T) bool) {
			ticker := time.NewTicker(windowSize)
			defer ticker.Stop()

			var window []T
			var mu sync.Mutex
			done := make(chan struct{})

			// Consumer goroutine that collects elements
			go func() {
				defer close(done)
				for v := range s.seq {
					select {
					case <-ctx.Done():
						return
					default:
						mu.Lock()
						window = append(window, v)
						mu.Unlock()
					}
				}
			}()

			// Yield windows at regular intervals
			for {
				select {
				case <-ctx.Done():
					// Yield remaining elements
					mu.Lock()
					if len(window) > 0 {
						yield(window)
					}
					mu.Unlock()
					return
				case <-done:
					// Source exhausted, yield remaining
					mu.Lock()
					if len(window) > 0 {
						yield(window)
					}
					mu.Unlock()
					return
				case <-ticker.C:
					mu.Lock()
					if len(window) > 0 {
						currentWindow := window
						window = nil
						mu.Unlock()
						if !yield(currentWindow) {
							return
						}
					} else {
						mu.Unlock()
					}
				}
			}
		},
	}
}

// SlidingTimeWindow groups elements into overlapping windows based on time.
// windowSize is the duration of each window, slideInterval is how often a new window starts.
func SlidingTimeWindow[T any](ctx context.Context, s Stream[T], windowSize, slideInterval time.Duration) Stream[[]T] {
	return Stream[[]T]{
		seq: func(yield func([]T) bool) {
			ticker := time.NewTicker(slideInterval)
			defer ticker.Stop()

			var elements []TimestampedValue[T]
			var mu sync.Mutex
			done := make(chan struct{})

			// Consumer goroutine
			go func() {
				defer close(done)
				for v := range s.seq {
					select {
					case <-ctx.Done():
						return
					default:
						mu.Lock()
						elements = append(elements, NewTimestamped(v))
						mu.Unlock()
					}
				}
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					// Yield final window
					mu.Lock()
					if len(elements) > 0 {
						window := make([]T, len(elements))
						for i, e := range elements {
							window[i] = e.Value
						}
						mu.Unlock()
						yield(window)
					} else {
						mu.Unlock()
					}
					return
				case <-ticker.C:
					now := time.Now()
					cutoff := now.Add(-windowSize)

					mu.Lock()
					// Remove old elements
					var newElements []TimestampedValue[T]
					var window []T
					for _, e := range elements {
						if e.Timestamp.After(cutoff) {
							newElements = append(newElements, e)
							window = append(window, e.Value)
						}
					}
					elements = newElements
					mu.Unlock()

					if len(window) > 0 {
						if !yield(window) {
							return
						}
					}
				}
			}
		},
	}
}

// SessionWindow groups elements into sessions separated by gaps of inactivity.
// A new session starts when no elements arrive within the gap duration.
func SessionWindow[T any](ctx context.Context, s Stream[T], gap time.Duration) Stream[[]T] {
	return Stream[[]T]{
		seq: func(yield func([]T) bool) {
			var session []T
			var mu sync.Mutex
			var lastActivity time.Time
			done := make(chan struct{})
			elementCh := make(chan T)

			// Source reader
			go func() {
				defer close(done)
				defer close(elementCh)
				for v := range s.seq {
					select {
					case <-ctx.Done():
						return
					case elementCh <- v:
					}
				}
			}()

			timer := time.NewTimer(gap)
			defer timer.Stop()

			for {
				select {
				case <-ctx.Done():
					mu.Lock()
					if len(session) > 0 {
						yield(session)
					}
					mu.Unlock()
					return

				case v, ok := <-elementCh:
					if !ok {
						// Source exhausted
						mu.Lock()
						if len(session) > 0 {
							yield(session)
						}
						mu.Unlock()
						return
					}
					mu.Lock()
					session = append(session, v)
					lastActivity = time.Now()
					mu.Unlock()
					// Stop timer and drain channel before reset to avoid ghost triggers
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(gap)

				case <-timer.C:
					mu.Lock()
					if len(session) > 0 && time.Since(lastActivity) >= gap {
						currentSession := session
						session = nil
						mu.Unlock()
						if !yield(currentSession) {
							return
						}
					} else {
						mu.Unlock()
					}
					timer.Reset(gap)
				}
			}
		},
	}
}

// --- Rate Limiting Operations ---

// Throttle ensures elements are emitted at most once per interval.
// Elements arriving faster are delayed; no elements are dropped.
func Throttle[T any](s Stream[T], interval time.Duration) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			var lastEmit time.Time
			for v := range s.seq {
				now := time.Now()
				if elapsed := now.Sub(lastEmit); elapsed < interval {
					time.Sleep(interval - elapsed)
				}
				lastEmit = time.Now()
				if !yield(v) {
					return
				}
			}
		},
	}
}

// ThrottleCtx is like Throttle but respects context cancellation.
func ThrottleCtx[T any](ctx context.Context, s Stream[T], interval time.Duration) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			var lastEmit time.Time
			for v := range s.seq {
				now := time.Now()
				if elapsed := now.Sub(lastEmit); elapsed < interval {
					select {
					case <-ctx.Done():
						return
					case <-time.After(interval - elapsed):
					}
				}
				lastEmit = time.Now()
				if !yield(v) {
					return
				}
			}
		},
	}
}

// RateLimit limits the stream to n elements per duration using a token bucket.
func RateLimit[T any](s Stream[T], n int, per time.Duration) Stream[T] {
	if n <= 0 {
		return Empty[T]()
	}
	return Stream[T]{
		seq: func(yield func(T) bool) {
			tokens := n
			refillInterval := per / time.Duration(n)
			lastRefill := time.Now()

			for v := range s.seq {
				// Refill tokens based on elapsed time
				now := time.Now()
				elapsed := now.Sub(lastRefill)
				newTokens := int(elapsed / refillInterval)
				if newTokens > 0 {
					tokens = min(tokens+newTokens, n)
					lastRefill = now
				}

				// Wait for token if none available
				if tokens <= 0 {
					time.Sleep(refillInterval)
					tokens = 1
					lastRefill = time.Now()
				}

				tokens--
				if !yield(v) {
					return
				}
			}
		},
	}
}

// RateLimitCtx is like RateLimit but respects context cancellation.
func RateLimitCtx[T any](ctx context.Context, s Stream[T], n int, per time.Duration) Stream[T] {
	if n <= 0 {
		return Empty[T]()
	}
	return Stream[T]{
		seq: func(yield func(T) bool) {
			tokens := n
			refillInterval := per / time.Duration(n)
			lastRefill := time.Now()

			for v := range s.seq {
				select {
				case <-ctx.Done():
					return
				default:
				}

				now := time.Now()
				elapsed := now.Sub(lastRefill)
				newTokens := int(elapsed / refillInterval)
				if newTokens > 0 {
					tokens = min(tokens+newTokens, n)
					lastRefill = now
				}

				if tokens <= 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(refillInterval):
					}
					tokens = 1
					lastRefill = time.Now()
				}

				tokens--
				if !yield(v) {
					return
				}
			}
		},
	}
}

// --- Debouncing ---

// Debounce emits an element only after a quiet period with no new elements.
// Useful for coalescing rapid updates into a single emission.
func Debounce[T any](ctx context.Context, s Stream[T], quiet time.Duration) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			inputCh := make(chan T)
			done := make(chan struct{})

			go func() {
				defer close(done)
				defer close(inputCh)
				for v := range s.seq {
					select {
					case <-ctx.Done():
						return
					case inputCh <- v:
					}
				}
			}()

			timer := time.NewTimer(quiet)
			defer timer.Stop()
			timer.Stop() // Don't fire initially

			var lastValue T
			hasValue := false

			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-inputCh:
					if !ok {
						// Input exhausted - emit last value if pending
						if hasValue {
							yield(lastValue)
						}
						return
					}
					lastValue = v
					hasValue = true
					// Stop timer and drain channel before reset to avoid ghost triggers
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(quiet)
				case <-timer.C:
					if hasValue {
						if !yield(lastValue) {
							return
						}
						hasValue = false
					}
				}
			}
		},
	}
}

// Sample emits the most recent element at regular intervals.
// Elements arriving between samples are dropped.
func Sample[T any](ctx context.Context, s Stream[T], interval time.Duration) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			var lastValue T
			var mu sync.Mutex
			hasValue := false
			done := make(chan struct{})

			go func() {
				defer close(done)
				for v := range s.seq {
					select {
					case <-ctx.Done():
						return
					default:
						mu.Lock()
						lastValue = v
						hasValue = true
						mu.Unlock()
					}
				}
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					// Emit final sample if available
					mu.Lock()
					if hasValue {
						yield(lastValue)
					}
					mu.Unlock()
					return
				case <-ticker.C:
					mu.Lock()
					if hasValue {
						v := lastValue
						hasValue = false
						mu.Unlock()
						if !yield(v) {
							return
						}
					} else {
						mu.Unlock()
					}
				}
			}
		},
	}
}

// --- Delay Operations ---

// Delay delays each element by the specified duration.
func Delay[T any](s Stream[T], duration time.Duration) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			for v := range s.seq {
				time.Sleep(duration)
				if !yield(v) {
					return
				}
			}
		},
	}
}

// DelayCtx is like Delay but respects context cancellation.
func DelayCtx[T any](ctx context.Context, s Stream[T], duration time.Duration) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			for v := range s.seq {
				select {
				case <-ctx.Done():
					return
				case <-time.After(duration):
					if !yield(v) {
						return
					}
				}
			}
		},
	}
}

// --- Timeout Operations ---

// Timeout returns an error if no element is received within the duration.
func Timeout[T any](ctx context.Context, s Stream[T], timeout time.Duration) Stream[Result[T]] {
	return Stream[Result[T]]{
		seq: func(yield func(Result[T]) bool) {
			elementCh := make(chan T)
			done := make(chan struct{})

			go func() {
				defer close(done)
				defer close(elementCh)
				for v := range s.seq {
					select {
					case <-ctx.Done():
						return
					case elementCh <- v:
					}
				}
			}()

			timer := time.NewTimer(timeout)
			defer timer.Stop()

			for {
				select {
				case <-ctx.Done():
					yield(Err[T](ctx.Err()))
					return
				case v, ok := <-elementCh:
					if !ok {
						return
					}
					// Stop timer and drain channel before reset to avoid ghost triggers
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(timeout)
					if !yield(Ok(v)) {
						return
					}
				case <-timer.C:
					if !yield(Err[T](context.DeadlineExceeded)) {
						return
					}
					timer.Reset(timeout)
				}
			}
		},
	}
}

// --- Interval Stream ---

// Interval creates a Stream that emits sequential integers at regular intervals.
// Starts from 0 and increments by 1 each interval.
func Interval(ctx context.Context, interval time.Duration) Stream[int] {
	return Stream[int]{
		seq: func(yield func(int) bool) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			i := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if !yield(i) {
						return
					}
					i++
				}
			}
		},
	}
}

// Timer creates a Stream that emits a single value after the specified duration.
func Timer[T any](ctx context.Context, duration time.Duration, value T) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
				yield(value)
			}
		},
	}
}
