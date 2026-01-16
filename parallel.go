package streams

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// --- Parallel Configuration ---
//

// ParallelConfig holds configuration for parallel operations.
type ParallelConfig struct {
	Concurrency int  // Number of concurrent workers
	Ordered     bool // Whether to preserve input order
	BufferSize  int  // Size of output buffer
	ChunkSize   int  // Chunk size for chunked reordering (0 = disabled, uses streaming mode)
}

// DefaultParallelConfig returns the default parallel configuration.
func DefaultParallelConfig() ParallelConfig {
	return ParallelConfig{
		Concurrency: runtime.NumCPU(),
		Ordered:     true,
		BufferSize:  runtime.NumCPU() * 2,
		ChunkSize:   0, // Disabled by default (streaming mode)
	}
}

// ParallelOption is a function that modifies ParallelConfig.
type ParallelOption func(*ParallelConfig)

// WithConcurrency sets the number of concurrent workers.
func WithConcurrency(n int) ParallelOption {
	return func(c *ParallelConfig) {
		if n > 0 {
			c.Concurrency = n
		}
	}
}

// WithOrdered sets whether to preserve input order.
func WithOrdered(ordered bool) ParallelOption {
	return func(c *ParallelConfig) {
		c.Ordered = ordered
	}
}

// WithBufferSize sets the output buffer size.
func WithBufferSize(size int) ParallelOption {
	return func(c *ParallelConfig) {
		if size > 0 {
			c.BufferSize = size
		}
	}
}

// WithChunkSize sets the chunk size for chunked reordering in ordered parallel operations.
// When set to a value > 0, ordered operations will process elements in chunks,
// limiting memory usage by only buffering up to ChunkSize results at a time.
// Set to 0 (default) to use streaming mode which may buffer all out-of-order results.
//
// Trade-off: Smaller chunk sizes reduce memory but may underutilize parallelism.
// WithChunkSize(1) provides minimum memory usage but processes sequentially within each chunk.
// A good starting point is 2-4x the concurrency level.
func WithChunkSize(size int) ParallelOption {
	return func(c *ParallelConfig) {
		if size >= 0 {
			c.ChunkSize = size
		}
	}
}

// --- Parallel Map ---

// indexedValue holds a value with its original index for ordered processing.
type indexedValue[T any] struct {
	index int
	value T
}

// ParallelMap transforms each element using the given function in parallel.
// By default, it preserves the input order.
func ParallelMap[T, U any](s Stream[T], fn func(T) U, opts ...ParallelOption) Stream[U] {
	return ParallelMapCtx(context.Background(), s, func(_ context.Context, v T) U {
		return fn(v)
	}, opts...)
}

// ParallelMapCtx transforms each element using the given function in parallel with context support.
// The context passed to fn is the same as the ctx parameter, allowing for cancellation checks.
func ParallelMapCtx[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) U, opts ...ParallelOption) Stream[U] {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Ordered {
		return parallelMapOrdered(ctx, s, fn, cfg)
	}
	return parallelMapUnordered(ctx, s, fn, cfg)
}

// parallelMapOrdered processes elements in parallel while preserving order.
func parallelMapOrdered[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) U, cfg ParallelConfig) Stream[U] {
	if cfg.ChunkSize > 0 {
		return parallelMapOrderedChunked(ctx, s, fn, cfg)
	}
	return parallelMapOrderedStreaming(ctx, s, fn, cfg)
}

// parallelMapOrderedStreaming preserves order but may buffer many out-of-order results (unbounded).
func parallelMapOrderedStreaming[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) U, cfg ParallelConfig) Stream[U] {
	return Stream[U]{
		seq: func(yield func(U) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				inputCh  = make(chan indexedValue[T], cfg.BufferSize)
				outputCh = make(chan indexedValue[U], cfg.BufferSize)
				done     = make(chan struct{})
				closed   atomic.Bool
				wg       sync.WaitGroup
				feedWg   sync.WaitGroup // Track feed goroutine for safe stop()
			)

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for range cfg.Concurrency {
				wg.Go(func() {
					for {
						select {
						case <-done:
							return
						case item, ok := <-inputCh:
							if !ok {
								return
							}
							result := fn(ctx, item.value)
							select {
							case <-done:
								return
							case outputCh <- indexedValue[U]{index: item.index, value: result}:
							}
						}
					}
				})
			}

			go func() { // separate closer: we don't want to tie to previous wg job
				wg.Wait()
				close(outputCh)
			}()

			feedWg.Go(func() {
				defer close(inputCh)
				idx := 0
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case inputCh <- indexedValue[T]{index: idx, value: v}:
						idx++
					}
				}
			})

			// Ensure feed goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			results := make(map[int]U)
			nextIdx := 0

			for item := range outputCh {
				results[item.index] = item.value

				for {
					if v, ok := results[nextIdx]; ok {
						if !yield(v) {
							// Signal early termination
							if closed.CompareAndSwap(false, true) {
								close(done)
							}
							// Drain remaining items to prevent goroutine leak
							for range outputCh {
							}
							return
						}
						delete(results, nextIdx)
						nextIdx++
					} else {
						break
					}
				}
			}
		},
	}
}

// parallelMapOrderedChunked processes inputs in fixed-size chunks to bound memory usage.
// Each chunk is processed in parallel up to cfg.Concurrency and yielded in order.
func parallelMapOrderedChunked[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) U, cfg ParallelConfig) Stream[U] {
	return Stream[U]{
		seq: func(yield func(U) bool) {
			next, stop := iter.Pull(s.seq)
			defer stop()

			done := make(chan struct{})
			var closed atomic.Bool

			// Ensure done channel is closed when function exits to prevent goroutine leak
			defer func() {
				if closed.CompareAndSwap(false, true) {
					close(done)
				}
			}()

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for {
				chunk := make([]T, 0, cfg.ChunkSize)
				for i := 0; i < cfg.ChunkSize; i++ {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						break
					}
					chunk = append(chunk, v)
				}
				if len(chunk) == 0 {
					return
				}

				results := make([]U, len(chunk))
				var (
					wg  sync.WaitGroup
					sem = make(chan struct{}, cfg.Concurrency)
				)

				for i, v := range chunk {
					select {
					case <-done:
						return
					case sem <- struct{}{}: // semaphore acquire with cancel check
					}
					wg.Go(func(idx int, val T) func() {
						return func() {
							defer func() { <-sem }() // semaphore release
							select {
							case <-done:
								return
							default:
							}
							results[idx] = fn(ctx, val)
						}
					}(i, v))
				}
				wg.Wait()

				for _, r := range results {
					if !yield(r) {
						if closed.CompareAndSwap(false, true) {
							close(done)
						}
						return
					}
				}
			}
		},
	}
}

// parallelMapUnordered processes elements in parallel without preserving order.
func parallelMapUnordered[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) U, cfg ParallelConfig) Stream[U] {
	return Stream[U]{
		seq: func(yield func(U) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				inputCh  = make(chan T, cfg.BufferSize)
				outputCh = make(chan U, cfg.BufferSize)
				done     = make(chan struct{})
				closed   atomic.Bool
				wg       sync.WaitGroup
				feedWg   sync.WaitGroup // Track feed goroutine for safe stop()
			)

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for range cfg.Concurrency {
				wg.Go(func() {
					for {
						select {
						case <-done:
							return
						case v, ok := <-inputCh:
							if !ok {
								return
							}
							result := fn(ctx, v)
							select {
							case <-done:
								return
							case outputCh <- result:
							}
						}
					}
				})
			}

			go func() { wg.Wait(); close(outputCh) }()

			feedWg.Go(func() {
				defer close(inputCh)
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case inputCh <- v:
					}
				}
			})

			// Ensure feed goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			for result := range outputCh {
				if !yield(result) {
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
					for range outputCh {
					}
					return
				}
			}
		},
	}
}

// --- Parallel Filter ---

// ParallelFilter filters elements using the given predicate in parallel.
// By default, it preserves the input order.
func ParallelFilter[T any](s Stream[T], pred func(T) bool, opts ...ParallelOption) Stream[T] {
	return ParallelFilterCtx(context.Background(), s, func(_ context.Context, v T) bool {
		return pred(v)
	}, opts...)
}

// ParallelFilterCtx filters elements using the given predicate in parallel with context support.
func ParallelFilterCtx[T any](ctx context.Context, s Stream[T], pred func(context.Context, T) bool, opts ...ParallelOption) Stream[T] {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Ordered {
		return parallelFilterOrdered(ctx, s, pred, cfg)
	}
	return parallelFilterUnordered(ctx, s, pred, cfg)
}

// filterResult holds a value and whether it passed the filter.
type filterResult[T any] struct {
	index  int
	value  T
	passed bool
}

// parallelFilterOrdered filters in parallel while preserving order.
func parallelFilterOrdered[T any](ctx context.Context, s Stream[T], pred func(context.Context, T) bool, cfg ParallelConfig) Stream[T] {
	if cfg.ChunkSize > 0 {
		return parallelFilterOrderedChunked(ctx, s, pred, cfg)
	}
	return parallelFilterOrderedStreaming(ctx, s, pred, cfg)
}

// parallelFilterOrderedStreaming preserves order but may buffer many out-of-order results (unbounded).
func parallelFilterOrderedStreaming[T any](ctx context.Context, s Stream[T], pred func(context.Context, T) bool, cfg ParallelConfig) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				inputCh  = make(chan indexedValue[T], cfg.BufferSize)
				outputCh = make(chan filterResult[T], cfg.BufferSize)
				done     = make(chan struct{})
				closed   atomic.Bool
				wg       sync.WaitGroup
				feedWg   sync.WaitGroup // Track feed goroutine for safe stop()
			)

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for range cfg.Concurrency {
				wg.Go(func() {
					for {
						select {
						case <-done:
							return
						case item, ok := <-inputCh:
							if !ok {
								return
							}
							passed := pred(ctx, item.value)
							select {
							case <-done:
								return
							case outputCh <- filterResult[T]{
								index:  item.index,
								value:  item.value,
								passed: passed,
							}:
							}
						}
					}
				})
			}

			go func() { wg.Wait(); close(outputCh) }()

			feedWg.Go(func() {
				defer close(inputCh)
				idx := 0
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case inputCh <- indexedValue[T]{index: idx, value: v}:
						idx++
					}
				}
			})

			// Ensure feed goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			results := make(map[int]filterResult[T])
			nextIdx := 0

			for item := range outputCh {
				results[item.index] = item

				for {
					if r, ok := results[nextIdx]; ok {
						if r.passed {
							if !yield(r.value) {
								if closed.CompareAndSwap(false, true) {
									close(done)
								}
								for range outputCh {
								}
								return
							}
						}
						delete(results, nextIdx)
						nextIdx++
					} else {
						break
					}
				}
			}
		},
	}
}

// parallelFilterOrderedChunked processes inputs in fixed-size chunks to bound memory usage.
func parallelFilterOrderedChunked[T any](ctx context.Context, s Stream[T], pred func(context.Context, T) bool, cfg ParallelConfig) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			next, stop := iter.Pull(s.seq)
			defer stop()

			done := make(chan struct{})
			var closed atomic.Bool

			// Ensure done channel is closed when function exits to prevent goroutine leak
			defer func() {
				if closed.CompareAndSwap(false, true) {
					close(done)
				}
			}()

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for {
				chunk := make([]T, 0, cfg.ChunkSize)
				for i := 0; i < cfg.ChunkSize; i++ {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						break
					}
					chunk = append(chunk, v)
				}
				if len(chunk) == 0 {
					return
				}

				type res struct {
					ok bool
					v  T
				}
				results := make([]res, len(chunk))
				var (
					wg  sync.WaitGroup
					sem = make(chan struct{}, cfg.Concurrency)
				)

				for i, v := range chunk {
					select {
					case <-done:
						return
					case sem <- struct{}{}: // semaphore acquire with cancel check
					}
					wg.Go(func(idx int, val T) func() {
						return func() {
							defer func() { <-sem }() // semaphore release
							select {
							case <-done:
								return
							default:
							}
							results[idx] = res{
								ok: pred(ctx, val),
								v:  val,
							}
						}
					}(i, v))
				}
				wg.Wait()

				for _, r := range results {
					if r.ok {
						if !yield(r.v) {
							if closed.CompareAndSwap(false, true) {
								close(done)
							}
							return
						}
					}
				}
			}
		},
	}
}

// parallelFilterUnordered filters in parallel without preserving order.
func parallelFilterUnordered[T any](ctx context.Context, s Stream[T], pred func(context.Context, T) bool, cfg ParallelConfig) Stream[T] {
	return Stream[T]{
		seq: func(yield func(T) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				inputCh  = make(chan T, cfg.BufferSize)
				outputCh = make(chan T, cfg.BufferSize)
				done     = make(chan struct{})
				closed   atomic.Bool
				wg       sync.WaitGroup
				feedWg   sync.WaitGroup // Track feed goroutine for safe stop()
			)

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for range cfg.Concurrency {
				wg.Go(func() {
					for {
						select {
						case <-done:
							return
						case v, ok := <-inputCh:
							if !ok {
								return
							}
							if pred(ctx, v) {
								select {
								case <-done:
									return
								case outputCh <- v:
								}
							}
						}
					}
				})
			}

			go func() { wg.Wait(); close(outputCh) }()

			feedWg.Go(func() {
				defer close(inputCh)
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case inputCh <- v:
					}
				}
			})

			// Ensure feed goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			for result := range outputCh {
				if !yield(result) {
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
					for range outputCh {
					}
					return
				}
			}
		},
	}
}

// --- Parallel FlatMap ---

// ParallelFlatMap maps each element to a stream and flattens the results in parallel.
func ParallelFlatMap[T, U any](s Stream[T], fn func(T) Stream[U], opts ...ParallelOption) Stream[U] {
	return ParallelFlatMapCtx(context.Background(), s, func(_ context.Context, v T) Stream[U] {
		return fn(v)
	}, opts...)
}

// ParallelFlatMapCtx maps each element to a stream and flattens the results in parallel with context support.
func ParallelFlatMapCtx[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) Stream[U], opts ...ParallelOption) Stream[U] {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Ordered {
		return parallelFlatMapOrdered(ctx, s, fn, cfg)
	}
	return parallelFlatMapUnordered(ctx, s, fn, cfg)
}

// flatMapResult holds all results from a single flatmap operation.
type flatMapResult[U any] struct {
	index  int
	values []U
}

// parallelFlatMapOrdered processes in parallel while preserving order.
// Note: This function collects each sub-stream into memory to preserve order.
// For large sub-streams, consider using parallelFlatMapUnordered instead.
// When ChunkSize > 0, uses chunked reordering to limit memory usage.
func parallelFlatMapOrdered[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) Stream[U], cfg ParallelConfig) Stream[U] {
	if cfg.ChunkSize > 0 {
		return parallelFlatMapOrderedChunked(ctx, s, fn, cfg)
	}
	return parallelFlatMapOrderedStreaming(ctx, s, fn, cfg)
}

// parallelFlatMapOrderedStreaming is the original streaming implementation.
// May buffer all out-of-order results in memory (worst case: O(N) sub-stream results
// when processing is highly unbalanced). Use chunked mode for bounded memory.
func parallelFlatMapOrderedStreaming[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) Stream[U], cfg ParallelConfig) Stream[U] {
	return Stream[U]{
		seq: func(yield func(U) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				inputCh  = make(chan indexedValue[T], cfg.BufferSize)
				outputCh = make(chan flatMapResult[U], cfg.BufferSize)
				done     = make(chan struct{})
				closed   atomic.Bool
				wg       sync.WaitGroup
				feedWg   sync.WaitGroup // Track feed goroutine for safe stop()
			)

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for range cfg.Concurrency {
				wg.Go(func() {
					for {
						select {
						case <-done:
							return
						case item, ok := <-inputCh:
							if !ok {
								return
							}
							// Collect sub-stream with cancellation support
							inner := fn(ctx, item.value)
							var values []U
							for u := range inner.seq {
								if closed.Load() {
									return // Stop collecting if downstream terminated
								}
								values = append(values, u)
							}
							select {
							case <-done:
								return
							case outputCh <- flatMapResult[U]{index: item.index, values: values}:
							}
						}
					}
				})
			}

			go func() { wg.Wait(); close(outputCh) }()

			feedWg.Go(func() {
				defer close(inputCh)
				idx := 0
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case inputCh <- indexedValue[T]{index: idx, value: v}:
						idx++
					}
				}
			})

			// Ensure feed goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			results := make(map[int][]U)
			nextIdx := 0

			for item := range outputCh {
				results[item.index] = item.values

				for {
					if values, ok := results[nextIdx]; ok {
						for _, v := range values {
							if !yield(v) {
								if closed.CompareAndSwap(false, true) {
									close(done)
								}
								for range outputCh {
								}
								return
							}
						}
						delete(results, nextIdx)
						nextIdx++
					} else {
						break
					}
				}
			}
		},
	}
}

// parallelFlatMapOrderedChunked processes in chunks to limit memory usage.
// Each chunk is fully processed before moving to the next, bounding memory to ChunkSize results.
func parallelFlatMapOrderedChunked[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) Stream[U], cfg ParallelConfig) Stream[U] {
	return Stream[U]{
		seq: func(yield func(U) bool) {
			next, stop := iter.Pull(s.seq)
			defer stop()

			done := make(chan struct{})
			var closed atomic.Bool

			// Ensure done channel is closed when function exits to prevent goroutine leak
			defer func() {
				if closed.CompareAndSwap(false, true) {
					close(done)
				}
			}()

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for {
				chunk := make([]T, 0, cfg.ChunkSize)
				for i := 0; i < cfg.ChunkSize; i++ {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						break
					}
					chunk = append(chunk, v)
				}

				if len(chunk) == 0 {
					return
				}

				results := make([][]U, len(chunk))
				var (
					wg  sync.WaitGroup
					sem = make(chan struct{}, cfg.Concurrency)
				)

				for i, v := range chunk {
					select {
					case <-done:
						return
					default:
					}

					select {
					case <-done:
						return
					case sem <- struct{}{}: // semaphore acquire with cancel check
					}
					wg.Go(func(idx int, val T) func() {
						return func() {
							defer func() { <-sem }() // semaphore release

							select {
							case <-done:
								return
							default:
							}

							inner := fn(ctx, val)
							var buf []U
							for u := range inner.seq {
								if closed.Load() {
									return
								}
								buf = append(buf, u)
							}
							results[idx] = buf
						}
					}(i, v))
				}

				wg.Wait()

				for _, values := range results {
					for _, v := range values {
						if !yield(v) {
							if closed.CompareAndSwap(false, true) {
								close(done)
							}
							return
						}
					}
				}
			}
		},
	}
}

// parallelFlatMapUnordered processes in parallel without preserving order.
func parallelFlatMapUnordered[T, U any](ctx context.Context, s Stream[T], fn func(context.Context, T) Stream[U], cfg ParallelConfig) Stream[U] {
	return Stream[U]{
		seq: func(yield func(U) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				inputCh  = make(chan T, cfg.BufferSize)
				outputCh = make(chan U, cfg.BufferSize)
				done     = make(chan struct{})
				closed   atomic.Bool
				wg       sync.WaitGroup
				feedWg   sync.WaitGroup // Track feed goroutine for safe stop()
			)

			// Monitor context cancellation
			go func() {
				select {
				case <-ctx.Done():
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
				case <-done:
				}
			}()

			for range cfg.Concurrency {
				wg.Go(func() {
					for {
						select {
						case <-done:
							return
						case v, ok := <-inputCh:
							if !ok {
								return
							}
							for u := range fn(ctx, v).seq {
								select {
								case <-done:
									return
								case outputCh <- u:
								}
							}
						}
					}
				})
			}

			go func() { wg.Wait(); close(outputCh) }()

			feedWg.Go(func() {
				defer close(inputCh)
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case inputCh <- v:
					}
				}
			})

			// Ensure feed goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			for result := range outputCh {
				if !yield(result) {
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
					for range outputCh {
					}
					return
				}
			}
		},
	}
}

// --- Prefetch ---

// Prefetch creates a Stream that prefetches n elements ahead in a goroutine.
// This decouples the producer from the consumer, allowing them to run concurrently.
func Prefetch[T any](s Stream[T], n int) Stream[T] {
	if n <= 0 {
		n = 1
	}
	return Stream[T]{
		seq: func(yield func(T) bool) {
			next, stop := iter.Pull(s.seq)

			var (
				ch     = make(chan T, n)
				done   = make(chan struct{})
				closed atomic.Bool
				feedWg sync.WaitGroup // Track producer goroutine for safe stop()
			)

			// Producer goroutine
			feedWg.Go(func() {
				defer close(ch)
				for {
					select {
					case <-done:
						return
					default:
					}
					v, ok := next()
					if !ok {
						return
					}
					select {
					case <-done:
						return
					case ch <- v:
					}
				}
			})

			// Ensure producer goroutine completes before calling stop()
			defer func() {
				feedWg.Wait()
				stop()
			}()

			// Consumer
			for v := range ch {
				if !yield(v) {
					if closed.CompareAndSwap(false, true) {
						close(done)
					}
					// Drain channel to prevent goroutine leak
					for range ch {
					}
					return
				}
			}
		},
	}
}

// --- Parallel ForEach ---

// ParallelForEach executes an action on each element in parallel.
// This is a terminal operation that blocks until all elements are processed.
func ParallelForEach[T any](s Stream[T], action func(T), opts ...ParallelOption) {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	inputCh := make(chan T, cfg.BufferSize)

	var wg sync.WaitGroup
	for range cfg.Concurrency {
		wg.Go(func() {
			for v := range inputCh {
				action(v)
			}
		})
	}

	for v := range s.seq {
		inputCh <- v
	}
	close(inputCh)

	wg.Wait()
}

// --- Parallel Reduce ---

// ParallelReduce reduces elements in parallel using an associative operation.
// The operation must be associative for correct results.
func ParallelReduce[T any](s Stream[T], identity T, op func(T, T) T, opts ...ParallelOption) T {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	// Collect all elements (we need them for parallel reduction)
	elements := s.Collect()
	if len(elements) == 0 {
		return identity
	}

	// If fewer elements than workers, just reduce sequentially
	if len(elements) <= cfg.Concurrency {
		result := identity
		for _, v := range elements {
			result = op(result, v)
		}
		return result
	}

	// Split work among workers
	chunkSize := (len(elements) + cfg.Concurrency - 1) / cfg.Concurrency
	results := make(chan T, cfg.Concurrency)

	var wg sync.WaitGroup
	for i := range cfg.Concurrency {
		start := i * chunkSize
		if start >= len(elements) {
			break
		}
		end := min(start+chunkSize, len(elements))
		chunk := elements[start:end]

		wg.Go(func() {
			localResult := identity
			for _, v := range chunk {
				localResult = op(localResult, v)
			}
			results <- localResult
		})
	}

	go func() { wg.Wait(); close(results) }()

	finalResult := identity
	for partialResult := range results {
		finalResult = op(finalResult, partialResult)
	}

	return finalResult
}

// --- Parallel Collect ---

// ParallelCollect collects elements in parallel into a slice.
// Note: Order is not guaranteed unless the source stream has been ordered.
func ParallelCollect[T any](s Stream[T], opts ...ParallelOption) []T {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	var (
		mu      sync.Mutex
		results []T
	)

	ParallelForEach(s, func(v T) {
		mu.Lock()
		results = append(results, v)
		mu.Unlock()
	}, opts...)

	return results
}

// ParallelForEachCtx executes an action on each element in parallel with context support.
func ParallelForEachCtx[T any](ctx context.Context, s Stream[T], action func(context.Context, T), opts ...ParallelOption) error {
	cfg := DefaultParallelConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	var (
		inputCh = make(chan T, cfg.BufferSize)
		wg      sync.WaitGroup
		closed  atomic.Bool
		done    = make(chan struct{})
	)

	// Monitor context cancellation
	go func() {
		select {
		case <-ctx.Done():
			if closed.CompareAndSwap(false, true) {
				close(done)
			}
		case <-done:
		}
	}()

	for range cfg.Concurrency {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				case v, ok := <-inputCh:
					if !ok {
						return
					}
					action(ctx, v)
				}
			}
		})
	}

	func() {
		defer close(inputCh)
		for v := range s.seq {
			select {
			case <-done:
				return
			case inputCh <- v:
			}
		}
	}()

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}
