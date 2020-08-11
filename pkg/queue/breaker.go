/*
Copyright 2018 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.uber.org/atomic"
)

var (
	// ErrUpdateCapacity indicates that the capacity could not be updated as wished.
	ErrUpdateCapacity = errors.New("failed to add all capacity to the breaker")
	// ErrRelease indicates that release was called more often than acquire.
	ErrRelease = errors.New("semaphore release error: returned tokens must be <= acquired tokens")
	// ErrRequestQueueFull indicates the breaker queue depth was exceeded.
	ErrRequestQueueFull = errors.New("pending request queue full")
)

// MaxBreakerCapacity is the largest valid value for the MaxConcurrency value of BreakerParams.
// This is limited by the maximum size of a chan struct{} in the current implementation.
const MaxBreakerCapacity = math.MaxInt32

// BreakerParams defines the parameters of the breaker.
type BreakerParams struct {
	QueueDepth      int
	MaxConcurrency  int
	InitialCapacity int
}

// Breaker is a component that enforces a concurrency limit on the
// execution of a function. It also maintains a queue of function
// executions in excess of the concurrency limit. Function call attempts
// beyond the limit of the queue are failed immediately.
type Breaker struct {
	inFlight   atomic.Int64
	totalSlots int64
	sem        *semaphore

	// release is the callback function returned to callers by Reserve to
	// allow the reservation made by Reserve to be released.
	release func()
}

// NewBreaker creates a Breaker with the desired queue depth,
// concurrency limit and initial capacity.
func NewBreaker(params BreakerParams) *Breaker {
	if params.QueueDepth <= 0 {
		panic(fmt.Sprintf("Queue depth must be greater than 0. Got %v.", params.QueueDepth))
	}
	if params.MaxConcurrency < 0 {
		panic(fmt.Sprintf("Max concurrency must be 0 or greater. Got %v.", params.MaxConcurrency))
	}
	if params.InitialCapacity < 0 || params.InitialCapacity > params.MaxConcurrency {
		panic(fmt.Sprintf("Initial capacity must be between 0 and max concurrency. Got %v.", params.InitialCapacity))
	}

	b := &Breaker{
		totalSlots: int64(params.QueueDepth + params.MaxConcurrency),
		sem:        newSemaphore(params.MaxConcurrency, params.InitialCapacity),
	}

	// Allocating the closure returned by Reserve here avoids an allocation in Reserve.
	b.release = func() {
		b.sem.release()
		b.releasePending()
	}

	return b
}

// tryAcquirePending tries to acquire a slot on the pending "queue".
func (b *Breaker) tryAcquirePending() bool {
	// This is an atomic version of:
	//
	// if inFlight == totalSlots {
	//   return false
	// } else {
	//   inFlight++
	//   return true
	// }
	//
	// We can't just use an atomic increment as we need to check if we're
	// "allowed" to increment first. Since a Load and a CompareAndSwap are
	// not done atomically, we need to retry until the CompareAndSwap succeeds
	// (it fails if we're raced to it) or if we don't fulfill the condition
	// anymore.
	for {
		cur := b.inFlight.Load()
		if cur == b.totalSlots {
			return false
		}
		if b.inFlight.CAS(cur, cur+1) {
			return true
		}
	}
}

// releasePending releases a slot on the pending "queue".
func (b *Breaker) releasePending() {
	b.inFlight.Dec()
}

// Reserve reserves an execution slot in the breaker, to permit
// richer semantics in the caller.
// The caller on success must execute the callback when done with work.
func (b *Breaker) Reserve(ctx context.Context) (func(), bool) {
	if !b.tryAcquirePending() {
		return nil, false
	}

	if !b.sem.tryAcquire() {
		b.releasePending()
		return nil, false
	}

	return b.release, true
}

// Maybe conditionally executes thunk based on the Breaker concurrency
// and queue parameters. If the concurrency limit and queue capacity are
// already consumed, Maybe returns immediately without calling thunk. If
// the thunk was executed, Maybe returns true, else false.
func (b *Breaker) Maybe(ctx context.Context, thunk func()) error {
	if !b.tryAcquirePending() {
		return ErrRequestQueueFull
	}

	defer b.releasePending()

	// Wait for capacity in the active queue.
	if err := b.sem.acquire(ctx); err != nil {
		return err
	}
	// Defer releasing capacity in the active.
	// It's safe to ignore the error returned by release since we
	// make sure the semaphore is only manipulated here and acquire
	// + release calls are equally paired.
	defer b.sem.release()

	// Do the thing.
	thunk()
	// Report success
	return nil
}

// InFlight returns the number of requests currently in flight in this breaker.
func (b *Breaker) InFlight() int {
	return int(b.inFlight.Load())
}

// UpdateConcurrency updates the maximum number of in-flight requests.
func (b *Breaker) UpdateConcurrency(size int) error {
	return b.sem.updateCapacity(size, nil)
}

// Capacity returns the number of allowed in-flight requests on this breaker.
func (b *Breaker) Capacity() int {
	return b.sem.Capacity()
}

// newSemaphore creates a semaphore with the desired maximal and initial capacity.
// Maximal capacity is the size of the buffered channel, it defines maximum number of tokens
// in the rotation. Attempting to add more capacity then the max will result in error.
// Initial capacity is the initial number of free tokens.
func newSemaphore(maxCapacity, initialCapacity int) *semaphore {
	queue := make(chan struct{}, maxCapacity)
	sem := &semaphore{queue: queue}
	if initialCapacity > 0 {
		sem.updateCapacity(initialCapacity, nil)
	}
	return sem
}

// semaphore is an implementation of a semaphore based on Go channels.
// The presence of elements in the `queue` buffered channel correspond to available tokens.
// Hence the max number of tokens to hand out equals to the size of the channel.
// `capacity` defines the current number of tokens in the rotation.
type semaphore struct {
	queue    chan struct{}
	capacity atomic.Int32
}

// tryAcquire receives the token from the semaphore if there's one
// otherwise an error is returned.
func (s *semaphore) tryAcquire() bool {
	select {
	case <-s.queue:
		return true
	default:
		return false
	}
}

// acquire receives the token from the semaphore, potentially blocking.
func (s *semaphore) acquire(ctx context.Context) error {
	select {
	case <-s.queue:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release potentially puts the token back to the queue.
// If the semaphore capacity was reduced in between and is not yet reflected,
// we remove the tokens from the rotation instead of returning them back.
func (s *semaphore) release() error {
	// We want to make sure releasing a token is always non-blocking.
	select {
	case s.queue <- struct{}{}:
		return nil
	default:
		// This only happens if release is called more often than acquire.
		return ErrRelease
	}
}

// updateCapacity updates the capacity of the semaphore to the desired
// size.
func (s *semaphore) updateCapacity(size int, abort <-chan struct{}) error {
	if size < 0 || size > cap(s.queue) {
		return ErrUpdateCapacity
	}
	size32 := int32(size)

	lastSize := s.capacity.Swap(size32)
	if lastSize == size32 {
		return nil
	}

	var dec, inc chan struct{}
	if lastSize < size32 {
		inc = s.queue
	} else {
		dec = s.queue
	}

	for lastSize != size32 {
		select {
		case inc <- struct{}{}:
			lastSize++

		case <-dec:
			lastSize--

		case <-abort:
			s.capacity.Add(lastSize - size32)
			return ErrUpdateCapacity

		}
	}

	return nil
}

// Capacity is the effective capacity after taking reducers into account.
func (s *semaphore) Capacity() int {
	return int(s.capacity.Load())
}
