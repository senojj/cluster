package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrPanic = errors.New("panic recovered")

type TaskFunc func(context.Context) (any, error)

type item struct {
	task TaskFunc
	out  chan Result
}

type Result struct {
	Outcome any
	Err     error
}

func NewScheduler(concurrency int) *Scheduler {
	return &Scheduler{
		limit: make(chan struct{}, concurrency),
	}
}

type Scheduler struct {
	limit chan struct{}
	wg    sync.WaitGroup
}

func (s *Scheduler) Schedule(ctx context.Context, fn TaskFunc) <-chan Result {
	ch := make(chan Result, 1)
	if ctx.Err() != nil {
		ch <- Result{
			Err: ctx.Err(),
		}
		return ch
	}

	select {
	case <-s.limit:
	case <-ctx.Done():
		ch <- Result{
			Err: ctx.Err(),
		}
		return ch
	}

	s.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- Result{
					Err: fmt.Errorf("%w: %s", ErrPanic, r),
				}
			}
			s.wg.Done()
			s.limit <- struct{}{}
		}()
		r, err := fn(ctx)
		ch <- Result{
			Outcome: r,
			Err:     err,
		}
	}()
	return ch
}

func (s *Scheduler) Close() error {
	s.wg.Wait()
	return nil
}
