package graph

import (
	"context"
	"errors"
	"sync"
)

var ErrPanic = errors.New("panic recovered")

type TaskFunc func(context.Context) (bool, error)

type item struct {
	task TaskFunc
	out  chan Result
}

type Result struct {
	Outcome bool
	Err     error
}

func NewScheduler(ctx context.Context, concurrency int) *Scheduler {
	ctx, cancel := context.WithCancel(ctx)

	sch := Scheduler{
		ctx:    ctx,
		cancel: cancel,
		chIn:   make(chan item),
	}

	for i := 0; i < concurrency; i++ {
		sch.wg.Add(1)
		go func() {
			defer sch.wg.Done()

		loop:
			for {
				select {
				case item, more := <-sch.chIn:
					if !more {
						break loop
					}
					value, err := item.task(sch.ctx)
					if err != nil {
						select {
						case item.out <- Result{
							Err: err,
						}:
						case <-sch.ctx.Done():
						}
						continue loop
					}

					if value {
						select {
						case item.out <- Result{
							Outcome: true,
						}:
						case <-sch.ctx.Done():
						}
					}
				case <-sch.ctx.Done():
					break loop
				}
			}
		}()
	}
	return &sch
}

type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	chIn   chan item
	wg     sync.WaitGroup
}

func (s *Scheduler) Schedule(ctx context.Context, fn TaskFunc) <-chan Result {
	ch := make(chan Result, 1)
	if s.ctx.Err() != nil || ctx.Err() != nil {
		ch <- Result{
			Err: errors.Join(s.ctx.Err(), ctx.Err()),
		}
		return ch
	}
	select {
	case s.chIn <- item{
		task: fn,
		out:  ch,
	}:
	case <-s.ctx.Done():
		ch <- Result{
			Err: s.ctx.Err(),
		}
	case <-ctx.Done():
		ch <- Result{
			Err: ctx.Err(),
		}
	}
	return ch
}

func (s *Scheduler) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}
