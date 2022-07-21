package scheduler

import (
	"context"
	"sync"
	"time"
)

type FnScheduler func(ctx context.Context)

type Scheduler struct {
	schedulers map[string]*time.Timer
	mutex      sync.RWMutex
}

func NewScheduler() *Scheduler {
	return &Scheduler{schedulers: make(map[string]*time.Timer)}
}

func (s *Scheduler) read(key string) (bool, *time.Timer) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	tm, isExists := s.schedulers[key]
	return isExists, tm
}

func (s *Scheduler) add(key string, duration time.Duration, fn FnScheduler) (err error) {
	isExists, _ := s.read(key)
	if isExists {
		err = ErrKeyIsExists
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.schedulers[key] = time.AfterFunc(duration, func() {
		fn(context.Background())
		s.deleteKey(key)
	})

	return
}

func (s *Scheduler) deleteKey(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.schedulers, key)
}

func (s *Scheduler) reschedule(key string, duration time.Duration) (err error) {
	isExists, tm := s.read(key)

	if !isExists {
		err = ErrKeyIsExists
		return
	}

	tm.Reset(duration)
	return
}

func (s *Scheduler) cancel(key string) (err error) {
	isExists, tm := s.read(key)
	if !isExists {
		err = ErrKeyIsNotExists
		return
	}

	tm.Stop()
	s.deleteKey(key)
	return
}

func (s *Scheduler) replace(key string, duration time.Duration, fn FnScheduler) (err error) {
	err = s.cancel(key)
	if err != nil {
		return
	}

	err = s.add(key, duration, fn)
	return
}

func (s *Scheduler) Add(key string, duration time.Duration, fn FnScheduler) (err error) {
	return s.add(key, duration, fn)
}

func (s *Scheduler) AddDate(key string, dateTime time.Time, fn FnScheduler) (err error) {
	duration, err := subtractDateTime(dateTime)
	if err != nil {
		return
	}

	return s.add(key, duration, fn)
}

func (s *Scheduler) Cancel(key string) (err error) {
	err = s.cancel(key)
	return
}

func (s *Scheduler) RescheduleTime(key string, duration time.Duration) (err error) {
	return s.reschedule(key, duration)
}

func (s *Scheduler) RescheduleDateTime(key string, dateTime time.Time) (err error) {
	duration, err := subtractDateTime(dateTime)
	if err != nil {
		return
	}

	return s.reschedule(key, duration)
}

func (s *Scheduler) Replace(key string, duration time.Duration, fn FnScheduler) (err error) {
	err = s.replace(key, duration, fn)
	return
}

func (s *Scheduler) ReplaceDateTime(key string, dateTime time.Time, fn FnScheduler) (err error) {
	duration, err := subtractDateTime(dateTime)
	if err != nil {
		return
	}

	err = s.replace(key, duration, fn)
	return
}
