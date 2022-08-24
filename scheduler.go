package scheduler

import (
	"context"
	"io"
	"sync"
	"time"
)

type FnScheduler func(ctx context.Context)

const (
	defaultUTCTimeZone = "UTC"
)

type Scheduler struct {
	schedulers      map[string]*detailScheduler
	schedulersSlice detailSchedulers
	mutex           sync.RWMutex
	config          Config
	locationTZ      *time.Location
}

type paramScheduler struct {
	duration time.Duration
	dateTime time.Time
}

type Config struct {
	TimeZone string
}

func NewScheduler(configs ...Config) *Scheduler {
	config := Config{}
	if len(configs) > 0 {
		config = configs[0]
	}

	scheduler := &Scheduler{
		schedulers: make(map[string]*detailScheduler),
		mutex:      sync.RWMutex{},
		config:     config,
	}

	scheduler.loadTZ()
	return scheduler
}

func (s *Scheduler) loadTZ() {
	var err error
	s.locationTZ, err = time.LoadLocation(s.config.TimeZone)
	if err != nil {
		s.locationTZ, _ = time.LoadLocation(defaultUTCTimeZone)
	}
}

func (s *Scheduler) read(key string) (bool, *detailScheduler) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	ds, isExists := s.schedulers[key]
	return isExists, ds
}

func (s *Scheduler) add(key string, param *paramScheduler, fn FnScheduler) (err error) {
	isExists, _ := s.read(key)
	if isExists {
		err = ErrKeyIsExists
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	ds := &detailScheduler{
		key: key,
		timer: time.AfterFunc(param.duration, func() {
			fn(context.Background())
			_, ds := s.read(key)
			s.deleteScheduler(key)
			s.deleteSchedulerSlice(ds)
		}),
		dateTime: param.dateTime,
		idx:      s.schedulersSlice.Total(),
	}

	s.schedulersSlice.Add(ds)
	s.schedulers[key] = ds

	return
}

func (s *Scheduler) deleteScheduler(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.schedulers, key)
}

func (s *Scheduler) deleteSchedulerSlice(ds *detailScheduler) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.schedulersSlice.Remove(ds.idx)
}

func (s *Scheduler) reschedule(key string, param *paramScheduler) (err error) {
	isExists, ds := s.read(key)

	if !isExists {
		err = ErrKeyIsNotExists
		return
	}

	ds.timer.Reset(param.duration)
	return
}

func (s *Scheduler) cancel(key string) (err error) {
	isExists, ds := s.read(key)
	if !isExists {
		err = ErrKeyIsNotExists
		return
	}

	ds.timer.Stop()
	s.deleteScheduler(key)
	s.deleteSchedulerSlice(ds)
	return
}

func (s *Scheduler) replace(key string, param *paramScheduler, fn FnScheduler) (err error) {
	err = s.cancel(key)
	if err != nil {
		return
	}

	err = s.add(key, param, fn)
	return
}

func (s *Scheduler) subtractDateTime(dateTime time.Time) (duration time.Duration, err error) {
	var (
		now        = time.Now().In(s.locationTZ)
		dateTimeTZ = dateTime.In(s.locationTZ)
	)

	if dateTimeTZ.Before(now) {
		err = ErrDateTimeLessThanNow
		return
	}

	duration = dateTimeTZ.Sub(now)
	return
}

func (s *Scheduler) fromDurationToDateTime(duration time.Duration) time.Time {
	return time.Now().In(s.locationTZ).Add(duration)
}

func (s *Scheduler) toResponseScheduler() (res []*ResponseScheduler) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for i := 0; i < len(s.schedulersSlice); i++ {
		res = append(res, &ResponseScheduler{
			Key:  s.schedulersSlice[i].key,
			Time: s.schedulersSlice[i].dateTime,
		})
	}

	return
}

func (s *Scheduler) Add(key string, duration time.Duration, fn FnScheduler) (err error) {
	return s.add(key, &paramScheduler{
		duration: duration,
		dateTime: s.fromDurationToDateTime(duration),
	}, fn)
}

func (s *Scheduler) AddDate(key string, dateTime time.Time, fn FnScheduler) (err error) {
	duration, err := s.subtractDateTime(dateTime)
	if err != nil {
		return
	}

	return s.add(key, &paramScheduler{
		duration: duration,
		dateTime: dateTime.In(s.locationTZ),
	}, fn)
}

func (s *Scheduler) Cancel(key string) (err error) {
	err = s.cancel(key)
	return
}

func (s *Scheduler) Reschedule(key string, duration time.Duration) (err error) {
	return s.reschedule(key, &paramScheduler{
		duration: duration,
		dateTime: s.fromDurationToDateTime(duration),
	})
}

func (s *Scheduler) RescheduleDateTime(key string, dateTime time.Time) (err error) {
	duration, err := s.subtractDateTime(dateTime)
	if err != nil {
		return
	}

	return s.reschedule(key, &paramScheduler{
		duration: duration,
		dateTime: dateTime.In(s.locationTZ),
	})
}

func (s *Scheduler) Replace(key string, duration time.Duration, fn FnScheduler) (err error) {
	err = s.replace(key, &paramScheduler{
		duration: duration,
		dateTime: s.fromDurationToDateTime(duration),
	}, fn)
	return
}

func (s *Scheduler) ReplaceDateTime(key string, dateTime time.Time, fn FnScheduler) (err error) {
	duration, err := s.subtractDateTime(dateTime)
	if err != nil {
		return
	}

	err = s.replace(key, &paramScheduler{
		duration: duration,
		dateTime: dateTime.In(s.locationTZ),
	}, fn)
	return
}

func (s *Scheduler) List(w io.Writer, lcs ...ListConverter) (n int, err error) {
	responseScheduler := s.toResponseScheduler()
	lc := NewDefaultResponse()
	if len(lcs) > 0 {
		lc = lcs[0]
	}

	bytes, err := lc.Convert(responseScheduler)
	return w.Write(bytes)
}
