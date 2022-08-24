package scheduler

import (
	"time"
)

type detailScheduler struct {
	key      string
	idx      int
	timer    *time.Timer
	dateTime time.Time
}

type detailSchedulers []*detailScheduler

func (dss *detailSchedulers) Remove(index int) {
	dsSlice := *dss
	dsSlice = append(dsSlice[:index], dsSlice[index+1:]...)
}

func (dss *detailSchedulers) Add(ds *detailScheduler) {
	*dss = append(*dss, ds)
}

func (dss *detailSchedulers) Replace(index int, ds *detailScheduler) {
	dssSlice := *dss
	dssSlice[index] = ds
}

func (dss *detailSchedulers) Total() int {
	return len(*dss)
}
