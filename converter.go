package scheduler

type ListConverter interface {
	Convert(data []*ResponseScheduler) ([]byte, error)
}
