package scheduler

import (
	"errors"
)

var (
	ErrKeyIsExists         = errors.New("the key is exists")
	ErrDateTimeLessThanNow = errors.New("the parameter date time cannot less than now")
	ErrKeyIsNotExists      = errors.New("the key is not exists")
)

type ListType int

const (
	ListTypeDefault ListType = iota
	ListTypeJSON
)
