package state

type TailerState int64

const (
	Running TailerState = iota + 1
	Paused
	Closed
)
