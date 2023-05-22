package state

type TailerState int64

const (
	Running TailerState = iota
	Paused
	Closed
)
