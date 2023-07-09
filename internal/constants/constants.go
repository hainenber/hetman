package constants

import "time"

// Unnoticeable sleep to prevent high CPU usage
// Ref: https://github.com/golang/go/issues/27707#issuecomment-698487427
const TIME_WAIT_FOR_NEXT_ITERATION = 1 * time.Microsecond // 1us
