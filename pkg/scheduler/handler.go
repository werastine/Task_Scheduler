package scheduler

import (
	"time"
)

type Sender interface {
	SendNow(
		taskType string,
		payload []byte,
	) (taskID string, err error)

	SendNowWithRepeat(
		repeatTimes int,
		repeatEvery time.Duration,
		taskType string,
		payload []byte,
	) (taskID string, err error)

	SendAfterWithRepeat(
		sendAfter time.Time,
		repeatTimes int,
		repeatEvery time.Duration,
		taskType string,
		payload []byte,
	) (taskID string, err error)

	SendAfter(
		sendAfter time.Time,
		taskType string,
		payload []byte,
	) (taskID string, err error)
}
