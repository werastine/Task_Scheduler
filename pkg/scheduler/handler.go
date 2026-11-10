package scheduler

import (
	"context"
	"time"
)

type Sender interface {
	SendNow(
		ctx context.Context,
		taskType string,
		payload []byte,
		taskID string,
		failTimes int,
		failTimeout time.Duration,
	) error

	SendNowWithRepeat(
		ctx context.Context,
		repeatTimes int,
		repeatEvery time.Duration,
		taskType string,
		payload []byte,
		taskID string,
		failTimes int,
		failTimeout time.Duration,
	) error

	SendAfter(
		ctx context.Context,
		sendAfter time.Time,
		taskType string,
		payload []byte,
		taskID string,
		failTimes int,
		failTimeout time.Duration,
	) error

	SendAfterWithRepeat(
		ctx context.Context,
		sendAfter time.Time,
		repeatTimes int,
		repeatEvery time.Duration,
		taskType string,
		payload []byte,
		taskID string,
		failTimes int,
		failTimeout time.Duration,
	) error
}
