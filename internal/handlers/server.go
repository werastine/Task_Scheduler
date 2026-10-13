package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	schedulerv1 "github.com/werastine/Task_Scheduler/pkg/api/scheduler/v1"
	"github.com/werastine/Task_Scheduler/pkg/scheduler"
)

type Server struct {
	schedulerv1.UnimplementedSchedulerServer

	sender scheduler.Sender
}

func NewServer(engine *scheduler.Engine) *Server {
	return &Server{
		sender: engine,
	}
}

func (s *Server) PushRequest(ctx context.Context, req *schedulerv1.Request) (*schedulerv1.Response, error) {
	taskID := uuid.NewString()

	switch sch := req.Schedule.(type) {
	case *schedulerv1.Request_Now:
		err := s.sender.SendNow(
			ctx,
			req.GetType(),
			req.GetPayload(),
			taskID,
		)

		if err != nil {
			slog.ErrorContext(ctx, "failed to use SendNow function", "err", err)
			return nil, fmt.Errorf("somethig went wrong")
		}

	case *schedulerv1.Request_NowRepeat:
		repeatTimes := int(sch.NowRepeat.RepeatTimes)
		repeatEvery := time.Duration(sch.NowRepeat.RepeatEverySeconds) * time.Second

		err := s.sender.SendNowWithRepeat(
			ctx,
			repeatTimes,
			repeatEvery,
			req.Type,
			req.Payload,
			taskID,
		)

		if err != nil {
			slog.ErrorContext(ctx, "failed to use SendNowWithRepeat function", "err", err)
			return nil, fmt.Errorf("somethig went wrong")
		}

	case *schedulerv1.Request_After:
		delay := time.Duration(sch.After.ExecuteAfterSeconds) * time.Second
		sendAfter := time.Now().Add(delay)

		err := s.sender.SendAfter(
			ctx,
			sendAfter,
			req.Type,
			req.Payload,
			taskID,
		)

		if err != nil {
			slog.ErrorContext(ctx, "failed to use SendAfter function", "err", err)
			return nil, fmt.Errorf("somethig went wrong")
		}

	case *schedulerv1.Request_AfterRepeat:
		delay := time.Duration(sch.AfterRepeat.ExecuteAfterSeconds) * time.Second
		sendAfter := time.Now().Add(delay)
		repeatTimes := int(sch.AfterRepeat.RepeatTimes)
		repeatEvery := time.Duration(sch.AfterRepeat.RepeatEverySeconds) * time.Second

		err := s.sender.SendAfterWithRepeat(
			ctx,
			sendAfter,
			repeatTimes,
			repeatEvery,
			req.Type,
			req.Payload,
			taskID,
		)
		if err != nil {
			slog.ErrorContext(ctx, "failed to use SendAfterWithRepeat function", "err", err)
			return nil, fmt.Errorf("somethig went wrong")
		}

	default:
		return nil, fmt.Errorf("didn't recieve any fields")
	}

	if ctx.Err() != nil {
		slog.WarnContext(ctx, "Context is done")
		return nil, fmt.Errorf("user leaved")
	}

	return &schedulerv1.Response{
		Status: "PENDING",
		TaskId: fmt.Sprintf("Your ID: %s", taskID),
	}, nil
}
