package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	schedulerv1 "github.com/werastine/Task_Scheduler/pkg/api/scheduler/v1"
	"github.com/werastine/Task_Scheduler/pkg/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	var err error

	switch sch := req.Schedule.(type) {
	case *schedulerv1.Request_Now:
		err = s.sender.SendNow(
			ctx,
			req.GetType(),
			req.GetPayload(),
			taskID,
		)

	case *schedulerv1.Request_NowRepeat:
		repeatTimes := int(sch.NowRepeat.RepeatTimes)
		repeatEvery := time.Duration(sch.NowRepeat.RepeatEverySeconds) * time.Second

		err = s.sender.SendNowWithRepeat(
			ctx,
			repeatTimes,
			repeatEvery,
			req.Type,
			req.Payload,
			taskID,
		)

	case *schedulerv1.Request_After:
		delay := time.Duration(sch.After.ExecuteAfterSeconds) * time.Second
		sendAfter := time.Now().Add(delay)

		err = s.sender.SendAfter(
			ctx,
			sendAfter,
			req.Type,
			req.Payload,
			taskID,
		)

	case *schedulerv1.Request_AfterRepeat:
		delay := time.Duration(sch.AfterRepeat.ExecuteAfterSeconds) * time.Second
		sendAfter := time.Now().Add(delay)
		repeatTimes := int(sch.AfterRepeat.RepeatTimes)
		repeatEvery := time.Duration(sch.AfterRepeat.RepeatEverySeconds) * time.Second

		err = s.sender.SendAfterWithRepeat(
			ctx,
			sendAfter,
			repeatTimes,
			repeatEvery,
			req.Type,
			req.Payload,
			taskID,
		)

	default:
		return nil, fmt.Errorf("invalid schedule type provided")
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.WarnContext(ctx, "task creation canceled", "taskID", taskID, "err", err)
			return nil, status.Error(codes.Canceled, "request canceled")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			slog.WarnContext(ctx, "task creation timed out", "taskID", taskID, "err", err)
			return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}
		slog.ErrorContext(ctx, "failed to create task", "taskID", taskID, "err", err)
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &schedulerv1.Response{
		Status: "PENDING",
		TaskId: fmt.Sprintf("Your ID: %s", taskID),
	}, nil
}
