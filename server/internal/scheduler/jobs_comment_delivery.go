package scheduler

import (
	"context"
	"time"
)

type CommentDeliverySweeper interface {
	SweepCommentDeliveries(context.Context) error
}

func CommentDeliveryJob(sweeper CommentDeliverySweeper) JobSpec {
	return JobSpec{
		Name: "comment_delivery", Cadence: 30 * time.Second,
		CatchUpMode: CatchUpLatestOnly, CatchUpWindow: time.Hour,
		RunTimeout: 45 * time.Second, StaleTimeout: time.Minute,
		HeartbeatInterval: 10 * time.Second, AllowStaleReentry: true,
		MaxAttempts: 1, Scopes: StaticScopes(ScopeGlobal),
		Handler: func(ctx context.Context, _ HandlerInput) (HandlerResult, error) {
			return HandlerResult{}, sweeper.SweepCommentDeliveries(ctx)
		},
	}
}
