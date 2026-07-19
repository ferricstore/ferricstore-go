package ferricstore

import (
	"context"
	"fmt"
)

type ScheduleOptions struct {
	Target         map[string]any
	Kind           string
	AtMS           *int64
	DelayMS        *int64
	StartAtMS      *int64
	EveryMS        *int64
	Cron           string
	Timezone       string
	OverlapPolicy  string
	OverlapRetryMS *int64
	MaxFires       *int64
	EndAtMS        *int64
	Overwrite      *bool
	NowMS          *int64
	DeadlineMS     *int64
}

type ScheduleListOptions struct {
	Kind       string
	State      string
	Timezone   string
	TargetType string
	FromMS     *int64
	ToMS       *int64
	Count      *int
	Rev        *bool
	DeadlineMS *int64
}

// ScheduleStatusOptions controls a schedule state mutation. DeadlineMS is a
// native request deadline; NowMS is the logical Flow mutation timestamp.
type ScheduleStatusOptions struct {
	NowMS      *int64
	DeadlineMS *int64
}

type ScheduleResult struct {
	ID                   string
	FlowID               string
	Kind                 string
	Status               string
	Target               map[string]any
	Timezone             string
	Cron                 string
	OverlapPolicy        string
	NextFireAtMS         int64
	LastFireAtMS         int64
	Fires                int64
	Attempts             int64
	MaxFires             int64
	EndAtMS              int64
	LastOverlapAtMS      int64
	LastOverlapTargetID  string
	LastOverlapReason    string
	LastSkippedAtMS      int64
	SkippedCount         int64
	OverlapQueuedDueAtMS int64
	EndReason            string
	// Scheduler summary fields are populated by the compatibility
	// ScheduleFireDue method. New callers should use
	// ScheduleFireDueWithOptions and ScheduleFireDueResult.
	Claimed        int64
	Fired          int64
	Skipped        int64
	Errors         []ScheduleFireDueError
	LastTargetID   string
	LastSkipReason string
	Raw            map[string]any
}

type ScheduleFireDueOptions struct {
	NowMS      *int64
	Worker     string
	LeaseMS    *int64
	BlockMS    *int64
	Limit      *int
	DeadlineMS *int64
}

type ScheduleFireDueError struct {
	ID     string
	Reason string
}

type ScheduleFireDueResult struct {
	Claimed        int64
	Fired          int64
	Skipped        int64
	Errors         []ScheduleFireDueError
	LastTargetID   string
	LastSkipReason string
	Raw            map[string]any
}

type ScheduleFireOptions struct {
	NowMS      *int64
	FireAtMS   *int64
	DeadlineMS *int64
}

type ScheduleFireResult struct {
	Fired    int64
	Skipped  int64
	TargetID string
	Reason   string
	Schedule ScheduleResult
	Raw      map[string]any
}

func (c *Client) ScheduleCreate(ctx context.Context, id string, opt ScheduleOptions) (ScheduleResult, error) {
	if err := validateScheduleCreate(id, opt); err != nil {
		return ScheduleResult{}, err
	}
	target, err := c.encodeScheduleTarget(opt.Target)
	if err != nil {
		return ScheduleResult{}, err
	}
	args := []any{"FLOW.SCHEDULE.CREATE", id}
	appendOpt(&args, "KIND", canonicalAdminEnum(opt.Kind))
	appendInt64Ptr(&args, "AT_MS", opt.AtMS)
	appendInt64Ptr(&args, "DELAY_MS", opt.DelayMS)
	appendInt64Ptr(&args, "START_AT_MS", opt.StartAtMS)
	appendInt64Ptr(&args, "EVERY_MS", opt.EveryMS)
	appendOpt(&args, "CRON", opt.Cron)
	appendOpt(&args, "TIMEZONE", opt.Timezone)
	if target != nil {
		appendOpt(&args, "TARGET", target)
	}
	appendOpt(&args, "OVERLAP_POLICY", canonicalAdminEnum(opt.OverlapPolicy))
	appendInt64Ptr(&args, "OVERLAP_RETRY_MS", opt.OverlapRetryMS)
	appendInt64Ptr(&args, "MAX_FIRES", opt.MaxFires)
	appendInt64Ptr(&args, "END_AT_MS", opt.EndAtMS)
	appendBoolPtr(&args, "OVERWRITE", opt.Overwrite)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	return scheduleResultWithCodec(value, err, c.codec)
}

func (c *Client) ScheduleGet(ctx context.Context, id string, deadlineMS *int64) (*ScheduleResult, error) {
	if err := validateScheduleGet(id, deadlineMS); err != nil {
		return nil, err
	}
	args := []any{"FLOW.SCHEDULE.GET", id}
	appendInt64Ptr(&args, "DEADLINE_MS", deadlineMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := scheduleResultWithCodec(value, nil, c.codec)
	return &result, err
}

func (c *Client) ScheduleFire(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	result, err := c.ScheduleFireWithOptions(ctx, id, ScheduleFireOptions{NowMS: nowMS})
	if err != nil {
		return ScheduleResult{}, err
	}
	schedule := result.Schedule
	schedule.Fired = result.Fired
	schedule.Skipped = result.Skipped
	schedule.LastTargetID = result.TargetID
	schedule.LastSkipReason = result.Reason
	// Preserve the released method's top-level Raw response while exposing the
	// correctly decoded nested schedule through typed fields.
	schedule.Raw = result.Raw
	return schedule, nil
}

// ScheduleFireWithOptions fires one schedule and decodes both the manual-fire
// outcome and the updated nested schedule.
func (c *Client) ScheduleFireWithOptions(ctx context.Context, id string, opt ScheduleFireOptions) (ScheduleFireResult, error) {
	if err := validateScheduleFireOptions(id, opt); err != nil {
		return ScheduleFireResult{}, err
	}
	args := []any{"FLOW.SCHEDULE.FIRE", id}
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "FIRE_AT_MS", opt.FireAtMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	return scheduleFireResultWithCodec(value, err, c.codec)
}

func (c *Client) SchedulePause(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.SchedulePauseWithOptions(ctx, id, ScheduleStatusOptions{NowMS: nowMS})
}

func (c *Client) ScheduleResume(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.ScheduleResumeWithOptions(ctx, id, ScheduleStatusOptions{NowMS: nowMS})
}

func (c *Client) ScheduleDelete(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.ScheduleDeleteWithOptions(ctx, id, ScheduleStatusOptions{NowMS: nowMS})
}

func (c *Client) SchedulePauseWithOptions(ctx context.Context, id string, opt ScheduleStatusOptions) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.PAUSE", id, opt)
}

func (c *Client) ScheduleResumeWithOptions(ctx context.Context, id string, opt ScheduleStatusOptions) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.RESUME", id, opt)
}

func (c *Client) ScheduleDeleteWithOptions(ctx context.Context, id string, opt ScheduleStatusOptions) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.DELETE", id, opt)
}

func (c *Client) scheduleStatus(ctx context.Context, command, id string, opt ScheduleStatusOptions) (ScheduleResult, error) {
	if err := validateScheduleStatus(id, opt); err != nil {
		return ScheduleResult{}, err
	}
	args := []any{command, id}
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return ScheduleResult{}, err
	}
	if isOK(value) {
		return ScheduleResult{ID: id, Status: "deleted", Raw: map[string]any{"id": id, "status": "deleted"}}, nil
	}
	return scheduleResultWithCodec(value, nil, c.codec)
}

func (c *Client) ScheduleFireDue(ctx context.Context, nowMS *int64, worker string, blockMS *int64, limit *int) (ScheduleResult, error) {
	result, err := c.ScheduleFireDueWithOptions(ctx, ScheduleFireDueOptions{
		NowMS: nowMS, Worker: worker, BlockMS: blockMS, Limit: limit,
	})
	if err != nil {
		return ScheduleResult{}, err
	}
	return ScheduleResult{
		Claimed: result.Claimed, Fired: result.Fired, Skipped: result.Skipped,
		Errors: result.Errors, LastTargetID: result.LastTargetID, LastSkipReason: result.LastSkipReason,
		Raw: result.Raw,
	}, nil
}

// ScheduleFireDueWithOptions runs due schedules and returns the scheduler
// outcome summary. It is the canonical alternative to ScheduleFireDue, whose
// ScheduleResult return type is retained for source compatibility.
func (c *Client) ScheduleFireDueWithOptions(ctx context.Context, opt ScheduleFireDueOptions) (ScheduleFireDueResult, error) {
	if err := validateScheduleFireDueOptions(opt); err != nil {
		return ScheduleFireDueResult{}, err
	}
	args := []any{"FLOW.SCHEDULE.FIRE_DUE"}
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendOpt(&args, "WORKER", opt.Worker)
	appendInt64Ptr(&args, "LEASE_MS", opt.LeaseMS)
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	appendIntPtr(&args, "LIMIT", opt.Limit)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	result, err := scheduleFireDueResult(c.typedReply(ctx, args...))
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	limit := effectiveFlowResponseLimit(
		opt.Limit, defaultFlowResponseLimitV080, 0,
	)
	if result.Claimed > int64(limit) {
		return ScheduleFireDueResult{}, fmt.Errorf(
			"FLOW.SCHEDULE.FIRE_DUE claimed %d schedules, limit is %d",
			result.Claimed,
			limit,
		)
	}
	return result, nil
}

func (c *Client) ScheduleList(ctx context.Context, opt ScheduleListOptions) ([]ScheduleResult, error) {
	if err := validateScheduleList(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.SCHEDULE.LIST"}
	appendOpt(&args, "KIND", canonicalAdminEnum(opt.Kind))
	appendOpt(&args, "STATE", opt.State)
	appendOpt(&args, "TIMEZONE", opt.Timezone)
	appendOpt(&args, "TARGET_TYPE", opt.TargetType)
	appendInt64Ptr(&args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(&args, "TO_MS", opt.ToMS)
	appendIntPtr(&args, "COUNT", opt.Count)
	appendBoolPtr(&args, "REV", opt.Rev)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	if err := validateDefaultedFlowResponseLimit(
		"FLOW.SCHEDULE.LIST", value, opt.Count,
		defaultFlowResponseLimitV080, maxClampedFlowListItemsV080,
	); err != nil {
		return nil, err
	}
	maps, err := mapList(value, nil)
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleResult, 0, len(maps))
	for _, item := range maps {
		result, err := scheduleResultFromMapWithCodec(item, c.codec)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, nil
}
