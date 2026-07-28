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
	CatchupPolicy  string
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

type ScheduleRecord struct {
	ID     string
	FlowID string
	Kind   string
	State  string
	Target map[string]any
	// CreatedAtMS is the logical timestamp at which the schedule was created.
	CreatedAtMS int64
	// EveryMS is set only for interval schedules.
	EveryMS  *int64
	Timezone string
	Cron     string
	// CatchupPolicy is "fire_once" for interval schedules and empty otherwise.
	CatchupPolicy string
	// CoalescedCount is the cumulative number of elapsed interval occurrences
	// intentionally not replayed as targets.
	CoalescedCount int64
	// LastCatchupAtMS is the wall-clock recovery time of the latest catch-up.
	LastCatchupAtMS *int64
	// LastCoalescedCount is the number coalesced by the latest recovery fire.
	LastCoalescedCount int64
	OverlapPolicy      string
	// OverlapRetryMS is an optional queue-after-previous retry override.
	OverlapRetryMS       *int64
	NextRunAtMS          *int64
	LastFireAtMS         *int64
	FireCount            int64
	Attempts             int64
	MaxFires             *int64
	EndAtMS              *int64
	LastOverlapAtMS      *int64
	LastOverlapTargetID  string
	LastOverlapReason    string
	LastSkippedAtMS      *int64
	SkippedCount         int64
	OverlapQueuedDueAtMS *int64
	EndReason            string
	LastPlanningError    string
	LastTargetID         string
	Raw                  map[string]any
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
	Claimed int64
	Fired   int64
	Skipped int64
	// Coalesced is the saturated aggregate of elapsed occurrences not replayed
	// across this bounded batch.
	Coalesced int64
	Errors    []ScheduleFireDueError
	// ClaimError reports a batch-level failure to request a later claim wave.
	ClaimError     string
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
	Schedule ScheduleRecord
	Raw      map[string]any
}

func (c *Client) ScheduleCreate(ctx context.Context, id string, opt ScheduleOptions) (ScheduleRecord, error) {
	if err := validateScheduleCreate(id, opt); err != nil {
		return ScheduleRecord{}, err
	}
	target, err := c.encodeScheduleTarget(opt.Target)
	if err != nil {
		return ScheduleRecord{}, err
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
	appendOpt(&args, "CATCHUP_POLICY", canonicalAdminEnum(opt.CatchupPolicy))
	appendOpt(&args, "OVERLAP_POLICY", canonicalAdminEnum(opt.OverlapPolicy))
	appendInt64Ptr(&args, "OVERLAP_RETRY_MS", opt.OverlapRetryMS)
	appendInt64Ptr(&args, "MAX_FIRES", opt.MaxFires)
	appendInt64Ptr(&args, "END_AT_MS", opt.EndAtMS)
	appendBoolPtr(&args, "OVERWRITE", opt.Overwrite)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	return scheduleRecordWithCodec(value, err, c.codec)
}

func (c *Client) ScheduleGet(ctx context.Context, id string, deadlineMS *int64) (*ScheduleRecord, error) {
	if err := validateScheduleGet(id, deadlineMS); err != nil {
		return nil, err
	}
	args := []any{"FLOW.SCHEDULE.GET", id}
	appendInt64Ptr(&args, "DEADLINE_MS", deadlineMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := scheduleRecordWithCodec(value, nil, c.codec)
	return &result, err
}

// ScheduleFire fires one schedule and returns both the outcome and updated record.
func (c *Client) ScheduleFire(ctx context.Context, id string, opt ScheduleFireOptions) (ScheduleFireResult, error) {
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

func (c *Client) SchedulePause(ctx context.Context, id string, opt ScheduleStatusOptions) (ScheduleRecord, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.PAUSE", id, opt)
}

func (c *Client) ScheduleResume(ctx context.Context, id string, opt ScheduleStatusOptions) (ScheduleRecord, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.RESUME", id, opt)
}

func (c *Client) ScheduleDelete(ctx context.Context, id string, opt ScheduleStatusOptions) error {
	if err := validateScheduleStatus(id, opt); err != nil {
		return err
	}
	args := []any{"FLOW.SCHEDULE.DELETE", id}
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return err
	}
	if !isOK(value) {
		return fmt.Errorf("FLOW.SCHEDULE.DELETE response must be OK")
	}
	return nil
}

func (c *Client) scheduleStatus(ctx context.Context, command, id string, opt ScheduleStatusOptions) (ScheduleRecord, error) {
	if err := validateScheduleStatus(id, opt); err != nil {
		return ScheduleRecord{}, err
	}
	args := []any{command, id}
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return ScheduleRecord{}, err
	}
	return scheduleRecordWithCodec(value, nil, c.codec)
}

// ScheduleFireDue runs one bounded due-schedule pass.
func (c *Client) ScheduleFireDue(ctx context.Context, opt ScheduleFireDueOptions) (ScheduleFireDueResult, error) {
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

func (c *Client) ScheduleList(ctx context.Context, opt ScheduleListOptions) ([]ScheduleRecord, error) {
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
	out := make([]ScheduleRecord, 0, len(maps))
	for _, item := range maps {
		result, err := scheduleRecordFromMapWithCodec(item, c.codec)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, nil
}
