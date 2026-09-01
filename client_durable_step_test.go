package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

const durableStepGoldenKey = "__ferricstore_step__:sha256:ea8eb3a35639b63a2fd520c0ec03b3c5508553f55f02f6e52e8ac5d9e37121b7"

type scriptedExecutorResult struct {
	value any
	err   error
}

type scriptedExecutor struct {
	calls   [][]any
	results []scriptedExecutorResult
}

type testRequestDeliveryError struct {
	delivery RequestDelivery
}

func (e testRequestDeliveryError) Error() string { return "test request delivery failure" }
func (e testRequestDeliveryError) RequestDelivery() RequestDelivery {
	return e.delivery
}

func (e *scriptedExecutor) Do(_ context.Context, args ...any) (any, error) {
	e.calls = append(e.calls, append([]any(nil), args...))
	index := len(e.calls) - 1
	if index >= len(e.results) {
		return nil, errors.New("unexpected command")
	}
	return e.results[index].value, e.results[index].err
}

func TestDurableStepValueNameUsesCrossSDKGoldenHash(t *testing.T) {
	if got := durableStepValueName("charge-customer:v1"); got != durableStepGoldenKey {
		t.Fatalf("durable step key = %q; want %q", got, durableStepGoldenKey)
	}
}

func TestAdvanceCommandUsesCompactClaimOnNativeAndHTTPWirePaths(t *testing.T) {
	args := []any{
		"FLOW.STEP_CONTINUE", "flow-1", "lease-1", "charge", "schedule_warning",
		"FENCING", int64(7), "LEASE_MS", int64(30_000), "NOW", int64(100),
		"PARTITION", "tenant:1", "RETURN", "JOBS_COMPACT",
	}
	native, err := buildNativeCommand(args)
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := native.payload.(map[string]any)
	if !ok || payload["return"] != "JOBS_COMPACT" || payload["worker"] != nil {
		t.Fatalf("native step payload = %#v", native.payload)
	}

	httpCommand, err := encodeHTTPCommandWithState(args, newHTTPEncodeState(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	httpMap, ok := httpCommand.(map[string]any)
	if !ok || httpMap["command"] != "FLOW.STEP_CONTINUE" {
		t.Fatalf("HTTP step command = %#v", httpCommand)
	}
	wrapper, ok := httpMap["payload"].(map[string]any)
	if !ok {
		t.Fatalf("HTTP step payload = %#v", httpMap["payload"])
	}
	pairs, ok := wrapper[httpMapTag].([]any)
	if !ok {
		t.Fatalf("HTTP step payload = %#v", httpMap["payload"])
	}
	httpPayload := make(map[string]any, len(pairs))
	for _, rawPair := range pairs {
		pair, ok := rawPair.([]any)
		if !ok || len(pair) != 2 {
			t.Fatalf("HTTP step payload pair = %#v", rawPair)
		}
		httpPayload[asString(pair[0])] = pair[1]
	}
	if httpPayload["return"] != "JOBS_COMPACT" || httpPayload["worker"] != nil {
		t.Fatalf("HTTP step payload = %#v", httpMap["payload"])
	}
}

func TestAdvanceInfersClaimIdentityAndReturnsRefreshedClaim(t *testing.T) {
	exec := &fakeExecutor{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}}
	client := NewClientWithExecutor(exec)
	job := ClaimedItem{
		ID: "flow-1", LeaseToken: "lease-1", FencingToken: 7, PartitionKey: "tenant:1",
		Type: "charge", State: "running", RunState: "charge", Payload: "payload",
		Attributes: map[string]any{"tenant": "acme"},
	}

	refreshed, err := client.Advance(context.Background(), job, "schedule_warning")
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.ID != job.ID || refreshed.PartitionKey != job.PartitionKey ||
		refreshed.LeaseToken != "lease-2" || refreshed.FencingToken != 8 ||
		refreshed.RunState != "schedule_warning" || refreshed.State != "running" {
		t.Fatalf("refreshed claim = %#v", refreshed)
	}
	if refreshed.Type != job.Type || refreshed.Payload != job.Payload ||
		!reflect.DeepEqual(refreshed.Attributes, job.Attributes) {
		t.Fatalf("refreshed claim lost local projection fields: %#v", refreshed)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("commands = %#v", exec.calls)
	}
	call := exec.calls[0]
	assertCommandPrefix(t, call, []any{
		"FLOW.STEP_CONTINUE", "flow-1", "lease-1", "charge", "schedule_warning",
		"FENCING", int64(7), "LEASE_MS", int64(30_000),
	})
	assertCommandPair(t, call, "PARTITION", "tenant:1")
	assertCommandPair(t, call, "RETURN", "JOBS_COMPACT")
	assertCommandMissing(t, call, "WORKER")
}

func TestAdvanceRejectsInvalidClaimBeforeIO(t *testing.T) {
	tests := []struct {
		name string
		job  ClaimedItem
	}{
		{name: "missing run state", job: func() ClaimedItem { job := durableStepClaim(); job.RunState = ""; return job }()},
		{name: "zero fencing token", job: func() ClaimedItem { job := durableStepClaim(); job.FencingToken = 0; return job }()},
		{name: "non-running state", job: func() ClaimedItem { job := durableStepClaim(); job.State = "scheduled"; return job }()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{}
			client := NewClientWithExecutor(exec)
			if _, err := client.Advance(context.Background(), test.job, "schedule_warning"); err == nil {
				t.Fatal("expected validation error")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("validation performed I/O: %#v", exec.calls)
			}
		})
	}
}

func TestAdvanceAcceptsCompactClaimWithOmittedState(t *testing.T) {
	exec := &fakeExecutor{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}}
	client := NewClientWithExecutor(exec)
	job := durableStepClaim()
	job.State = ""

	refreshed, err := client.Advance(context.Background(), job, "schedule_warning")
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.State != "running" || refreshed.RunState != "schedule_warning" {
		t.Fatalf("refreshed compact claim = %#v", refreshed)
	}
}

func TestStepValidatesThenCommitsResultAndAdvancesAtomically(t *testing.T) {
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: durableStepFlowRecord(nil)},
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
	}}
	client := NewClientWithExecutor(exec)
	job := durableStepClaim()
	runs := 0

	refreshed, result, err := client.Step(
		context.Background(),
		job,
		"charge-customer:v1",
		func() (any, error) {
			runs++
			return map[string]any{"charge_id": "ch_1"}, nil
		},
		"schedule_warning",
	)
	if err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("closure runs = %d; want 1", runs)
	}
	if refreshed.LeaseToken != "lease-2" || refreshed.FencingToken != 8 || refreshed.RunState != "schedule_warning" {
		t.Fatalf("refreshed claim = %#v", refreshed)
	}
	if !reflect.DeepEqual(result, map[string]any{"charge_id": "ch_1"}) {
		t.Fatalf("result = %#v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("commands = %#v", exec.calls)
	}
	assertCommandPrefix(t, exec.calls[0], []any{
		"FLOW.EXTEND_LEASE", "flow-1", "lease-1", "FENCING", int64(7), "LEASE_MS", int64(30_000),
	})
	commit := exec.calls[1]
	assertCommandPrefix(t, commit, []any{
		"FLOW.STEP_CONTINUE", "flow-1", "lease-1", "charge", "schedule_warning",
	})
	assertCommandTriple(t, commit, "VALUE", durableStepGoldenKey, map[string]any{"charge_id": "ch_1"})
	assertCommandPair(t, commit, "RETURN", "JOBS_COMPACT")
}

func TestStepReplaysCommittedResultWithoutRunningOrAdvancing(t *testing.T) {
	preflight := durableStepFlowRecord(map[string]any{durableStepGoldenKey: map[string]any{
		"ref": "ref-1", "version": int64(1), "digest": "digest-1",
	}})
	preflight["run_state"] = "schedule_warning"
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: preflight},
		{value: []any{map[string]any{"charge_id": "ch_1"}}},
	}}
	client := NewClientWithExecutor(exec)
	runs := 0
	job := durableStepClaim()
	job.RunState = "schedule_warning"

	refreshed, result, err := client.Step(
		context.Background(),
		job,
		"charge-customer:v1",
		func() (any, error) {
			runs++
			return nil, errors.New("must not run")
		},
		"schedule_warning",
	)
	if err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("closure runs = %d; want 0", runs)
	}
	if refreshed.LeaseToken != "lease-1" || refreshed.FencingToken != 7 || refreshed.RunState != "schedule_warning" {
		t.Fatalf("validated claim = %#v", refreshed)
	}
	if !reflect.DeepEqual(result, map[string]any{"charge_id": "ch_1"}) {
		t.Fatalf("replayed result = %#v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("commands = %#v", exec.calls)
	}
	assertCommandPrefix(t, exec.calls[1], []any{"FLOW.VALUE.MGET", "ref-1"})
	for _, call := range exec.calls {
		if len(call) > 0 && call[0] == "FLOW.STEP_CONTINUE" {
			t.Fatalf("replay advanced the flow: %#v", exec.calls)
		}
	}
}

func TestStepReplayRejectsCommittedResultBeforeTargetState(t *testing.T) {
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: durableStepFlowRecord(map[string]any{durableStepGoldenKey: map[string]any{
			"ref": "ref-1",
		}})},
	}}
	client := NewClientWithExecutor(exec)
	runs := 0

	_, _, err := client.Step(
		context.Background(),
		durableStepClaim(),
		"charge-customer:v1",
		func() (any, error) {
			runs++
			return "must-not-run", nil
		},
		"schedule_warning",
	)

	if err == nil || !strings.Contains(err.Error(), "target state") {
		t.Fatalf("error = %v; want committed target-state mismatch", err)
	}
	if runs != 0 {
		t.Fatalf("closure runs = %d; want 0", runs)
	}
	if len(exec.calls) != 1 || exec.calls[0][0] != "FLOW.EXTEND_LEASE" {
		t.Fatalf("mismatched replay continued after preflight: %#v", exec.calls)
	}
}

func TestStepFailsClosedOnMalformedPreflightValueRefs(t *testing.T) {
	tests := []struct {
		name      string
		valueRefs any
	}{
		{name: "value refs is not a map", valueRefs: "malformed"},
		{name: "step descriptor missing ref", valueRefs: map[string]any{
			durableStepGoldenKey: map[string]any{"version": int64(1)},
		}},
		{name: "step descriptor is null", valueRefs: map[string]any{
			durableStepGoldenKey: nil,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			preflight := durableStepFlowRecord(nil)
			preflight["value_refs"] = test.valueRefs
			exec := &scriptedExecutor{results: []scriptedExecutorResult{{value: preflight}}}
			client := NewClientWithExecutor(exec)
			runs := 0

			_, _, err := client.Step(context.Background(), durableStepClaim(), "charge-customer:v1", func() (any, error) {
				runs++
				return "must-not-run", nil
			}, "schedule_warning")

			if err == nil {
				t.Fatal("malformed preflight value refs were accepted")
			}
			if runs != 0 {
				t.Fatalf("closure runs = %d; want 0", runs)
			}
			if len(exec.calls) != 1 || exec.calls[0][0] != "FLOW.EXTEND_LEASE" {
				t.Fatalf("malformed preflight continued to another command: %#v", exec.calls)
			}
		})
	}
}

func TestStepReplayRejectsMissingValueButAcceptsEncodedJSONNull(t *testing.T) {
	tests := []struct {
		name       string
		storedWire any
		wantErr    bool
	}{
		{name: "missing value", storedWire: nil, wantErr: true},
		{name: "encoded null", storedWire: []byte("null")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			preflight := durableStepFlowRecord(map[string]any{durableStepGoldenKey: map[string]any{"ref": "ref-1"}})
			preflight["payload"] = []byte(`"payload"`)
			preflight["run_state"] = "schedule_warning"
			exec := &scriptedExecutor{results: []scriptedExecutorResult{
				{value: preflight},
				{value: []any{test.storedWire}},
			}}
			client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))
			runs := 0

			job := durableStepClaim()
			job.RunState = "schedule_warning"
			_, result, err := client.Step(context.Background(), job, "charge-customer:v1", func() (any, error) {
				runs++
				return "must-not-run", nil
			}, "schedule_warning")

			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "missing") {
					t.Fatalf("error = %v; want missing stored value", err)
				}
			} else if err != nil || result != nil {
				t.Fatalf("encoded null result = %#v, error = %v", result, err)
			}
			if runs != 0 {
				t.Fatalf("closure runs = %d; want 0", runs)
			}
		})
	}
}

func TestStepNewCommitReturnsSameCodecTypeAsReplay(t *testing.T) {
	original := map[string]any{"amount": 150, "nested": []int{1, 2}}
	commitPreflight := durableStepFlowRecord(nil)
	commitPreflight["payload"] = []byte(`"payload"`)
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: commitPreflight},
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
	}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))

	_, committed, err := client.Step(context.Background(), durableStepClaim(), "charge-customer:v1", func() (any, error) {
		return original, nil
	}, "schedule_warning")
	if err != nil {
		t.Fatal(err)
	}
	replayPreflight := durableStepFlowRecord(map[string]any{durableStepGoldenKey: map[string]any{"ref": "ref-1"}})
	replayPreflight["payload"] = []byte(`"payload"`)
	replayPreflight["run_state"] = "schedule_warning"
	replayExec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: replayPreflight},
		{value: []any{[]byte(`{"amount":150,"nested":[1,2]}`)}},
	}}
	replayClient := NewClientWithExecutor(replayExec, WithCodec(JSONCodec{}))
	replayJob := durableStepClaim()
	replayJob.RunState = "schedule_warning"
	_, replayed, err := replayClient.Step(context.Background(), replayJob, "charge-customer:v1", func() (any, error) {
		return nil, errors.New("must not run")
	}, "schedule_warning")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(committed, replayed) {
		t.Fatalf("new result = %#v (%T); replay = %#v (%T)", committed, committed, replayed, replayed)
	}
	amount := committed.(map[string]any)["amount"]
	if _, ok := amount.(float64); !ok {
		t.Fatalf("committed amount type = %T; want codec-decoded float64", amount)
	}
}

func TestAdvanceOnlyInfersStateForCompactResponse(t *testing.T) {
	tests := []struct {
		name    string
		missing string
	}{
		{name: "missing physical state", missing: "state"},
		{name: "missing run state", missing: "run_state"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			malformedFull := map[string]any{
				"id": "flow-1", "partition_key": "tenant:1", "state": "running",
				"run_state": "schedule_warning", "lease_token": "lease-2", "fencing_token": int64(8),
			}
			delete(malformedFull, test.missing)
			client := NewClientWithExecutor(&fakeExecutor{value: malformedFull})

			_, err := client.Advance(context.Background(), durableStepClaim(), "schedule_warning")

			if err == nil || !strings.Contains(err.Error(), "missing "+strings.ReplaceAll(test.missing, "_", " ")) {
				t.Fatalf("error = %v; want malformed full-response %s error", err, test.missing)
			}
			if !errors.Is(err, ErrDurableMutationUncertain) {
				t.Fatalf("error = %v; malformed post-commit response must be uncertain", err)
			}
		})
	}
}

func TestStepDoesNotRunClosureWhenLeaseValidationFails(t *testing.T) {
	stale := errors.New("ERR stale fencing token")
	exec := &scriptedExecutor{results: []scriptedExecutorResult{{err: stale}}}
	client := NewClientWithExecutor(exec)
	runs := 0

	_, _, err := client.Step(context.Background(), durableStepClaim(), "charge-customer:v1", func() (any, error) {
		runs++
		return "charged", nil
	}, "schedule_warning")
	if !errors.Is(err, stale) {
		t.Fatalf("error = %v; want %v", err, stale)
	}
	if runs != 0 {
		t.Fatalf("closure runs = %d; want 0", runs)
	}
	if len(exec.calls) != 1 || exec.calls[0][0] != "FLOW.EXTEND_LEASE" {
		t.Fatalf("commands = %#v", exec.calls)
	}
}

func TestStepDoesNotAdvanceWhenClosureFails(t *testing.T) {
	runErr := errors.New("provider unavailable")
	exec := &scriptedExecutor{results: []scriptedExecutorResult{{value: durableStepFlowRecord(nil)}}}
	client := NewClientWithExecutor(exec)

	_, _, err := client.Step(context.Background(), durableStepClaim(), "charge-customer:v1", func() (any, error) {
		return nil, runErr
	}, "schedule_warning")
	if !errors.Is(err, runErr) {
		t.Fatalf("error = %v; want %v", err, runErr)
	}
	if len(exec.calls) != 1 || exec.calls[0][0] != "FLOW.EXTEND_LEASE" {
		t.Fatalf("commands = %#v", exec.calls)
	}
}

func TestStepReturnsCommitFailureAfterClosureRuns(t *testing.T) {
	commitErr := errors.New("connection lost after commit")
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: durableStepFlowRecord(nil)},
		{err: commitErr},
	}}
	client := NewClientWithExecutor(exec)
	runs := 0

	_, _, err := client.Step(context.Background(), durableStepClaim(), "charge-customer:v1", func() (any, error) {
		runs++
		return "charged", nil
	}, "schedule_warning")
	if !errors.Is(err, commitErr) {
		t.Fatalf("error = %v; want %v", err, commitErr)
	}
	if !errors.Is(err, ErrDurableMutationUncertain) {
		t.Fatalf("error = %v; want durable mutation uncertainty", err)
	}
	if runs != 1 || len(exec.calls) != 2 {
		t.Fatalf("closure runs = %d, commands = %#v", runs, exec.calls)
	}
}

func TestAdvanceClassifiesDefiniteAndAmbiguousHTTPFailures(t *testing.T) {
	tests := []struct {
		name      string
		httpError *HTTPError
		uncertain bool
	}{
		{
			name:      "request rejected",
			httpError: &HTTPError{StatusCode: 400, Code: "invalid_request", Message: "bad request"},
		},
		{
			name:      "authentication rejected",
			httpError: &HTTPError{StatusCode: 401, Code: "unauthorized", Message: "unauthorized"},
		},
		{
			name:      "pre-send size limit",
			httpError: &HTTPError{Code: "request_too_large", Message: "too large"},
		},
		{
			name:      "closed before send",
			httpError: &HTTPError{Code: "closed", Message: "closed"},
		},
		{
			name:      "transport failure",
			httpError: &HTTPError{Code: "transport_error", Message: "connection lost"},
			uncertain: true,
		},
		{
			name:      "request timeout after dispatch",
			httpError: &HTTPError{StatusCode: 408, Code: "request_timeout", Message: "timed out"},
			uncertain: true,
		},
		{
			name:      "unclassified client status",
			httpError: &HTTPError{StatusCode: 429, Code: "rate_limited", Message: "slow down"},
			uncertain: true,
		},
		{
			name:      "successful status malformed response",
			httpError: &HTTPError{StatusCode: 200, Code: "invalid_response", Message: "bad response"},
			uncertain: true,
		},
		{
			name: "server overload explicitly rejected before dispatch",
			httpError: &HTTPError{
				StatusCode: 503, Code: "server_overloaded", Message: "overloaded", SafeToRetry: true,
			},
		},
		{
			name:      "command rejection in successful envelope",
			httpError: &HTTPError{StatusCode: 200, Code: "noperm", Message: "forbidden"},
		},
		{
			name:      "unclassified command outcome",
			httpError: &HTTPError{StatusCode: 200, Code: "raft_timeout", Message: "outcome unknown"},
			uncertain: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &scriptedExecutor{results: []scriptedExecutorResult{{err: test.httpError}}}
			_, err := NewClientWithExecutor(exec).Advance(
				context.Background(), durableStepClaim(), "schedule_warning",
			)
			if !errors.Is(err, test.httpError) {
				t.Fatalf("error = %v; want original HTTP error", err)
			}
			if got := errors.Is(err, ErrDurableMutationUncertain); got != test.uncertain {
				t.Fatalf("uncertain = %v; want %v (error %v)", got, test.uncertain, err)
			}
		})
	}
}

func TestAdvanceClassifiesNativeServerDeliveryConservatively(t *testing.T) {
	tests := []struct {
		name      string
		nativeErr NativeError
		uncertain bool
	}{
		{name: "bad request", nativeErr: NativeError{Status: 6, Value: "bad request"}},
		{name: "known stale lease code", nativeErr: NativeError{Status: 1, Value: map[string]any{"code": "stale_lease"}}},
		{name: "known stale lease message", nativeErr: NativeError{Status: 1, Value: "ERR stale flow lease"}},
		{name: "generic outcome timeout", nativeErr: NativeError{Status: 1, Value: map[string]any{"code": "timeout"}}, uncertain: true},
		{name: "future server status", nativeErr: NativeError{Status: 99, Value: map[string]any{"message": "future"}}, uncertain: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &scriptedExecutor{results: []scriptedExecutorResult{{err: test.nativeErr}}}
			_, err := NewClientWithExecutor(exec).Advance(
				context.Background(), durableStepClaim(), "schedule_warning",
			)
			var nativeErr NativeError
			if !errors.As(err, &nativeErr) || nativeErr.Status != test.nativeErr.Status {
				t.Fatalf("error = %v; want native status %d", err, test.nativeErr.Status)
			}
			if got := errors.Is(err, ErrDurableMutationUncertain); got != test.uncertain {
				t.Fatalf("uncertain = %v; want %v (error %v)", got, test.uncertain, err)
			}
		})
	}
}

func TestCustomExecutorRequestDeliveryMetadataControlsMutationRecovery(t *testing.T) {
	for _, test := range []struct {
		name     string
		delivery RequestDelivery
	}{
		{name: "not sent", delivery: RequestDeliveryNotSent},
		{name: "rejected", delivery: RequestDeliveryRejected},
		{name: "unknown", delivery: RequestDeliveryUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := testRequestDeliveryError{delivery: test.delivery}
			exec := &scriptedExecutor{results: []scriptedExecutorResult{{err: failure}}}
			_, err := NewClientWithExecutor(exec).Advance(
				context.Background(), durableStepClaim(), "schedule_warning",
			)
			wantUnknown := test.delivery == RequestDeliveryUnknown
			if got := errors.Is(err, ErrDurableMutationUncertain); got != wantUnknown {
				t.Fatalf("delivery %d uncertain = %v; want %v", test.delivery, got, wantUnknown)
			}
		})
	}
}

func TestDurableMutationTreatsHTTPEncodingFailureAsPreSendRejection(t *testing.T) {
	executor, err := NewHTTPExecutorFromURL("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()

	_, err = executor.Do(context.Background(), "SET", "key", make(chan int))
	if err == nil {
		t.Fatal("unsupported HTTP value was accepted")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || !httpErr.SafeToRetry {
		t.Fatalf("HTTP encoding error = %#v; want a definite pre-send HTTP error", err)
	}
	classified := durableMutationCommandError("FLOW.STEP_CONTINUE", err)
	if errors.Is(classified, ErrDurableMutationUncertain) {
		t.Fatalf("pre-send HTTP encoding error was classified uncertain: %v", classified)
	}
}

func TestDurableMutationTreatsNativeEncodingFailureAsPreSendRejection(t *testing.T) {
	executor := NewNativeExecutor("unused")
	defer func() { _ = executor.Close() }()

	_, err := executor.writeRequest(
		context.Background(), nativeOpCommandExec, 1, 1,
		map[string]any{"value": make(chan int)}, 0, nil, false,
	)
	if err == nil {
		t.Fatal("unsupported native value was accepted")
	}
	var notSent *commandNotSentError
	if !errors.As(err, &notSent) {
		t.Fatalf("native encoding error = %#v; want commandNotSentError", err)
	}
	classified := durableMutationCommandError("FLOW.STEP_CONTINUE", err)
	if errors.Is(classified, ErrDurableMutationUncertain) {
		t.Fatalf("pre-send native encoding error was classified uncertain: %v", classified)
	}
}

func TestDurableMutationTreatsClosedNativeExecutorAsNotSent(t *testing.T) {
	executor := NewNativeExecutor("unused")
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := executor.Do(context.Background(), "FLOW.STEP_CONTINUE", "flow-1", "lease-1", "charge", "next")
	var notSent *commandNotSentError
	if !errors.As(err, &notSent) {
		t.Fatalf("closed native executor error = %#v; want commandNotSentError", err)
	}
	if classified := durableMutationCommandError("FLOW.STEP_CONTINUE", err); errors.Is(classified, ErrDurableMutationUncertain) {
		t.Fatalf("closed native executor was classified uncertain: %v", classified)
	}
}

func TestDurableStepRejectsInvalidInputsBeforeIO(t *testing.T) {
	tests := []struct {
		name     string
		job      ClaimedItem
		stepName string
		toState  string
		run      func() (any, error)
	}{
		{name: "missing id", job: func() ClaimedItem { job := durableStepClaim(); job.ID = ""; return job }(), stepName: "step", toState: "next", run: func() (any, error) { return nil, nil }},
		{name: "missing run state", job: func() ClaimedItem { job := durableStepClaim(); job.RunState = ""; return job }(), stepName: "step", toState: "next", run: func() (any, error) { return nil, nil }},
		{name: "zero fencing token", job: func() ClaimedItem { job := durableStepClaim(); job.FencingToken = 0; return job }(), stepName: "step", toState: "next", run: func() (any, error) { return nil, nil }},
		{name: "non-running state", job: func() ClaimedItem { job := durableStepClaim(); job.State = "scheduled"; return job }(), stepName: "step", toState: "next", run: func() (any, error) { return nil, nil }},
		{name: "missing step name", job: durableStepClaim(), stepName: "", toState: "next", run: func() (any, error) { return nil, nil }},
		{name: "invalid utf8 step name", job: durableStepClaim(), stepName: string([]byte{0xff}), toState: "next", run: func() (any, error) { return nil, nil }},
		{name: "missing target", job: durableStepClaim(), stepName: "step", toState: "", run: func() (any, error) { return nil, nil }},
		{name: "missing closure", job: durableStepClaim(), stepName: "step", toState: "next"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &fakeExecutor{}
			client := NewClientWithExecutor(exec)
			_, _, err := client.Step(context.Background(), tt.job, tt.stepName, tt.run, tt.toState)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("validation performed I/O: %#v", exec.calls)
			}
		})
	}
}

func durableStepClaim() ClaimedItem {
	return ClaimedItem{
		ID: "flow-1", LeaseToken: "lease-1", FencingToken: 7, PartitionKey: "tenant:1",
		Type: "charge", State: "running", RunState: "charge", Payload: "payload",
		Attributes: map[string]any{"tenant": "acme"},
	}
}

func durableStepFlowRecord(valueRefs map[string]any) map[string]any {
	return map[string]any{
		"id": "flow-1", "type": "charge", "state": "running", "run_state": "charge",
		"partition_key": "tenant:1", "lease_token": "lease-1", "fencing_token": int64(7),
		"payload": "payload", "attributes": map[string]any{"tenant": "acme"},
		"value_refs": valueRefs,
	}
}

func assertCommandPrefix(t *testing.T, got, want []any) {
	t.Helper()
	if len(got) < len(want) || !reflect.DeepEqual(got[:len(want)], want) {
		t.Fatalf("command = %#v; want prefix %#v", got, want)
	}
}

func assertCommandPair(t *testing.T, command []any, name string, value any) {
	t.Helper()
	for index := 0; index+1 < len(command); index++ {
		if command[index] == name && reflect.DeepEqual(command[index+1], value) {
			return
		}
	}
	t.Fatalf("command %#v does not contain %s %#v", command, name, value)
}

func assertCommandTriple(t *testing.T, command []any, name string, first, second any) {
	t.Helper()
	for index := 0; index+2 < len(command); index++ {
		if command[index] == name && reflect.DeepEqual(command[index+1], first) && reflect.DeepEqual(command[index+2], second) {
			return
		}
	}
	t.Fatalf("command %#v does not contain %s %#v %#v", command, name, first, second)
}

func assertCommandMissing(t *testing.T, command []any, name string) {
	t.Helper()
	for _, value := range command {
		if text, ok := value.(string); ok && strings.EqualFold(text, name) {
			t.Fatalf("command %#v unexpectedly contains %s", command, name)
		}
	}
}
