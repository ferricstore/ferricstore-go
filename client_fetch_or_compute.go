package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

// legacyFetchOrComputeToken keeps the released protocol's coordinator value
// opaque while allowing completion helpers to select its tokenless wire shape.
type legacyFetchOrComputeToken struct {
	coordinator any
}

func (c *Client) FetchOrCompute(ctx context.Context, key string, ttlMS int64, hint string) (FetchOrComputeResult, error) {
	if ttlMS <= 0 {
		return FetchOrComputeResult{}, errors.New("FETCH_OR_COMPUTE ttl must be positive")
	}
	args := []any{"FETCH_OR_COMPUTE", key, ttlMS}
	if hint != "" {
		args = append(args, hint)
	}
	response, err := c.typedReply(ctx, args...)
	if err != nil {
		return FetchOrComputeResult{}, err
	}
	items, ok := response.([]any)
	if !ok || len(items) < 1 {
		return FetchOrComputeResult{}, fmt.Errorf("expected fetch_or_compute response")
	}
	status, err := responseString(items[0], nil)
	if err != nil {
		return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute status: %w", err)
	}
	switch status {
	case "hit":
		if len(items) != 2 {
			return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute hit response length %d", len(items))
		}
		decoded, err := decodeValue(c.codec, items[1])
		return FetchOrComputeResult{Status: status, Value: decoded}, err
	case "compute":
		return fetchOrComputeMiss(items)
	default:
		return FetchOrComputeResult{}, fmt.Errorf("unsupported fetch_or_compute status %q", status)
	}
}

func fetchOrComputeMiss(items []any) (FetchOrComputeResult, error) {
	switch len(items) {
	case 2:
		coordinator, err := fetchOrComputeToken(items[1:])
		if err != nil {
			return FetchOrComputeResult{}, err
		}
		return FetchOrComputeResult{
			Status:       "compute",
			ComputeToken: legacyFetchOrComputeToken{coordinator: coordinator},
		}, nil
	case 3:
		if _, err := responseString(items[1], nil); err != nil {
			return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute hint: %w", err)
		}
		token, err := fetchOrComputeToken(items[2:])
		if err != nil {
			return FetchOrComputeResult{}, err
		}
		return FetchOrComputeResult{Status: "compute", ComputeToken: token}, nil
	default:
		return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute compute response length %d", len(items))
	}
}

// FetchOrComputeResult publishes a computed value using the tokenless
// completion shape supported by released FerricStore versions.
func (c *Client) FetchOrComputeResult(ctx context.Context, key string, value any, ttlMS int64) (bool, error) {
	return c.fetchOrComputeResult(ctx, key, value, ttlMS, nil)
}

// FetchOrComputeResultWithToken publishes a computed value using the opaque
// token returned by FetchOrCompute. A token from a released tokenless server
// is recognized and automatically uses that server's compatible wire shape.
func (c *Client) FetchOrComputeResultWithToken(ctx context.Context, key string, value any, ttlMS int64, computeToken any) (bool, error) {
	return c.fetchOrComputeResult(ctx, key, value, ttlMS, []any{computeToken})
}

func (c *Client) fetchOrComputeResult(ctx context.Context, key string, value any, ttlMS int64, computeToken []any) (bool, error) {
	if ttlMS < 0 {
		return false, errors.New("FETCH_OR_COMPUTE_RESULT ttl must be non-negative")
	}
	completion, err := fetchOrComputeCompletion(computeToken)
	if err != nil {
		return false, err
	}
	encoded, err := c.encode(value)
	if err != nil {
		return false, err
	}
	args := []any{"FETCH_OR_COMPUTE_RESULT", key}
	if !completion.legacy {
		args = append(args, completion.token)
	}
	args = append(args, encoded, ttlMS)
	response, err := c.typedReply(ctx, args...)
	return responseOK(response, err)
}

// FetchOrComputeError publishes a compute failure using the released
// tokenless completion shape.
func (c *Client) FetchOrComputeError(ctx context.Context, key, message string) (bool, error) {
	return c.fetchOrComputeError(ctx, key, message, nil)
}

// FetchOrComputeErrorWithToken publishes a compute failure using the opaque
// token returned by FetchOrCompute.
func (c *Client) FetchOrComputeErrorWithToken(ctx context.Context, key, message string, computeToken any) (bool, error) {
	return c.fetchOrComputeError(ctx, key, message, []any{computeToken})
}

func (c *Client) fetchOrComputeError(ctx context.Context, key, message string, computeToken []any) (bool, error) {
	completion, err := fetchOrComputeCompletion(computeToken)
	if err != nil {
		return false, err
	}
	args := []any{"FETCH_OR_COMPUTE_ERROR", key}
	if !completion.legacy {
		args = append(args, completion.token)
	}
	args = append(args, message)
	response, err := c.typedReply(ctx, args...)
	return responseOK(response, err)
}

type fetchOrComputeCompletionPlan struct {
	token  any
	legacy bool
}

func fetchOrComputeCompletion(tokens []any) (fetchOrComputeCompletionPlan, error) {
	if len(tokens) == 0 {
		return fetchOrComputeCompletionPlan{legacy: true}, nil
	}
	if len(tokens) != 1 {
		return fetchOrComputeCompletionPlan{}, fmt.Errorf(
			"fetch_or_compute completion accepts at most one compute token, got %d", len(tokens),
		)
	}
	if _, ok := tokens[0].(legacyFetchOrComputeToken); ok {
		return fetchOrComputeCompletionPlan{legacy: true}, nil
	}
	token, err := fetchOrComputeToken(tokens)
	if err != nil {
		return fetchOrComputeCompletionPlan{}, err
	}
	return fetchOrComputeCompletionPlan{token: token}, nil
}

func fetchOrComputeToken(tokens []any) (any, error) {
	if len(tokens) != 1 {
		return nil, fmt.Errorf("fetch_or_compute completion requires exactly one compute token, got %d", len(tokens))
	}
	switch token := tokens[0].(type) {
	case string:
		if token != "" {
			return token, nil
		}
	case []byte:
		if len(token) != 0 {
			return token, nil
		}
	}
	return nil, fmt.Errorf("fetch_or_compute compute token must be a non-empty string or byte slice, got %T", tokens[0])
}
