package ferricstore

import (
	"context"
	"fmt"
)

func (c *Client) FetchOrCompute(ctx context.Context, key string, ttlMS int64, hint string) (FetchOrComputeResult, error) {
	if err := validateNativeTTLMSV080("FETCH_OR_COMPUTE", ttlMS, false); err != nil {
		return FetchOrComputeResult{}, err
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
	if len(items) != 3 {
		return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute compute response length %d", len(items))
	}
	hint, err := responseString(items[1], nil)
	if err != nil {
		return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute hint: %w", err)
	}
	token, err := fetchOrComputeToken(items[2])
	if err != nil {
		return FetchOrComputeResult{}, err
	}
	return FetchOrComputeResult{
		Status:         "compute",
		Hint:           hint,
		OwnershipToken: token,
	}, nil
}

// FetchOrComputeResult publishes a computed value using the ownership token
// returned in the corresponding compute response.
func (c *Client) FetchOrComputeResult(ctx context.Context, key string, ownershipToken, value any, ttlMS int64) (bool, error) {
	if err := validateNativeTTLMSV080("FETCH_OR_COMPUTE_RESULT", ttlMS, true); err != nil {
		return false, err
	}
	token, err := fetchOrComputeToken(ownershipToken)
	if err != nil {
		return false, err
	}
	encoded, err := c.encode(value)
	if err != nil {
		return false, err
	}
	args := []any{"FETCH_OR_COMPUTE_RESULT", key, token, encoded, ttlMS}
	response, err := c.typedReply(ctx, args...)
	return responseOK(response, err)
}

// FetchOrComputeError publishes a compute failure using the ownership token
// returned in the corresponding compute response.
func (c *Client) FetchOrComputeError(ctx context.Context, key string, ownershipToken any, message string) (bool, error) {
	token, err := fetchOrComputeToken(ownershipToken)
	if err != nil {
		return false, err
	}
	args := []any{"FETCH_OR_COMPUTE_ERROR", key, token, message}
	response, err := c.typedReply(ctx, args...)
	return responseOK(response, err)
}

func fetchOrComputeToken(value any) (any, error) {
	switch token := value.(type) {
	case string:
		if token != "" {
			return token, nil
		}
	case []byte:
		if len(token) != 0 {
			return token, nil
		}
	}
	if token, ok := commandText(value); ok && token != "" {
		return token, nil
	}
	return nil, fmt.Errorf("fetch_or_compute ownership token must be a non-empty string or byte slice, got %T", value)
}
