package ferricstore

import "errors"

func newHTTPTransportContextError(contextErr error) *HTTPError {
	code, retryable := classifyHTTPTransportError(contextErr, contextErr)
	return &HTTPError{
		Code: code, Message: "FerricStore HTTP request failed", Retryable: retryable, Cause: contextErr,
	}
}

func isHTTPTransportError(err error) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.Code {
	case "transport_canceled", "transport_timeout", "transport_error":
		return true
	default:
		return false
	}
}
