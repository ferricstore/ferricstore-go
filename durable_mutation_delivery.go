package ferricstore

import (
	"net/http"
	"strings"
)

func (e NativeError) RequestDelivery() RequestDelivery {
	switch e.Status {
	case nativeStatusAuth, nativeStatusNoPerm, nativeStatusBusy, nativeStatusReroute, nativeStatusBadRequest:
		return RequestDeliveryRejected
	case nativeStatusError:
		if safe, _ := nativeErrorField(e.Value, "safe_to_retry").(bool); safe ||
			knownDurableRejectionCode(e.Kind) ||
			knownDurableRejectionCode(nativeErrorField(e.Value, "code")) ||
			knownDurableRejectionCode(nativeErrorField(e.Value, "error_code")) ||
			knownDurableRejectionMessage(e.Value) ||
			knownDurableRejectionMessage(nativeErrorField(e.Value, "message")) {
			return RequestDeliveryRejected
		}
	}
	return RequestDeliveryUnknown
}

func (e *HTTPError) RequestDelivery() RequestDelivery {
	if e == nil {
		return RequestDeliveryUnknown
	}
	if e.StatusCode == http.StatusRequestTimeout || e.Code == "request_timeout" {
		return RequestDeliveryUnknown
	}
	if e.StatusCode == 0 {
		switch e.Code {
		case "closed", "invalid_request", "request_too_large", "too_many_commands", "client_overloaded":
			return RequestDeliveryNotSent
		default:
			return RequestDeliveryUnknown
		}
	}
	if e.SafeToRetry {
		return RequestDeliveryRejected
	}
	if e.StatusCode >= 500 {
		return RequestDeliveryUnknown
	}
	if e.StatusCode == http.StatusOK {
		if knownDurableRejectionCode(e.Code) {
			return RequestDeliveryRejected
		}
		return RequestDeliveryUnknown
	}
	switch e.StatusCode {
	case 400, 401, 403, 404, 405, 406, 411, 413, 414, 415, 422, 426, 431:
		return RequestDeliveryRejected
	default:
		return RequestDeliveryUnknown
	}
}

func knownDurableRejectionMessage(value any) bool {
	message := strings.ToLower(strings.TrimSpace(asString(value)))
	for _, prefix := range []string{"noauth ", "noperm "} {
		if strings.HasPrefix(message, prefix) {
			return true
		}
	}
	for _, fragment := range []string{
		"err stale flow lease",
		"err stale lease",
		"err stale token",
		"err wrong state",
		"err flow not found",
		"err flow does not exist",
		"err wrong number of arguments",
		"err syntax error",
		"err request too large",
	} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func knownDurableRejectionCode(value any) bool {
	code := strings.ToLower(asString(value))
	switch code {
	case "auth", "unauthenticated", "unauthorized", "noperm", "forbidden",
		"bad_request", "invalid_command", "invalid_request", "not_found",
		"flow_not_found", "stale_lease", "wrong_state", "conflict", "request_too_large":
		return true
	default:
		return false
	}
}
