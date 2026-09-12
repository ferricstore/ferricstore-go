package ferricstore

import "errors"

// RequestDelivery describes whether a failed command reached FerricStore.
// Mutation recovery must only issue a fallback write for NotSent or Rejected;
// Unknown may hide a successful commit and requires read/reclaim recovery.
type RequestDelivery uint8

const (
	RequestDeliveryUnknown RequestDelivery = iota
	RequestDeliveryNotSent
	RequestDeliveryRejected
)

// RequestDeliveryFailure is implemented by transport and server errors whose
// delivery outcome is known. Custom executors should implement this interface
// when they can prove that a request was not sent or was rejected.
type RequestDeliveryFailure interface {
	error
	RequestDelivery() RequestDelivery
}

// commandNotSentError marks a local command failure that happened before any
// request bytes were written. Mutation callers can safely distinguish it from
// a transport failure whose application outcome is unknown.
type commandNotSentError struct{ cause error }

func (e *commandNotSentError) Error() string { return e.cause.Error() }
func (e *commandNotSentError) Unwrap() error { return e.cause }
func (e *commandNotSentError) RequestDelivery() RequestDelivery {
	return RequestDeliveryNotSent
}

func markCommandNotSent(err error) error {
	if err == nil {
		return nil
	}
	var marked *commandNotSentError
	if errors.As(err, &marked) {
		return err
	}
	return &commandNotSentError{cause: err}
}
