// Package ferricstore provides a Go client for FerricStore and FerricFlow.
//
// The client speaks FerricStore's Redis-compatible command protocol. It exposes
// typed helpers for FerricFlow state machines, durable queues, workflow workers,
// value refs, locks, rate limits, cluster/admin commands, and common data
// structures while keeping Client.Command available for unsupported commands.
package ferricstore
