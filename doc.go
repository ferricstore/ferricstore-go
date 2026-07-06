// Package ferricstore provides a Go client for FerricStore and FerricFlow.
//
// The client speaks FerricStore's native command protocol. It exposes
// typed helpers for FerricFlow state machines, durable queues, workflow workers,
// value refs, locks, rate limits, cluster/admin commands, management-plane
// reads/mutations, topology-aware routing, and common data structures while
// keeping Client.Command available for unsupported commands.
package ferricstore
