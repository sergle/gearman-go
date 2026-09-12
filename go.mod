module github.com/sergle/gearman-go

// go 1.23 is a floor, not a preference: (*Client).startTimer reuses one
// time.Timer across calls, and only from 1.23 does Reset clear a value already
// sent on the channel. Under 1.21 semantics a response that raced the fire left
// a stale tick for the next caller to read as an immediate timeout.
go 1.23
