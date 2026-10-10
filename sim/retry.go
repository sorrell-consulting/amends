// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"fmt"
	"math"
)

// Retry is explicit logical-clock state: three unsuccessful observations park
// an actor until a resume event. Unknown outcomes count as unsuccessful replies,
// regardless of their durable outcome. No wall clock, sleeping, or jitter.
type Retry struct {
	Attempts  int
	Next      int64
	Exhausted bool
}

func (r Retry) Failed(now int64) (Retry, error) {
	if r.Exhausted || now < r.Next || r.Attempts < 0 || r.Attempts >= 3 {
		return r, fmt.Errorf("retry before due time or after exhaustion")
	}
	r.Attempts++
	delay := int64(1) << (r.Attempts - 1)
	if now < 0 || now > math.MaxInt64-delay {
		return r, fmt.Errorf("retry clock overflow")
	}
	r.Next = now + delay
	r.Exhausted = r.Attempts >= 3
	return r, nil
}
func (m *Model) RetryState(actor string, partition int) Retry {
	return m.retries[fmt.Sprintf("%s/%d", actor, partition)]
}
