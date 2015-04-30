// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reqtrace

import (
	"sync"
	"time"
)

type span struct {
	// Fixed at creation.
	desc  string
	start time.Time

	// Updated by report functions.
	end time.Time
	err error
}

// All of the state for a particular trace root. The zero value is usable.
type traceState struct {
	mu sync.Mutex

	// The list of spans associated with this state. Append-only.
	//
	// GUARDED_BY(mu)
	spans []*span
}

func (ts *traceState) report(span int, err error) {
	// TODO(jacobsa): Do something interesting.
}

// Associate a new span with the trace. Return a function that will report its
// completion.
func (ts *traceState) CreateSpan(desc string) (report ReportFunc) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	index := len(ts.spans)
	ts.spans = append(ts.spans, &span{desc: desc, start: time.Now()})

	report = func(err error) { ts.report(index, err) }
	return
}

// Log information about the spans in this trace.
func (ts *traceState) Log() {
	// TODO(jacobsa): Do something interesting.
}
