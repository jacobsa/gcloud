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
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

type span struct {
	// Fixed at creation.
	desc  string
	start time.Time

	// Updated by report functions.
	finished bool
	end      time.Time
	err      error
}

// All of the state for a particular trace root. The zero value is usable.
type traceState struct {
	mu sync.Mutex

	// The list of spans associated with this state. Append-only.
	//
	// GUARDED_BY(mu)
	spans []*span
}

func (ts *traceState) report(spanIndex int, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	s := ts.spans[spanIndex]
	s.finished = true
	s.end = time.Now()
	s.err = err
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

func round(x float64) float64 {
	if x < 0 {
		return math.Ceil(x - 0.5)
	}

	return math.Floor(x + 0.5)
}

// Log information about the spans in this trace.
func (ts *traceState) Log() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	log.Println("TRACE: ==============================================")

	// Special case: we require at least one span.
	if len(ts.spans) == 0 {
		log.Println("(No spans)")
		return
	}

	// Find the minimum start time and maximum end time of all durations.
	var minStart time.Time
	var maxEnd time.Time
	for _, s := range ts.spans {
		if !s.finished {
			continue
		}

		if minStart.IsZero() || s.start.Before(minStart) {
			minStart = s.start
		}

		if maxEnd.Before(s.end) {
			maxEnd = s.end
		}
	}

	// Bail out if something weird happened.
	//
	// TODO(jacobsa): Be more graceful.
	totalDuration := maxEnd.Sub(minStart)
	if minStart.IsZero() || maxEnd.IsZero() || totalDuration <= 0 {
		log.Println("(Weird trace)")
		return
	}

	// Calculate the number of nanoseconds elapsed, as a floating point number.
	totalNs := float64(totalDuration / time.Nanosecond)

	// Log each span with some ASCII art showing its length relative to the
	// total.
	for _, s := range ts.spans {
		log.Println(s.desc)

		if !s.finished {
			log.Println("(Unfinished)")
			log.Println()
			continue
		}

		d := s.end.Sub(s.start)
		if d <= 0 {
			log.Println("(Weird duration)")
			log.Println()
			continue
		}

		relWidth := float64(d/time.Nanosecond) / totalNs
		offset := float64(s.start.Sub(minStart)/time.Nanosecond) / totalNs

		const totalNumCols float64 = 120
		banner := strings.Repeat(" ", int(round(offset*totalNumCols)))
		banner += strings.Repeat("-", int(round(relWidth*totalNumCols)))

		log.Println(d)
		log.Println(banner)
		log.Println()
	}
}
