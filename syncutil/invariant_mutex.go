// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package syncutil

import (
	"flag"
	"sync"
)

var fCheckInvariants = flag.Bool("syncutil.check_invariants", false, "Crash when registered invariants are violated.")

// A reader/writer mutex like sync.RWMutex that additionally runs a check for
// registered invariants at times when invariants should hold, when enabled.
//
// Must be created with NewInvariantMutex. See that function for more details.
type InvariantMutex struct {
	mu    sync.RWMutex
	check func()
}

func (i *InvariantMutex) Lock() {
	i.mu.Lock()
	i.checkIfEnabled()
}

func (i *InvariantMutex) Unlock() {
	i.checkIfEnabled()
	i.mu.Unlock()
}

func (i *InvariantMutex) RLock() {
	i.mu.RLock()
	i.checkIfEnabled()
}

func (i *InvariantMutex) RUnlock() {
	i.checkIfEnabled()
	i.mu.RUnlock()
}

func (i *InvariantMutex) checkIfEnabled() {
	if *fCheckInvariants {
		i.check()
	}
}

// Create a reader/writer mutex which, when the flag -syncutil.check_invariants
// is set, will call the supplied function at moments when invariants protected
// by the mutex should hold (e.g. just after acquiring the lock). The function
// should crash if an invariant is violated. It should not have side effects,
// as there are no guarantees that it will run.
func NewInvariantMutex(check func()) InvariantMutex {
	if check == nil {
		panic("check must be non-nil.")
	}

	return InvariantMutex{
		check: check,
	}
}
