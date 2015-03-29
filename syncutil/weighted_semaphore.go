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

package syncutil

import "sync"

// A weighted counted semaphore. Can be created as part of larger structs. Must
// be initialized with Init.
type WeightedSemaphore struct {
	// Constant after Init returns.
	capacity uint64

	mu InvariantMutex

	// The remaining capacity.
	//
	// INVARIANT: remaining <= capacity
	//
	// GUARDED_BY(mu)
	remaining uint64

	// Signalled when remaining is changed.
	remainingChanged sync.Cond
}

// Initialize the semaphore with the given capacity.
func (ws *WeightedSemaphore) Init(capacity uint64)

// Return the capacity with which the semaphore was initialized.
func (ws *WeightedSemaphore) Capacity() uint64

// Block until the supplied capacity is available.
//
// REQUIRES: c <= ws.Capacity()
func (ws *WeightedSemaphore) Acquire(c uint64)

// Release previously-acquired capacity.
func (ws *WeightedSemaphore) Release(c uint64)
