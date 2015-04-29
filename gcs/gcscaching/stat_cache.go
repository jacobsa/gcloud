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

package gcscaching

import (
	"time"

	"github.com/jacobsa/gcloud/gcs"
)

// A cache mapping from name to most recent known record for the object of that
// name. External synchronization must be provided.
type StatCache interface {
	// Insert an entry for the given object record. The entry will not replace
	// any entry with a newer generation number, or any entry with an equivalent
	// generation number but newer metadata generation number, and will not be
	// available after the supplied expiration time.
	Insert(o *gcs.Object, expiration time.Time)

	// Erase the entry for the given object name, if any.
	Erase(name string)

	// Return the current entry for the given name, or nil if none. Use the
	// supplied time to decide whether entries have expired.
	LookUp(name string, now time.Time) (o *gcs.Object)
}

// Create a new stat cache that holds the given number of entries, which must
// be positive.
func NewStatCache(capacity int) (sc StatCache) {
	panic("TODO")
}
