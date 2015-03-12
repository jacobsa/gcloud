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

package gcs

import "errors"

// A sentinel error. See notes on the methods of Bucket.
var ErrNotFound = errors.New("not found")

// A *PreconditionError value is an error that indicates a precondition failed.
// See notes on the methods of Bucket.
type PreconditionError struct {
	// A wrapped error. PreconditionError.Error simply returns Err.Error().
	Err error
}

func (pe *PreconditionError) Error() string {
	return pe.Err.Error()
}
