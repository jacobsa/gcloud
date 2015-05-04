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

import (
	"flag"
	"log"
)

var fDebug = flag.Bool(
	"gcs.debug",
	false,
	"Write FUSE debugging messages to stderr.")

// If debugging is enabled, wrap the supplied bucket in a layer that prints
// debug messages.
func newDebugBucket(wrapped Bucket) (b Bucket) {
	// TODO
	b = wrapped
	return
}

type debugBucket struct {
	logger  log.Logger
	wrapped Bucket
}
