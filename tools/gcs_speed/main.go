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

// A program that makes random reads within a large object on GCS, reporting
// throughput and latency information.
package gcs_speed

import "flag"

var fBucket = flag.String("bucket", "", "The GCS bucket from which to read.")
var fObject = flag.String("object", "", "The object within which to read.")
var fReadSize = flag.Int("size", 1<<21, "The size of each read in bytes.")
var fWorkers = flag.Int("workers", 1, "The number of workers to run.")
