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

package httputil

import "io"

type ContentTypedReader struct {
	ContentType string
	Reader      io.Reader
}

// Create a reader that streams an HTTP multipart body (see RFC 2388) composed
// of the contents of each component reader in sequence, each with a
// Content-Type header as specified.
func NewMultipartReader(readers []ContentTypedReader) io.Reader
