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

// A tool to measure the upload throughput of GCS.
package main

import (
	"errors"
	"flag"
	"log"
)

var fBucket = flag.String("bucket", "", "Name of bucket.")
var fKeyFile = flag.String("key_file", "", "Path to JSON key file.")
var fSize = flag.Int("size", 1<<26, "Size of content to write.")

func run() (err error) {
	err = errors.New("TODO")
	return
}

func main() {
	err := run()
	if err != nil {
		log.Fatalln(err)
	}
}
