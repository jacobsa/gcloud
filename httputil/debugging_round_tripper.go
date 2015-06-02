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

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

var fDebug = flag.Bool(
	"httputil.debug",
	false,
	"Dump information about HTTP requests.")

// An interface for transports that support the signature of
// http.Transport.CancelRequest.
type CancellableRoundTripper interface {
	http.RoundTripper
	CancelRequest(*http.Request)
}

// When the flag --httputil.debug is set, wrap the supplied round tripper in a
// layer that dumps information about HTTP requests. Otherwise, return it
// unmodified.
func DebuggingRoundTripper(
	in CancellableRoundTripper) (out CancellableRoundTripper) {
	if *fDebug {
		out = &debuggingRoundTripper{wrapped: in}
	} else {
		out = in
	}

	return
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func readAllAndClose(rc io.ReadCloser) string {
	// Read.
	contents, err := ioutil.ReadAll(rc)
	if err != nil {
		panic(err)
	}

	// Close.
	if err := rc.Close(); err != nil {
		panic(err)
	}

	return string(contents)
}

// Read everything from *rc, then replace it with a copy.
func snarfBody(rc *io.ReadCloser) string {
	contents := readAllAndClose(*rc)
	*rc = ioutil.NopCloser(bytes.NewBufferString(contents))
	return contents
}

////////////////////////////////////////////////////////////////////////
// debuggingRoundTripper
////////////////////////////////////////////////////////////////////////

type debuggingRoundTripper struct {
	wrapped CancellableRoundTripper
}

func (t *debuggingRoundTripper) RoundTrip(
	req *http.Request) (*http.Response, error) {
	// Print information about the request.
	fmt.Println("========== REQUEST ===========")
	fmt.Println(req.Method, req.URL, req.Proto)
	for k, vs := range req.Header {
		for _, v := range vs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	if req.Body != nil {
		fmt.Printf("\n%s\n", snarfBody(&req.Body))
	}

	// Execute the request.
	res, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return res, err
	}

	// Print the response.
	fmt.Println("========== RESPONSE ==========")
	fmt.Println(res.Proto, res.Status)
	for k, vs := range res.Header {
		for _, v := range vs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	if res.Body != nil {
		fmt.Printf("\n%s\n", snarfBody(&res.Body))
	}
	fmt.Println("==============================")

	return res, err
}

func (t *debuggingRoundTripper) CancelRequest(req *http.Request) {
	t.wrapped.CancelRequest(req)
}
