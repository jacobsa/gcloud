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
	"io"
	"net/http"
	"net/url"
)

// Create an HTTP request with the supplied information. This is the same as
// calling http.NewRequest, except that it makes it more difficult to forget to
// include a user agent.
func NewRequest(
	method string,
	url *url.URL,
	body io.Reader,
	userAgent string) (req *http.Request, err error) {
	// Create the request.
	req, err = http.NewRequest(method, url.String(), body)
	if err != nil {
		return
	}

	// Set the User-Agent header.
	req.Header.Set("User-Agent", userAgent)

	return
}
