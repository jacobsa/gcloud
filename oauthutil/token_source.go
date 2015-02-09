// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package oauthutil

import (
	"flag"

	"golang.org/x/oauth2"
)

type TokenSourceParams struct {
	ClientID          string
	ClientSecret      string
	ClientRedirectURL string
}

// Create an oauth2.TokenSource that retrieves tokens in the following
// preference order:
//
//  1. From a copy of the latest obtained token, if still valid.
//
//  2. From a cache on disk, when it contains a valid token. The supplied file
//     basename will be placed in the user's home directory.
//
//  3. From an oauth token refresher, seeded with the last valid cached token.
//
//  4. By exchanging a new authorization code obtained via the supplied flag.
//     If the flag has not been set, the program will be aborted with a helpful
//     message.
//
// Note in particular the warning about aborting the program in #4.
func NewTerribleTokenSource(
	params TokenSourceParams,
	authCodeFlag *flag.Flag,
	cacheFilename string) (oauth2.TokenSource, error)
