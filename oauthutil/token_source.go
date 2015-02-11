// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

// Utility code for working with OAuth. Subject to interface change.
package oauthutil

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"

	"golang.org/x/oauth2"
)

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
	config *oauth2.Config,
	authCodeFlag *flag.Flag,
	cacheFilename string) (oauth2.TokenSource, error) {
	// Catch the common mistake of an unknown flag.
	if authCodeFlag == nil {
		return nil, errors.New("NewTerribleTokenSource passed a nil flag.")
	}

	// Expand the path to the token cache.
	homedir, err := getHomeDir()
	if err != nil {
		return nil, fmt.Errorf("Finding homedir: %v", err)
	}

	cachePath := path.Join(homedir, cacheFilename)

	// Create the underlying token source.
	var ts oauth2.TokenSource = &tokenSource{
		oauth2Config: config,
		authCodeFlag: authCodeFlag,
		cachePath:    cachePath,
	}

	// Make sure not to consult the cache when a valid token is already lying
	// around in memory.
	ts = oauth2.ReuseTokenSource(nil, ts)

	return ts, nil
}

func getHomeDir() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("user.Current: %v", err)
	}

	return usr.HomeDir, nil
}

type tokenSource struct {
	oauth2Config *oauth2.Config
	authCodeFlag *flag.Flag
	cachePath    string
}

func (ts *tokenSource) getAuthCode() string {
	s := ts.authCodeFlag.Value.String()
	if s == "" {
		// NOTE(jacobsa): As of 2015-02-05 the documentation for
		// oauth2.Config.AuthCodeURL says that it is required to set this, but as
		// far as I can tell (cf. RFC 6749 §10.12) it is irrelevant for an
		// installed application that doesn't have a meaningful redirect URL.
		const csrfToken = ""

		fmt.Printf("You must set -%s.\n", ts.authCodeFlag.Name)
		fmt.Println("Visit this URL to obtain a code:")
		fmt.Println("    ", ts.oauth2Config.AuthCodeURL(csrfToken, oauth2.AccessTypeOffline))
		os.Exit(1)
	}

	return s
}

func (ts *tokenSource) getToken() (*oauth2.Token, error) {
	// First consult the cache.
	t, err := ts.LookUp()
	if err != nil {
		// Log the error and ignore it.
		log.Println("Error loading from token cache: ", err)
	}

	// If the cached token is valid, we can return it.
	if t != nil && t.Valid() {
		return t, nil
	}

	// Otherwise if there was a cached token, it is probably expired and we can
	// attempt to refresh it.
	if t != nil {
		if t, err = ts.oauth2Config.TokenSource(oauth2.NoContext, t).Token(); err == nil {
			return t, nil
		}

		log.Println("Error refreshing token:", err)
	}

	// We must fall back to exchanging an auth code.
	log.Println("Exchanging auth code for token.")
	return ts.oauth2Config.Exchange(oauth2.NoContext, ts.getAuthCode())
}

func (ts *tokenSource) Token() (*oauth2.Token, error) {
	// Obtain the token.
	t, err := ts.getToken()
	if err != nil {
		return nil, err
	}

	// Insert into cache, then return the token.
	err = ts.Insert(t)
	if err != nil {
		log.Println("Error inserting into token cache:", err)
	}

	return t, nil
}

// Look for a token in the cache. Returns nil, nil on miss.
func (ts *tokenSource) LookUp() (*oauth2.Token, error) {
	// Open the cache file.
	file, err := os.Open(ts.cachePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Decode the token.
	t := &oauth2.Token{}
	if err := json.NewDecoder(file).Decode(t); err != nil {
		return nil, err
	}

	return t, nil
}

func (ts *tokenSource) Insert(t *oauth2.Token) error {
	const flags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	const perm = 0600

	// Open the cache file.
	file, err := os.OpenFile(ts.cachePath, flags, perm)
	if err != nil {
		return err
	}

	// Encode the token.
	if err := json.NewEncoder(file).Encode(t); err != nil {
		file.Close()
		return err
	}

	// Close the file.
	if err := file.Close(); err != nil {
		return err
	}

	return nil
}
