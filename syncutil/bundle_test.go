// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package syncutil_test

import (
	"testing"

	"github.com/jacobsa/gcloud/syncutil"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
)

func TestOgletest(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type BundleTest struct {
	bundle       *syncutil.Bundle
	cancelParent context.CancelFunc
}

func init() { RegisterTestSuite(&BundleTest{}) }

func (t *BundleTest) SetUp(ti *TestInfo) {
	// Set up the parent context.
	parentCtx, cancelParent := context.WithCancel(context.Background())
	t.cancelParent = cancelParent

	// Set up the bundle.
	t.bundle = syncutil.NewBundle(parentCtx)
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *BundleTest) NoOperations() {
	ExpectEq(nil, t.bundle.Join())
}

func (t *BundleTest) SingleOp_Success() {
	t.bundle.Add(func(c context.Context) error {
		return nil
	})

	ExpectEq(nil, t.bundle.Join())
}

func (t *BundleTest) SingleOp_Error() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) SingleOp_ParentCancelled() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_Success() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_UnorderedErrors() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_OneError_OthersDontWait() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_OneError_OthersWaitForCancellation() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_ParentCancelled() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_PreviousError_NewOpsObserve() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_PreviousParentCancel_NewOpsObserve() {
	AssertFalse(true, "TODO")
}
