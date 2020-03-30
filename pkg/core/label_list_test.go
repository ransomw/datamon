package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	context2 "github.com/oneconcern/datamon/pkg/context"

	"github.com/oneconcern/datamon/pkg/model"
	"github.com/oneconcern/datamon/pkg/storage"
	"github.com/oneconcern/datamon/pkg/storage/mockstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type labelFixture struct {
	name          string
	repo          string
	prefix        string
	wantError     bool
	expected      model.LabelDescriptors
	errorContains []string
}

var (
	initLabelBatchFixture     sync.Once
	labelBatchFixture         []string
	expectedLabelBatchFixture model.LabelDescriptors
	baseTime                  time.Time
)

func labelTestCases() []labelFixture {
	return []labelFixture{
		{
			name:     happyPath,
			repo:     "myRepo",
			prefix:   "myLab",
			expected: model.LabelDescriptors{fakeLD("myLabel-test")},
		},
		{
			name:     happyWithBatches,
			repo:     "myRepo",
			prefix:   "myLab",
			expected: expectedLabelBatchFixture,
		},
	}
}

func init() {
	baseTime = time.Now().Truncate(time.Hour).UTC() // avoid loss of time resolution through yaml marshalling
}

func testTime() time.Time {
	return baseTime
}

func buildLabelBatchFixture(t *testing.T) func() {
	return func() {
		labelBatchFixture = make([]string, maxTestKeys)
		expectedLabelBatchFixture = make(model.LabelDescriptors, maxTestKeys)
		for i := 0; i < maxTestKeys; i++ {
			labelBatchFixture[i] = fakeLabelPath("myRepo", fmt.Sprintf("myLabel-%0.3d", i))
			expectedLabelBatchFixture[i] = fakeLD(fmt.Sprintf("myLabel-%0.3d", i))
		}
		require.Truef(t, sort.IsSorted(expectedBatchFixture), "got %v", expectedBatchFixture)
	}
}

func mockedLabelStore(testcase string) storage.Store {
	switch testcase {
	case happyPath:
		return &mockstorage.StoreMock{
			HasFunc: goodHasFunc,
			KeysPrefixFunc: func(_ context.Context, _ string, _ string, _ string, _ int) ([]string, string, error) {
				return []string{fakeLabelPath("myRepo", "myLabel-test")}, "", nil
			},
			KeysFunc: goodKeysFunc,
			GetFunc:  goodGetLabelFunc,
		}
	case happyWithBatches:
		return &mockstorage.StoreMock{
			HasFunc: goodHasFunc,
			KeysPrefixFunc: func(_ context.Context, _ string, _ string, _ string, _ int) ([]string, string, error) {
				return labelBatchFixture, "", nil
			},
			KeysFunc: goodKeysFunc,
			GetFunc:  goodGetLabelFunc,
		}
	default:
		return nil
	}
}

func mockedLabelContextStores(scenario string) context2.Stores {
	mockStore := mockedLabelStore(scenario)
	return context2.NewStores(nil, nil, nil, mockStore, mockStore)
}

func testListLabels(t *testing.T, concurrency int, i int) {
	initLabelBatchFixture.Do(buildLabelBatchFixture(t))
	defer goleak.VerifyNone(t,
		// opencensus stats collection goroutine
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"))
	for _, toPin := range labelTestCases() {
		testcase := toPin

		t.Run(fmt.Sprintf("ListLabels-%s-%d-%d", testcase.name, concurrency, i), func(t *testing.T) {
			//t.Parallel() // too much resource w/ -race on CI
			labels, err := ListLabels(testcase.repo, mockedLabelContextStores(testcase.name),
				testcase.prefix, ConcurrentList(concurrency), BatchSize(testBatchSize))
			assertLabels(t, testcase, labels, err)
		})
		t.Run(fmt.Sprintf("ListLabelsApply-%s-%d-%d", testcase.name, concurrency, i), func(t *testing.T) {
			//t.Parallel()
			labels := make(model.LabelDescriptors, 0, typicalReposNum)
			err := ListLabelsApply(testcase.repo, mockedLabelContextStores(testcase.name),
				ApplyLabelFunc{ToLabel: func(label model.LabelDescriptor) error {
					labels = append(labels, label)
					return nil
				}}, WithPrefix(testcase.prefix), ConcurrentList(concurrency), BatchSize(testBatchSize))
			assertLabels(t, testcase, labels, err)
		})
	}
}

func assertLabels(t *testing.T, testcase labelFixture, labels model.LabelDescriptors, err error) {
	if testcase.wantError {
		require.Error(t, err)
		for _, expectedMsg := range testcase.errorContains { // assert error message (opt-in)
			assert.Contains(t, err.Error(), expectedMsg)
		}

		assert.Len(t, labels, len(testcase.expected)) // assert result, possibly partial
		return
	}
	require.NoError(t, err)
	if !assert.ElementsMatch(t, testcase.expected, labels, "expected returned labels to match expected descriptors") {
		// output the details upon failure
		exp, _ := json.MarshalIndent(testcase.expected, "", " ")
		act, _ := json.MarshalIndent(labels, "", " ")
		assert.JSONEqf(t, string(exp), string(act), "expected equivalent marshalled JSON")
	}
	assert.Truef(t, sort.IsSorted(labels), "expected a sorted output, got: %v", labels)
}

func TestListLabels(t *testing.T) {
	for i := 0; i < 10; i++ { // check results remain stable over 10 independent iterations
		for _, concurrency := range []int{0, 1, 50, 100, 400} { // test several concurrency parameters
			t.Logf("simulating ListLabels with concurrency-factor=%d, iteration=%d", concurrency, i)
			testListLabels(t, concurrency, i)
		}
	}
}

///

type labelVersionedFixture struct {
	name     string
	repo     string
	expected []storage.Version
}

func labelVersionedTestCases() []labelVersionedFixture {
	return []labelVersionedFixture{
		{
			name:     happyPath,
			repo:     "myRepo",
			expected: []storage.Version{fakeLV(1), fakeLV(2)},
		},
	}
}

func mockedLabelVersionsStore(testcase string) storage.Store {
	switch testcase {
	case happyPath:
		// to mock a type that implements multiple interfaces such as the gcs store,
		// mock each interface seperately and rely on promoted methods of embedded fields
		// of an anonymous struct.
		storeMock := &mockstorage.StoreMock{
			HasFunc: goodHasFunc,
			KeysPrefixFunc: func(_ context.Context, _ string, _ string, _ string, _ int) ([]string, string, error) {
				return []string{fakeLabelPath("myRepo", "myLabel")}, "", nil
			},
			KeysFunc: goodKeysFunc,
			GetFunc:  goodGetLabelFunc,
		}
		storeVersionedMock := &mockstorage.StoreVersionedMock{
			KeyVersionsFunc: func(_ context.Context, _ string) ([]storage.Version, error) {
				return []storage.Version{
					storage.NewVersionGcs(1),
					storage.NewVersionGcs(2),
				}, nil
			},
		}
		return struct {
			*mockstorage.StoreMock
			*mockstorage.StoreVersionedMock
		}{
			StoreMock:          storeMock,
			StoreVersionedMock: storeVersionedMock,
		}
	default:
		return nil
	}
}

func mockedLabelVersionsContextStores(scenario string) context2.Stores {
	mockStore := mockedLabelVersionsStore(scenario)
	return context2.NewStores(nil, nil, nil, mockStore, mockStore)
}

func testListLabelVersions(t *testing.T, concurrency int, i int) {
	defer goleak.VerifyNone(t,
		// opencensus stats collection goroutine
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"))
	for _, testcase := range labelVersionedTestCases() {

		t.Run(fmt.Sprintf("ListLabelsApply-%s-%d-%d", testcase.name, concurrency, i), func(t *testing.T) {
			//t.Parallel()
			versions := make([]storage.Version, 0, typicalReposNum)
			err := ListLabelsApply(testcase.repo, mockedLabelVersionsContextStores(testcase.name),
				ApplyLabelFunc{ToVersion: func(version string) error {
					// the core package currently uses string typing to express versions, while
					// this test demonstrates how to back a storage.Version type out of the strings.
					gcsGeneration, err := strconv.ParseInt(version, 10, 64)
					require.NoError(t, err, "parse version string")
					versions = append(versions,
						storage.NewVersionGcs(gcsGeneration))
					return nil
				}}, WithLabel("myLabel"), ConcurrentList(concurrency), BatchSize(testBatchSize))
			assertVersions(t, testcase, versions, err)
		})

	}
}

func assertVersions(t *testing.T, testcase labelVersionedFixture, versions []storage.Version, err error) {
	require.NoError(t, err)
	require.ElementsMatch(t,
		testcase.expected, versions,
		"expected returned versions to match expected descriptors")
}

func TestListLabelVersions(t *testing.T) {
	for i := 0; i < 10; i++ {
		for _, concurrency := range []int{0, 1, 50, 100, 400} {
			t.Logf("simulating ListLabels with concurrency-factor=%d, iteration=%d", concurrency, i)
			testListLabelVersions(t, concurrency, i)
		}
	}
}
