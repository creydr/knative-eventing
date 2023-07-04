/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mtping

import (
	"bytes"
	"context"
	"encoding/base64"
	nethttp "net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	_ "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/logging"
	rectesting "knative.dev/pkg/reconciler/testing"

	sourcesv1 "knative.dev/eventing/pkg/apis/sources/v1"
	"knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/kncloudevents/test"
)

const (
	threeSecondsTillNextMinCronJob = 60 - 3
	sampleData                     = "some data"
	sampleJSONData                 = `{"msg":"some data"}`
	sampleXmlData                  = "<pre>Value</pre>"
	sampleDataBase64               = "c29tZSBkYXRh"                 // "some data"
	sampleJSONDataBase64           = "eyJtc2ciOiJzb21lIGRhdGEifQ==" // {"msg":"some data"}
)

func decodeBase64(base64Str string) []byte {
	decoded, _ := base64.StdEncoding.DecodeString(base64Str)
	return decoded
}

func TestAddRunRemoveSchedules(t *testing.T) {
	testCases := map[string]struct {
		src             *sourcesv1.PingSource
		wantContentType string
		wantData        []byte
	}{
		"TestAddRunRemoveSchedule": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * ?",
					ContentType: cloudevents.TextPlain,
					Data:        sampleData,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a sink"},
					},
				},
			},
			wantData:        []byte(sampleData),
			wantContentType: cloudevents.TextPlain,
		}, "TestAddRunRemoveScheduleWithExtensionOverride": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{
							Extensions: map[string]string{"1": "one", "2": "two"},
						},
					},
					Schedule:    "* * * * ?",
					ContentType: cloudevents.TextPlain,
					Data:        sampleData,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a sink"},
					},
				},
			},
			wantData:        []byte(sampleData),
			wantContentType: cloudevents.TextPlain,
		}, "TestAddRunRemoveScheduleWithDataBase64": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * ?",
					ContentType: cloudevents.TextPlain,
					DataBase64:  sampleDataBase64,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a sink"},
					},
				},
			},
			wantData:        decodeBase64(sampleDataBase64),
			wantContentType: cloudevents.TextPlain,
		}, "TestAddRunRemoveScheduleWithJsonData": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * ?",
					Data:        sampleJSONData,
					ContentType: cloudevents.ApplicationJSON,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a sink"},
					},
				},
			},
			wantData:        []byte(sampleJSONData),
			wantContentType: cloudevents.ApplicationJSON,
		}, "TestAddRunRemoveScheduleWithJsonDataBase64": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * ?",
					DataBase64:  sampleJSONDataBase64,
					ContentType: cloudevents.ApplicationJSON,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a sink"},
					},
				},
			},
			wantData:        decodeBase64(sampleJSONDataBase64),
			wantContentType: cloudevents.ApplicationJSON,
		}, "TestAddRunRemoveScheduleWithXmlData": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * ?",
					Data:        sampleXmlData,
					ContentType: cloudevents.ApplicationXML,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a sink"},
					},
				},
			},
			wantData:        []byte(sampleXmlData),
			wantContentType: cloudevents.ApplicationXML,
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx, _ := rectesting.SetupFakeContext(t)
			logger := logging.FromContext(ctx)
			ceClient := test.NewFakeClient()

			runner := NewCronJobsRunner(ceClient, kubeclient.Get(ctx), logger)
			entryId := runner.AddSchedule(tc.src)

			entry := runner.cron.Entry(entryId)
			if entry.ID != entryId {
				t.Error("Entry has not been added")
			}

			entry.Job.Run()

			validateSent(t, ceClient, tc.wantData, tc.wantContentType, tc.src.Spec.CloudEventOverrides.Extensions)

			runner.RemoveSchedule(entryId)

			entry = runner.cron.Entry(entryId)
			if entry.ID == entryId {
				t.Error("Entry has not been removed")
			}
		})
	}
}

func TestSendEventsTLS(t *testing.T) {

	ctx, _ := rectesting.SetupFakeContext(t)
	requestsChan := make(chan *nethttp.Request, 10)
	handler := eventingtlstesting.RequestsChannelHandler(requestsChan)
	events := make([]*cloudevents.Event, 0, 8)
	ca := eventingtlstesting.StartServer(ctx, t, 8334, handler)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range requestsChan {
			func() {
				message := cehttp.NewMessageFromHttpRequest(r)
				defer message.Finish(nil)

				event, err := binding.ToEvent(ctx, message)
				require.Nil(t, err)

				events = append(events, event)
			}()
		}
	}()

	testCases := map[string]struct {
		src             *sourcesv1.PingSource
		wantContentType string
		wantData        []byte
	}{
		"Valid CA certs": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name1",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * *",
					ContentType: cloudevents.TextPlain,
					Data:        sampleData,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI:     &apis.URL{Scheme: "https", Host: "localhost:8334"},
						SinkCACerts: pointer.String(ca),
					},
				},
			},
			wantData:        []byte(sampleData),
			wantContentType: cloudevents.TextPlain,
		},
		"No CA certs": {
			src: &sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name2",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * *",
					ContentType: cloudevents.TextPlain,
					Data:        sampleData,
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Scheme: "https", Host: "localhost:8334"},
					},
				},
			},
			wantData:        []byte(sampleData),
			wantContentType: cloudevents.TextPlain,
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			logger := logging.FromContext(ctx)
			ceClient := kncloudevents.NewClient()

			runner := NewCronJobsRunner(ceClient, kubeclient.Get(ctx), logger)
			entryId := runner.AddSchedule(tc.src)

			entry := runner.cron.Entry(entryId)
			if entry.ID != entryId {
				t.Error("Entry has not been added")
			}

			entry.Job.Run()

			// as we cache the certs for an addressable, make sure to delete the addressable from the cache after each run
			kncloudevents.DeleteAddressableHandler(duckv1.Addressable{
				URL: tc.src.Status.SinkURI,
			})
		})
	}

	close(requestsChan)
	wg.Wait()

	require.Len(t, events, 1)

	event := events[0]

	require.Equal(t, sourcesv1.PingSourceEventType, event.Type())
	require.Equal(t, sourcesv1.PingSourceSource("test-ns", "test-name1"), event.Source())
}

func TestStartStopCron(t *testing.T) {
	ctx, _ := rectesting.SetupFakeContext(t)
	logger := logging.FromContext(ctx)
	ceClient := test.NewFakeClient()

	runner := NewCronJobsRunner(ceClient, kubeclient.Get(ctx), logger)

	ctx, cancel := context.WithCancel(context.Background())
	wctx, wcancel := context.WithCancel(context.Background())

	go func() {
		runner.Start(ctx.Done())
		wcancel()
	}()

	cancel()

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("expected cron to be stopped after 2 seconds")
	case <-wctx.Done():
	}
}

func TestStartStopCronDelayWait(t *testing.T) {
	tn := time.Now()
	seconds := tn.Second()
	if seconds > threeSecondsTillNextMinCronJob {
		time.Sleep(time.Second * 4) // ward off edge cases
	}
	ctx, _ := rectesting.SetupFakeContext(t)
	logger := logging.FromContext(ctx)
	ceClient := test.NewFakeClientWithDelay(time.Second * 5)

	runner := NewCronJobsRunner(ceClient, kubeclient.Get(ctx), logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		runner.AddSchedule(
			&sourcesv1.PingSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-name",
					Namespace: "test-ns",
				},
				Spec: sourcesv1.PingSourceSpec{
					SourceSpec: duckv1.SourceSpec{
						CloudEventOverrides: &duckv1.CloudEventOverrides{},
					},
					Schedule:    "* * * * *",
					ContentType: cloudevents.TextPlain,
					Data:        "some delayed data",
				},
				Status: sourcesv1.PingSourceStatus{
					SourceStatus: duckv1.SourceStatus{
						SinkURI: &apis.URL{Path: "a delayed sink"},
					},
				},
			})
		runner.Start(ctx.Done())
	}()

	tn = time.Now()
	seconds = tn.Second()

	time.Sleep(time.Second * (61 - time.Duration(seconds))) // one second past the minute

	runner.Stop() // cron job because of delay is still running.

	validateSent(t, ceClient, []byte("some delayed data"), cloudevents.TextPlain, nil)
}

func validateSent(t *testing.T, ce *test.FakeClient, wantData []byte, wantContentType string, extensions map[string]string) {
	if got := len(ce.SentEvents()); got != 1 {
		t.Error("Expected 1 event to be sent, got", got)
		return
	}

	event := ce.SentEvents()[0]

	if gotContentType := event.DataContentType(); gotContentType != wantContentType {
		t.Errorf("Expected event with contentType=%q to be sent, got %q", wantContentType, gotContentType)
	}

	if got := event.Data(); !bytes.Equal(wantData, got) {
		t.Errorf("Expected %q event to be sent, got %q", wantData, got)
	}

	gotExtensions := event.Context.GetExtensions()

	if extensions == nil && gotExtensions != nil {
		t.Error("Expected event with no extension overrides, got:", gotExtensions)
	}

	if extensions != nil && gotExtensions == nil {
		t.Error("Expected event with extension overrides but got nil")
	}

	if extensions != nil {
		compareTo := make(map[string]interface{}, len(extensions))
		for k, v := range extensions {
			compareTo[k] = v
		}
		if !reflect.DeepEqual(compareTo, gotExtensions) {
			t.Errorf("Expected event with extension overrides to be the same want: %v, but got: %v", extensions, gotExtensions)
		}
	}
}
