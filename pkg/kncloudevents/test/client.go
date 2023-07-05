/*
Copyright 2023 The Knative Authors

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

package test

import (
	"context"
	"fmt"
	nethttp "net/http"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/metrics/source"
)

var _ kncloudevents.Client = (*FakeClient)(nil)

func NewFakeClient() *FakeClient {
	return &FakeClient{
		sentEvents: make([]event.Event, 0),
	}
}

func NewFakeClientWithDelay(delay time.Duration) *FakeClient {
	client := NewFakeClient()
	client.delay = delay

	return client
}

type FakeClient struct {
	delay          time.Duration
	sentEvents     []event.Event
	requestOptions []kncloudevents.RequestOption
}

// SentEvents returns all events sent within all requests of this client.
func (c *FakeClient) SentEvents() []event.Event {
	return c.sentEvents
}

func (c *FakeClient) Send(ctx context.Context, request *kncloudevents.Request) (*nethttp.Response, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}

	if err := c.applyRequestOptions(request); err != nil {
		return nil, err
	}

	// get event from request to add to sentEvents
	message := http.NewMessageFromHttpRequest(request.HTTPRequest())
	defer message.Finish(nil)

	event, err := binding.ToEvent(context.TODO(), message)
	if err != nil {
		return nil, err
	}

	c.sentEvents = append(c.sentEvents, *event)

	return &nethttp.Response{
		StatusCode: nethttp.StatusOK,
	}, nil
}

func (c *FakeClient) SendWithRetries(ctx context.Context, request *kncloudevents.Request, config *kncloudevents.RetryConfig) (*nethttp.Response, error) {
	return c.Send(ctx, request)
}

func (c *FakeClient) SetTimeout(time time.Duration) {
	// in the test client we don't care about client timeouts
}

func (c *FakeClient) SetStatsReporter(r source.StatsReporter) {
	// in the test client we don't care about the reporter yet
}

func (c *FakeClient) AddRequestOptions(opts ...kncloudevents.RequestOption) {
	c.requestOptions = append(c.requestOptions, opts...)
}

func (c *FakeClient) applyRequestOptions(req *kncloudevents.Request) error {
	for _, opt := range c.requestOptions {
		if err := opt(req); err != nil {
			return fmt.Errorf("could not apply request option: %w", err)
		}
	}

	return nil
}
