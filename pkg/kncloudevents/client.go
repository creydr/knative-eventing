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

package kncloudevents

import (
	"context"
	"fmt"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/hashicorp/go-retryablehttp"
	"knative.dev/eventing/pkg/metrics/source"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

const RetryAfterHeader = "Retry-After"

type Client interface {
	Send(context.Context, *Request) (*nethttp.Response, error)
	SendWithRetries(context.Context, *Request, *RetryConfig) (*nethttp.Response, error)
	SetTimeout(time.Duration)
	SetStatsReporter(source.StatsReporter)
	AddRequestOptions(...RequestOption)
}

var _ Client = (*clientImpl)(nil)

func NewClient() Client {
	c := newClientImpl()
	return &c
}

type clientImpl struct {
	timeout        time.Duration
	statsReporter  source.StatsReporter
	requestOptions []RequestOption
}

func newClientImpl() clientImpl {
	return clientImpl{
		requestOptions: make([]RequestOption, 0),
	}
}

func (c *clientImpl) Send(ctx context.Context, req *Request) (*nethttp.Response, error) {
	if err := c.applyRequestOptions(ctx, req); err != nil {
		return nil, fmt.Errorf("could not apply request options: %w", err)
	}

	client, err := c.getConfiguredHttpClient(req.target)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for addressable: %w", err)
	}

	resp, err := client.Do(req.Request)
	c.reportMetrics(ctx, resp, err)

	return resp, err
}

func (c *clientImpl) SendWithRetries(ctx context.Context, req *Request, config *RetryConfig) (*nethttp.Response, error) {
	if config == nil {
		return c.Send(ctx, req)
	}

	if err := c.applyRequestOptions(ctx, req); err != nil {
		return nil, fmt.Errorf("could not apply request options: %w", err)
	}

	client, err := c.getConfiguredHttpClient(req.target)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for addressable: %w", err)
	}

	if config.RequestTimeout != 0 {
		client = &nethttp.Client{
			Transport:     client.Transport,
			CheckRedirect: client.CheckRedirect,
			Jar:           client.Jar,
			Timeout:       config.RequestTimeout,
		}
	}

	retryableClient := retryablehttp.Client{
		HTTPClient:   client,
		RetryWaitMin: defaultRetryWaitMin,
		RetryWaitMax: defaultRetryWaitMax,
		RetryMax:     config.RetryMax,
		CheckRetry:   retryablehttp.CheckRetry(config.CheckRetry),
		Backoff:      generateBackoffFn(config),
		ErrorHandler: func(resp *nethttp.Response, err error, numTries int) (*nethttp.Response, error) {
			return resp, err
		},
	}

	retryableReq, err := retryablehttp.FromRequest(req.Request)
	if err != nil {
		return nil, fmt.Errorf("could not create retryable request: %w", err)
	}

	resp, err := retryableClient.Do(retryableReq)
	c.reportMetrics(ctx, resp, err)

	return resp, err
}

func (c *clientImpl) SetTimeout(t time.Duration) {
	c.timeout = t
}

func (c *clientImpl) SetStatsReporter(reporter source.StatsReporter) {
	c.statsReporter = reporter
}

func (c *clientImpl) AddRequestOptions(opts ...RequestOption) {
	if len(opts) > 0 {
		c.requestOptions = append(c.requestOptions, opts...)
	}
}

func (c *clientImpl) applyRequestOptions(ctx context.Context, req *Request) error {
	for _, opt := range c.requestOptions {
		if err := opt(ctx, req); err != nil {
			return fmt.Errorf("could not apply request option: %w", err)
		}
	}

	return nil
}

func (c *clientImpl) getConfiguredHttpClient(target duckv1.Addressable) (*nethttp.Client, error) {
	client, err := getClientForAddressable(target)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for addressable: %w", err)
	}

	// apply changes on client copy
	clientCopy := *client
	if c.timeout > 0 {
		clientCopy.Timeout = c.timeout
	}

	return &clientCopy, nil
}

func (c *clientImpl) reportMetrics(ctx context.Context, response *nethttp.Response, err error) {
	if c.statsReporter == nil {
		return
	}

	if err != nil {
		// TODO report metrics for error
		return
	}

	event, err := http.NewEventFromHTTPRequest(response.Request)
	if err != nil {
		return
	}

	tags := MetricTagFrom(ctx)
	if tags == nil {
		return
	}
	reportArgs := &source.ReportArgs{
		Namespace:     tags.Namespace,
		EventSource:   event.Source(),
		EventType:     event.Type(),
		Name:          tags.Name,
		ResourceGroup: tags.ResourceGroup,
	}

	c.statsReporter.ReportEventCount(reportArgs, response.StatusCode)

	// TODO check if we can get the number of retries from SendWithRetries
	// client and report these metrics via c.statsReporter.ReportRetryEventCount
}

// MetricTag context
type MetricTag struct {
	Name          string
	Namespace     string
	ResourceGroup string
}

type metricKey struct{}

// WithMetricTag returns a copy of parent context in which the
// value associated with metric key is the supplied metric tag.
func WithMetricTag(ctx context.Context, metric MetricTag) context.Context {
	return context.WithValue(ctx, metricKey{}, metric)
}

// MetricTagFrom returns the metric tag stored in context.
// Returns nil if no metric tag is set in context
func MetricTagFrom(ctx context.Context) *MetricTag {
	val := ctx.Value(metricKey{})
	if val == nil {
		return nil
	}
	mt := val.(MetricTag)
	return &mt
}

// generateBackoffFunction returns a valid retryablehttp.Backoff implementation which
// wraps the provided RetryConfig.Backoff implementation with optional "Retry-After"
// header support.
func generateBackoffFn(config *RetryConfig) retryablehttp.Backoff {
	return func(_, _ time.Duration, attemptNum int, resp *nethttp.Response) time.Duration {

		//
		// NOTE - The following logic will need to be altered slightly once the "delivery-retryafter"
		//        experimental-feature graduates from Alpha/Beta to Stable/GA.  This is according to
		//        plan as described in https://github.com/knative/eventing/issues/5811.
		//
		//        During the Alpha/Beta stages the ability to respect Retry-After headers is "opt-in"
		//        requiring the DeliverySpec.RetryAfterMax to be populated.  The Stable/GA behavior
		//        will be "opt-out" where Retry-After headers are always respected (in the context of
		//        calculating backoff durations for 429 / 503 responses) unless the
		//        DeliverySpec.RetryAfterMax is set to "PT0S".
		//
		//        While this might seem unnecessarily complex, it achieves the following design goals...
		//          - Does not require an explicit "enabled" flag in the DeliverySpec.
		//          - Does not require implementations calling the message_sender to be aware of experimental-features.
		//          - Does not modify existing Knative CRs with arbitrary default "max" values.
		//
		//        The intended behavior of RetryConfig.RetryAfterMaxDuration is as follows...
		//
		//          RetryAfterMaxDuration    Alpha/Beta                              Stable/GA
		//          ---------------------    ----------                              ---------
		//               nil                 Do NOT respect Retry-After headers      Respect Retry-After headers without Max
		//                0                  Do NOT respect Retry-After headers      Do NOT respect Retry-After headers
		//               >0                  Respect Retry-After headers with Max    Respect Retry-After headers with Max
		//

		// If Response is 429 / 503, Then Parse Any Retry-After Header Durations & Enforce Optional MaxDuration
		var retryAfterDuration time.Duration
		// TODO - Remove this check when experimental-feature moves to Stable/GA to convert behavior from opt-in to opt-out
		if config.RetryAfterMaxDuration != nil {
			// TODO - Keep this logic as is (no change required) when experimental-feature is Stable/GA
			if resp != nil && (resp.StatusCode == nethttp.StatusTooManyRequests || resp.StatusCode == nethttp.StatusServiceUnavailable) {
				retryAfterDuration = parseRetryAfterDuration(resp)
				if config.RetryAfterMaxDuration != nil && *config.RetryAfterMaxDuration < retryAfterDuration {
					retryAfterDuration = *config.RetryAfterMaxDuration
				}
			}
		}

		// Calculate The RetryConfig Backoff Duration
		backoffDuration := config.Backoff(attemptNum, resp)

		// Return The Larger Of The Two Backoff Durations
		if retryAfterDuration > backoffDuration {
			return retryAfterDuration
		}
		return backoffDuration
	}
}

// parseRetryAfterDuration returns a Duration expressing the amount of time
// requested to wait by a Retry-After header, or 0 if not present or invalid.
// According to the spec (https://tools.ietf.org/html/rfc7231#section-7.1.3)
// the Retry-After Header's value can be one of an HTTP-date or delay-seconds,
// both of which are supported here.
func parseRetryAfterDuration(resp *nethttp.Response) (retryAfterDuration time.Duration) {

	// Return 0 Duration If No Response / Headers
	if resp == nil || resp.Header == nil {
		return
	}

	// Return 0 Duration If No Retry-After Header
	retryAfterString := resp.Header.Get(RetryAfterHeader)
	if len(retryAfterString) <= 0 {
		return
	}

	// Attempt To Parse Retry-After Header As Seconds - Return If Successful
	retryAfterInt, parseIntErr := strconv.ParseInt(retryAfterString, 10, 64)
	if parseIntErr == nil {
		return time.Duration(retryAfterInt) * time.Second
	}

	// Attempt To Parse Retry-After Header As Timestamp (RFC850 & ANSIC) - Return If Successful
	retryAfterTime, parseTimeErr := nethttp.ParseTime(retryAfterString)
	if parseTimeErr != nil {
		fmt.Printf("failed to parse Retry-After header: ParseInt Error = %v, ParseTime Error = %v\n", parseIntErr, parseTimeErr)
		return
	}
	return time.Until(retryAfterTime)
}
