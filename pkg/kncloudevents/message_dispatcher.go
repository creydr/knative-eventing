/*
Copyright 2023 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implie
See the License for the specific language governing permissions and
limitations under the License.
*/

package kncloudevents

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/event"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"go.opencensus.io/trace"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"
	"knative.dev/pkg/system"

	eventingapis "knative.dev/eventing/pkg/apis"

	"knative.dev/eventing/pkg/broker"
	"knative.dev/eventing/pkg/channel/attributes"
	"knative.dev/eventing/pkg/tracing"
	"knative.dev/eventing/pkg/utils"
)

const (
	// noDuration signals that the dispatch step hasn't started
	NoDuration = -1
	NoResponse = -1
)

type SendOption func(*senderConfig) error

func WithReply(reply *duckv1.Addressable) SendOption {
	return func(sc *senderConfig) error {
		sc.reply = reply

		return nil
	}
}

func WithDeadLetterSink(dls *duckv1.Addressable) SendOption {
	return func(sc *senderConfig) error {
		sc.deadLetterSink = dls

		return nil
	}
}

func WithHeader(header http.Header) SendOption {
	return func(sc *senderConfig) error {
		sc.additionalHeaders = header

		return nil
	}
}

type DispatchInfo struct {
	Duration       time.Duration
	ResponseCode   int
	ResponseHeader http.Header
	ResponseBody   []byte
}

type senderConfig struct {
	reply             *duckv1.Addressable
	deadLetterSink    *duckv1.Addressable
	additionalHeaders http.Header
	retryConfig       *RetryConfig
}

func SendEvent(ctx context.Context, event event.Event, destination duckv1.Addressable, options ...SendOption) (*DispatchInfo, error) {
	message := binding.ToMessage(&event)

	return SendMessage(ctx, message, destination, options...)
}

func SendMessage(ctx context.Context, message binding.Message, destination duckv1.Addressable, options ...SendOption) (*DispatchInfo, error) {
	config := &senderConfig{
		additionalHeaders: make(http.Header),
	}

	// apply options
	for _, opt := range options {
		if err := opt(config); err != nil {
			return nil, fmt.Errorf("could not apply option: %w", err)
		}
	}

	return send(ctx, message, destination, config)
}

func send(ctx context.Context, message binding.Message, destination duckv1.Addressable, config *senderConfig) (*DispatchInfo, error) {
	// All messages that should be finished at the end of this function
	// are placed in this slice
	var messagesToFinish []binding.Message
	defer func() {
		for _, msg := range messagesToFinish {
			_ = msg.Finish(nil)
		}
	}()

	// sanitize eventual host-only URLs
	destination = *sanitizeAddressable(&destination)
	config.reply = sanitizeAddressable(config.reply)
	config.deadLetterSink = sanitizeAddressable(config.deadLetterSink)

	// If there is a destination, variables response* are filled with the response of the destination
	// Otherwise, they are filled with the original message
	var responseMessage cloudevents.Message
	var responseAdditionalHeaders http.Header
	var dispatchExecutionInfo *DispatchInfo

	if destination.URL != nil {
		var err error
		// Try to send to destination
		messagesToFinish = append(messagesToFinish, message)

		// Add `Prefer: reply` header no matter if a reply destination is provide Discussion: https://github.com/knative/eventing/pull/5764
		additionalHeadersForDestination := http.Header{}
		if config.additionalHeaders != nil {
			additionalHeadersForDestination = config.additionalHeaders.Clone()
		}
		additionalHeadersForDestination.Set("Prefer", "reply")

		ctx, responseMessage, dispatchExecutionInfo, err = executeRequest(ctx, destination, message, additionalHeadersForDestination, config.retryConfig)
		if err != nil {
			// If DeadLetter is configured, then send original message with knative error extensions
			if config.deadLetterSink != nil {
				dispatchTransformers := dispatchExecutionInfoTransformers(destination.URL, dispatchExecutionInfo)
				_, deadLetterResponse, dispatchExecutionInfo, deadLetterErr := executeRequest(ctx, *config.deadLetterSink, message, config.additionalHeaders, config.retryConfig, dispatchTransformers...)
				if deadLetterErr != nil {
					return dispatchExecutionInfo, fmt.Errorf("unable to complete request to either %s (%v) or %s (%v)", destination.URL, err, config.deadLetterSink.URL, deadLetterErr)
				}
				if deadLetterResponse != nil {
					messagesToFinish = append(messagesToFinish, deadLetterResponse)
				}

				return dispatchExecutionInfo, nil
			}
			// No DeadLetter, just fail
			return dispatchExecutionInfo, fmt.Errorf("unable to complete request to %s: %v", destination.URL, err)
		}

		responseAdditionalHeaders = dispatchExecutionInfo.ResponseHeader
	} else {
		// No destination url, try to send to reply if available
		responseMessage = message
		responseAdditionalHeaders = config.additionalHeaders
	}

	if config.additionalHeaders.Get(eventingapis.KnNamespaceHeader) != "" {
		if responseAdditionalHeaders == nil {
			responseAdditionalHeaders = make(http.Header)
		}
		responseAdditionalHeaders.Set(eventingapis.KnNamespaceHeader, config.additionalHeaders.Get(eventingapis.KnNamespaceHeader))
	}

	if responseMessage == nil {
		// No response, dispatch completed
		return dispatchExecutionInfo, nil
	}

	messagesToFinish = append(messagesToFinish, responseMessage)

	if config.reply == nil {
		return dispatchExecutionInfo, nil
	}

	// send reply

	ctx, responseResponseMessage, dispatchExecutionInfo, err := executeRequest(ctx, *config.reply, responseMessage, responseAdditionalHeaders, config.retryConfig)
	if err != nil {
		// If DeadLetter is configured, then send original message with knative error extensions
		if config.deadLetterSink != nil {
			dispatchTransformers := dispatchExecutionInfoTransformers(config.reply.URL, dispatchExecutionInfo)
			_, deadLetterResponse, dispatchExecutionInfo, deadLetterErr := executeRequest(ctx, *config.deadLetterSink, message, responseAdditionalHeaders, config.retryConfig, dispatchTransformers)
			if deadLetterErr != nil {
				return dispatchExecutionInfo, fmt.Errorf("failed to forward reply to %s (%v) and failed to send it to the dead letter sink %s (%v)", config.reply.URL, err, config.deadLetterSink.URL, deadLetterErr)
			}
			if deadLetterResponse != nil {
				messagesToFinish = append(messagesToFinish, deadLetterResponse)
			}

			return dispatchExecutionInfo, nil
		}
		// No DeadLetter, just fail
		return dispatchExecutionInfo, fmt.Errorf("failed to forward reply to %s: %v", config.reply.URL, err)
	}
	if responseResponseMessage != nil {
		messagesToFinish = append(messagesToFinish, responseResponseMessage)
	}

	return dispatchExecutionInfo, nil
}

func executeRequest(ctx context.Context, target duckv1.Addressable, message cloudevents.Message, additionalHeaders http.Header, retryConfig *RetryConfig, transformers ...binding.Transformer) (context.Context, cloudevents.Message, *DispatchInfo, error) {
	dispatchInfo := DispatchInfo{
		Duration:       NoDuration,
		ResponseCode:   NoResponse,
		ResponseHeader: make(http.Header),
	}

	ctx, span := trace.StartSpan(ctx, "knative.dev", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	req, err := NewCloudEventRequest(ctx, target)
	if err != nil {
		return ctx, nil, &dispatchInfo, err
	}

	if span.IsRecordingEvents() {
		transformers = append(transformers, tracing.PopulateSpan(span, target.URL.String()))
	}

	err = WriteRequestWithAdditionalHeaders(ctx, message, req, additionalHeaders, transformers...)
	if err != nil {
		return ctx, nil, &dispatchInfo, err
	}

	start := time.Now()
	response, err := req.SendWithRetries(retryConfig)
	dispatchTime := time.Since(start)
	if err != nil {
		dispatchInfo.Duration = dispatchTime
		dispatchInfo.ResponseCode = http.StatusInternalServerError
		dispatchInfo.ResponseBody = []byte(fmt.Sprintf("dispatch error: %s", err.Error()))
		return ctx, nil, &dispatchInfo, err
	}

	dispatchInfo.ResponseCode = response.StatusCode
	dispatchInfo.ResponseHeader = utils.PassThroughHeaders(response.Header)

	body := new(bytes.Buffer)
	_, readErr := body.ReadFrom(response.Body)

	if isFailure(response.StatusCode) {
		// Read response body into dispatchInfo for failures
		if readErr != nil && readErr != io.EOF {
			dispatchInfo.ResponseBody = []byte(fmt.Sprintf("dispatch resulted in status \"%s\". Could not read response body: error: %s", response.Status, err.Error()))
		} else {
			dispatchInfo.ResponseBody = body.Bytes()
		}
		response.Body.Close()

		// Reject non-successful responses.
		return ctx, nil, &dispatchInfo, fmt.Errorf("unexpected HTTP response, expected 2xx, got %d", response.StatusCode)
	}

	var responseMessageBody []byte
	if readErr != nil && readErr != io.EOF {
		responseMessageBody = []byte(fmt.Sprintf("Failed to read response body: %s", err.Error()))
	} else {
		responseMessageBody = body.Bytes()
	}
	responseMessage := cehttp.NewMessage(response.Header, io.NopCloser(bytes.NewReader(responseMessageBody)))

	if responseMessage.ReadEncoding() == binding.EncodingUnknown {
		// Response is a non event, discard it
		response.Body.Close()
		responseMessage.BodyReader.Close()
		return ctx, nil, &dispatchInfo, nil
	}

	return ctx, responseMessage, &dispatchInfo, nil
}

// dispatchExecutionTransformer returns Transformers based on the specified destination and DispatchExecutionInfo
func dispatchExecutionInfoTransformers(destination *apis.URL, dispatchExecutionInfo *DispatchInfo) binding.Transformers {
	if destination == nil {
		destination = &apis.URL{}
	}

	httpResponseBody := dispatchExecutionInfo.ResponseBody
	if destination.Host == network.GetServiceHostname("broker-filter", system.Namespace()) {

		var errExtensionInfo broker.ErrExtensionInfo

		err := json.Unmarshal(dispatchExecutionInfo.ResponseBody, &errExtensionInfo)
		if err != nil {
			//d.logger.Debug("Unmarshal dispatchExecutionInfo ResponseBody failed", zap.Error(err))
			return nil
		}
		destination = errExtensionInfo.ErrDestination
		httpResponseBody = errExtensionInfo.ErrResponseBody
	}

	destination = sanitizeURL(destination)

	// Encodes response body as base64 for the resulting length.
	bodyLen := len(httpResponseBody)
	encodedLen := base64.StdEncoding.EncodedLen(bodyLen)
	if encodedLen > attributes.KnativeErrorDataExtensionMaxLength {
		encodedLen = attributes.KnativeErrorDataExtensionMaxLength
	}
	encodedBuf := make([]byte, encodedLen)
	base64.StdEncoding.Encode(encodedBuf, httpResponseBody)

	return attributes.KnativeErrorTransformers(*destination.URL(), dispatchExecutionInfo.ResponseCode, string(encodedBuf[:encodedLen]))
}

// isFailure returns true if the status code is not a successful HTTP status.
func isFailure(statusCode int) bool {
	return statusCode < http.StatusOK /* 200 */ ||
		statusCode >= http.StatusMultipleChoices /* 300 */
}

func sanitizeAddressable(addressable *duckv1.Addressable) *duckv1.Addressable {
	if addressable == nil {
		return nil
	}

	addressable.URL = sanitizeURL(addressable.URL)

	return addressable
}

func sanitizeURL(url *apis.URL) *apis.URL {
	if url == nil {
		return nil
	}

	if url.Scheme == "http" || url.Scheme == "https" {
		// Already a URL with a known scheme.
		return url
	}

	return &apis.URL{
		Scheme: "http",
		Host:   url.Host,
		Path:   "/",
	}
}
