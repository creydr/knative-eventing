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

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

type RequestOption func(context.Context, *Request) error

func WithHeader(key, value string) RequestOption {
	return func(_ context.Context, r *Request) error {
		r.SetHeader(key, value)

		return nil
	}
}

func WithCEOverride(overrides *duckv1.CloudEventOverrides) RequestOption {
	return func(ctx context.Context, req *Request) error {
		// get event from request
		message := http.NewMessageFromHttpRequest(req.Request)
		defer message.Finish(nil)

		event, err := binding.ToEvent(ctx, message)
		if err != nil {
			return fmt.Errorf("could not get event from request: %w", err)
		}

		// add overrides
		for n, v := range overrides.Extensions {
			event.SetExtension(n, v)
		}

		// write event back to request
		if err = req.BindEvent(ctx, *event); err != nil {
			return fmt.Errorf("could not write updated event back to request: %w", err)
		}

		return nil
	}
}
