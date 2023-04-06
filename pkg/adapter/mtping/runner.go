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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	kncloudevents "knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/eventing/pkg/adapter/v2/util/crstatusevent"
	sourcesv1 "knative.dev/eventing/pkg/apis/sources/v1"
	"knative.dev/eventing/pkg/observability"
)

type CronJobRunner interface {
	Start(stopCh <-chan struct{})
	Stop()
	AddSchedule(source *sourcesv1.PingSource) cron.EntryID
	RemoveSchedule(id cron.EntryID)
}

type cronJobsRunner struct {
	// The cron job runner
	cron cron.Cron

	// client sends cloudevents.
	Client cloudevents.Client

	// Where to send logs
	Logger *zap.SugaredLogger

	// kubeClient for sending k8s events
	kubeClient kubernetes.Interface
}

const (
	resourceGroup = "pingsources.sources.knative.dev"
)

func NewCronJobsRunner(ceClient cloudevents.Client, kubeClient kubernetes.Interface, logger *zap.SugaredLogger, opts ...cron.Option) *cronJobsRunner {
	return &cronJobsRunner{
		cron:       *cron.New(opts...),
		Client:     ceClient,
		Logger:     logger,
		kubeClient: kubeClient,
	}
}

func (a *cronJobsRunner) AddSchedule(source *sourcesv1.PingSource) cron.EntryID {
	event, err := makeEvent(source)
	if err != nil {
		a.Logger.Error("failed to makeEvent: ", zap.Error(err))
	}

	ctx := context.Background()
	ctx = cloudevents.ContextWithTarget(ctx, source.Status.SinkURI.String())

	var kubeEventSink record.EventSink = &typedcorev1.EventSinkImpl{Interface: a.kubeClient.CoreV1().Events(source.Namespace)}
	ctx = crstatusevent.ContextWithCRStatus(ctx, &kubeEventSink, "ping-source-mt-adapter", source, a.Logger.Infof)

	// Simple retry configuration to be less than 1mn.
	// We might want to retry more times for less-frequent schedule.
	ctx = cloudevents.ContextWithRetriesExponentialBackoff(ctx, 50*time.Millisecond, 5)

	metricTag := &kncloudevents.MetricTag{
		Namespace:     source.Namespace,
		Name:          source.Name,
		ResourceGroup: resourceGroup,
	}

	// See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#span-name
	spanName := source.Status.SinkURI.String() + " send"

	ctx = observability.WithSpanData(ctx, spanName, int(trace.SpanKindProducer),
		observability.K8sAttributes(source.Name, source.Namespace, sourcesv1.Resource("pingsource").String()))

	schedule := source.Spec.Schedule
	if source.Spec.Timezone != "" {
		schedule = "CRON_TZ=" + source.Spec.Timezone + " " + schedule
	}

	ctx = kncloudevents.ContextWithMetricTag(ctx, metricTag)
	id, _ := a.cron.AddFunc(schedule, a.cronTick(ctx, event))
	return id
}

func (a *cronJobsRunner) RemoveSchedule(id cron.EntryID) {
	a.cron.Remove(id)
}

func (a *cronJobsRunner) Start(stopCh <-chan struct{}) {
	a.cron.Start()
	<-stopCh
}

func (a *cronJobsRunner) Stop() {
	ctx := a.cron.Stop() // no more ticks
	if ctx != nil {
		// Wait for all jobs to be done.
		<-ctx.Done()
	}
}

func (a *cronJobsRunner) cronTick(ctx context.Context, event cloudevents.Event) func() {
	return func() {
		event := event.Clone()
		event.SetID(uuid.New().String()) // provide an ID here so we can track it with logging
		defer a.Logger.Debug("Finished sending cloudevent id: ", event.ID())
		target := cecontext.TargetFrom(ctx).String()
		source := event.Context.GetSource()

		// Provide a delay so not all ping fired instantaneously distribute load on resources.
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) //nolint:gosec // Cryptographic randomness not necessary here.

		a.Logger.Debugf("sending cloudevent id: %s, source: %s, target: %s", event.ID(), source, target)

		// get token and add to headers (could get target for event request from context)
		body := []byte("client_id=knative-service-1&client_secret=tXiXbovizltYEfqvBpSV7wGpzAgfkouJ&grant_type=client_credentials&scope=openid")
		request, err := http.NewRequest("POST", "https://192.168.178.22:8443/realms/knative-test/protocol/openid-connect/token", bytes.NewBuffer(body))
		if err != nil {
			a.Logger.Error("failed to create request to get OIDC token", zap.Error(err))
		}
		request.Header.Add("Content-Type", "application/x-www-form-urlencoded") //during my tests I didn't see my keycloak accepting application/json (thus body is not in JSON format :/ )
		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // XO
				},
			},
		}
		res, err := httpClient.Do(request)
		if err != nil {
			a.Logger.Error("failed to execute request to get OIDC token", zap.Error(err))
		}
		defer res.Body.Close()

		result := &Token{}
		if err = json.NewDecoder(res.Body).Decode(result); err != nil {
			a.Logger.Error("failed to decode OIDC response into token", zap.Error(err))
		}

		header := http.Header{}
		header.Set("Authorization", fmt.Sprintf("Bearer %s", result.IdToken))
		ctx = cehttp.WithCustomHeader(ctx, header)

		if result := a.Client.Send(ctx, event); !cloudevents.IsACK(result) {
			// Exhausted number of retries. Event is lost.
			a.Logger.Error("failed to send cloudevent result: ", zap.Any("result", result),
				zap.String("source", source), zap.String("target", target), zap.String("id", event.ID()))
		}
	}
}

type Token struct {
	AccessToken      string `json:"access_token,omitempty"`
	ExpiresIn        int    `json:"expires_in,omitempty"`
	RefreshExpiresIn int    `json:"refresh_expires_in,omitempty"`
	TokenType        string `json:"token_type,omitempty"`
	IdToken          string `json:"id_token,omitempty"`
	NotBeforePolicy  int    `json:"not-before-policy,omitempty"`
	Scope            string `json:"scope,omitempty"`
}

func makeEvent(source *sourcesv1.PingSource) (cloudevents.Event, error) {
	event := cloudevents.NewEvent()
	event.SetType(sourcesv1.PingSourceEventType)
	event.SetSource(sourcesv1.PingSourceSource(source.Namespace, source.Name))
	if source.Spec.CloudEventOverrides != nil && source.Spec.CloudEventOverrides.Extensions != nil {
		for key, override := range source.Spec.CloudEventOverrides.Extensions {
			event.SetExtension(key, override)
		}
	}

	var data interface{}
	if source.Spec.DataBase64 != "" {
		data, _ = base64.StdEncoding.DecodeString(source.Spec.DataBase64)
	} else if source.Spec.Data != "" {
		data = []byte(source.Spec.Data)
	}

	if data != nil {
		if err := event.SetData(source.Spec.ContentType, data); err != nil {
			return event, fmt.Errorf("error when SetData(%v, %v), err: %v", source.Spec.ContentType, data, err)
		}
	}

	return event, nil
}
