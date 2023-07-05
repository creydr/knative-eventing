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

package apiserver

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/adapter/apiserver/events"
	"knative.dev/eventing/pkg/kncloudevents"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

type resourceDelegate struct {
	ce                  kncloudevents.Client
	source              string
	ref                 bool
	apiServerSourceName string
	target              duckv1.Addressable

	logger *zap.SugaredLogger
}

var _ cache.Store = (*resourceDelegate)(nil)

func (a *resourceDelegate) Add(obj interface{}) error {
	ctx, event, err := events.MakeAddEvent(a.source, a.apiServerSourceName, obj, a.ref)
	if err != nil {
		a.logger.Infow("event creation failed", zap.Error(err))
		return err
	}
	a.sendCloudEvent(ctx, event)
	return nil
}

func (a *resourceDelegate) Update(obj interface{}) error {
	ctx, event, err := events.MakeUpdateEvent(a.source, a.apiServerSourceName, obj, a.ref)
	if err != nil {
		a.logger.Info("event creation failed", zap.Error(err))
		return err
	}
	a.sendCloudEvent(ctx, event)
	return nil
}

func (a *resourceDelegate) Delete(obj interface{}) error {
	ctx, event, err := events.MakeDeleteEvent(a.source, a.apiServerSourceName, obj, a.ref)
	if err != nil {
		a.logger.Info("event creation failed", zap.Error(err))
		return err
	}
	a.sendCloudEvent(ctx, event)
	return nil
}

// sendCloudEvent sends a cloudevent everytime k8s api event is created, updated or deleted.
func (a *resourceDelegate) sendCloudEvent(ctx context.Context, event cloudevents.Event) {
	event.SetID(uuid.New().String()) // provide an ID here so we can track it with logging
	defer a.logger.Debug("Finished sending cloudevent id: ", event.ID())
	source := event.Context.GetSource()
	subject := event.Context.GetSubject()
	a.logger.Debugf("sending cloudevent id: %s, source: %s, subject: %s", event.ID(), source, subject)

	req, err := kncloudevents.NewRequest(ctx, a.target)
	if err != nil {
		a.logger.Errorw("failed to create cloudevent request",
			zap.Error(err),
			zap.Any("target", a.target),
			zap.String("id", event.ID()))
		return
	}

	err = req.BindEvent(ctx, event)
	if err != nil {
		a.logger.Errorw("failed to bind cloudevent to request",
			zap.Error(err),
			zap.Any("target", a.target),
			zap.String("id", event.ID()))
		return
	}

	resp, err := a.ce.Send(ctx, req)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		a.logger.Errorw("failed to send cloudevent",
			zap.Error(err),
			zap.String("response-status", resp.Status),
			zap.Any("target", a.target),
			zap.String("source", source),
			zap.String("subject", subject),
			zap.String("id", event.ID()))
	} else {
		a.logger.Debugf("cloudevent sent id: %s, source: %s, subject: %s", event.ID(), source, subject)
	}
}

// Stub cache.Store impl

// Implements cache.Store
func (a *resourceDelegate) List() []interface{} {
	return nil
}

// Implements cache.Store
func (a *resourceDelegate) ListKeys() []string {
	return nil
}

// Implements cache.Store
func (a *resourceDelegate) Get(obj interface{}) (item interface{}, exists bool, err error) {
	return nil, false, nil
}

// Implements cache.Store
func (a *resourceDelegate) GetByKey(key string) (item interface{}, exists bool, err error) {
	return nil, false, nil
}

// Implements cache.Store
func (a *resourceDelegate) Replace([]interface{}, string) error {
	return nil
}

// Implements cache.Store
func (a *resourceDelegate) Resync() error {
	return nil
}
