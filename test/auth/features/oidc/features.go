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

package oidc

import (
	"github.com/cloudevents/sdk-go/v2/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/auth"
	"knative.dev/eventing/test/rekt/resources/addressable"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/eventshub"
	eventassert "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/resources/service"
)

func BrokerGetsAudiencePopulated(namespace string) *feature.Feature {
	f := feature.NewFeature()

	brokerName := feature.MakeRandomK8sName("broker")

	f.Setup("install broker", broker.Install(brokerName, broker.WithEnvConfig()...))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	expectedAudience := auth.GetAudience(broker.GVR().GroupVersion().WithKind("Broker"), metav1.ObjectMeta{
		Name:      brokerName,
		Namespace: namespace,
	})

	f.Alpha("Broker").Must("have audience set", broker.ValidateAddress(brokerName, addressable.AssertAddressWithAudience(expectedAudience)))

	return f
}

func BrokerRejectEventForWrongAudience() *feature.Feature {
	f := feature.NewFeatureNamed("Broker reject event for wrong OIDC audience")

	source := feature.MakeRandomK8sName("source")
	brokerName := feature.MakeRandomK8sName("broker")
	sink := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("triggerName")

	eventForInvalidAudience := test.FullEvent()
	eventForInvalidAudience.SetID("event-for-invalid-aud")

	// Install the broker
	f.Setup("install broker", broker.Install(brokerName, broker.WithEnvConfig()...))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	// Install the sink
	f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiver))

	// Install the trigger and Point the Trigger subscriber to the sink svc.
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(service.AsKReference(sink), ""),
	))
	f.Setup("trigger goes ready", trigger.IsReady(triggerName))

	// Send event with wrong audience
	f.Requirement("install source", eventshub.Install(
		source,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.OIDCInvalidAudience(),
		eventshub.InputEvent(eventForInvalidAudience),
	))

	f.Alpha("Broker").
		Must("event sent", eventassert.OnStore(source).MatchSentEvent(test.HasId(eventForInvalidAudience.ID())).Exact(1)).
		Must("get 401 on response", eventassert.OnStore(source).Match(eventassert.MatchStatusCode(401)).Exact(1))

	return f
}

func BrokerHandlesEventWithValidOIDCToken() *feature.Feature {
	f := feature.NewFeatureNamed("Broker supports OIDC")

	source := feature.MakeRandomK8sName("source")
	brokerName := feature.MakeRandomK8sName("broker")
	sink := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("triggerName")

	event := test.FullEvent()

	// Install the broker
	f.Setup("install broker", broker.Install(brokerName, broker.WithEnvConfig()...))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	// Install the sink
	f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiver))

	// Install the trigger and Point the Trigger subscriber to the sink svc.
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(service.AsKReference(sink), ""),
	))
	f.Setup("trigger goes ready", trigger.IsReady(triggerName))

	// Send event with a valid OIDC token audience
	f.Requirement("install source", eventshub.Install(
		source,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.OIDCValidToken(),
		eventshub.InputEvent(event),
	))

	f.Alpha("Broker").
		Must("handles event with valid OIDC token", eventassert.OnStore(sink).MatchReceivedEvent(test.HasId(event.ID())).Exact(1))

	return f
}