/*
Copyright 2019 The Knative Authors

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
package main

import (
	"context"
	"net"

	//nolint:gosec
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	cmdbroker "knative.dev/eventing/cmd/broker"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/auth"
	"knative.dev/eventing/pkg/certificates"
	eventpolicyinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1alpha1/eventpolicy"
	"knative.dev/eventing/pkg/client/injection/informers/sinks/v1alpha1/integrationsink"
	sinkslister "knative.dev/eventing/pkg/client/listers/sinks/v1alpha1"
	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/eventing/pkg/kncloudevents"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/filtered"
	filteredFactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	configmap "knative.dev/pkg/configmap/informer"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	secretinformer "knative.dev/pkg/injection/clients/namespacedkube/informers/core/v1/secret"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/network"
	"knative.dev/pkg/signals"
	"knative.dev/pkg/system"
)

const component = "auth-proxy"

type envConfig struct {
	TargetHost      string `envconfig:"TARGET_HOST" default:"localhost"`
	TargetHTTPPort  int    `envconfig:"TARGET_HTTP_PORT"  default:"8080"`
	TargetHTTPSPort int    `envconfig:"TARGET_HTTPS_PORT"  default:"8443"`
	ProxyHTTPPort   int    `envconfig:"PROXY_HTTP_PORT" default:"3128"`
	ProxyHTTPSPort  int    `envconfig:"PROXY_HTTPS_PORT" default:"3129"`

	IntegrationSinkName      string `envconfig:"INTEGRATION_SINK_NAME" required:"true"`
	IntegrationSinkNamespace string `envconfig:"INTEGRATION_SINK_NAMESPACE" required:"true"`
}

type Handler struct {
	k8s          kubernetes.Interface
	lister       sinkslister.IntegrationSinkLister
	withContext  func(ctx context.Context) context.Context
	authVerifier *auth.Verifier
	httpProxy    *httputil.ReverseProxy
	httpsProxy   *httputil.ReverseProxy
	ref          types.NamespacedName
}

func main() {
	ctx := signals.NewContext()

	cfg := injection.ParseAndGetRESTConfigOrDie()
	ctx = injection.WithConfig(ctx, cfg)
	ctx = filteredFactory.WithSelectors(ctx,
		eventingtls.TrustBundleLabelSelector,
	)

	ctx, informers := injection.Default.SetupInformers(ctx, cfg)
	ctx = injection.WithConfig(ctx, cfg)

	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Fatal("Failed to process env var", zap.Error(err))
	}

	loggingConfig, err := cmdbroker.GetLoggingConfig(ctx, system.Namespace(), logging.ConfigMapName())
	if err != nil {
		log.Fatal("Error loading/parsing logging configuration:", err)
	}
	sl, atomicLevel := logging.NewLoggerFromConfig(loggingConfig, component)
	logger := sl.Desugar()
	defer flush(sl)

	// Watch the logging config map and dynamically update logging levels.
	configMapWatcher := configmap.NewInformedWatcher(kubeclient.Get(ctx), system.Namespace())

	// Watch the observability config map and dynamically update request logs.
	configMapWatcher.Watch(logging.ConfigMapName(), logging.UpdateLevelFromConfigMap(sl, atomicLevel, component))

	trustBundleConfigMapLister := configmapinformer.Get(ctx, eventingtls.TrustBundleLabelSelector).Lister().ConfigMaps(system.Namespace())

	featureStore := feature.NewStore(logging.FromContext(ctx).Named("feature-config-store"))
	featureStore.WatchConfigs(configMapWatcher)

	handler := &Handler{
		k8s:          kubeclient.Get(ctx),
		lister:       integrationsink.Get(ctx).Lister(),
		authVerifier: auth.NewVerifier(ctx, eventpolicyinformer.Get(ctx).Lister(), trustBundleConfigMapLister, configMapWatcher),
		ref: types.NamespacedName{
			Name:      env.IntegrationSinkName,
			Namespace: env.IntegrationSinkNamespace,
		},
	}

	// Decorate contexts with the current state of the feature config.
	handler.withContext = func(ctx context.Context) context.Context {
		return logging.WithLogger(featureStore.ToContext(ctx), sl)
	}

	tlsConfig, err := getServerTLSConfig(ctx, handler.ref)
	if err != nil {
		log.Fatal("Failed to get TLS config", err)
	}

	sm, err := eventingtls.NewServerManager(ctx,
		kncloudevents.NewHTTPEventReceiver(env.ProxyHTTPPort),
		kncloudevents.NewHTTPEventReceiver(env.ProxyHTTPSPort,
			kncloudevents.WithTLSConfig(tlsConfig)),
		handler,
		configMapWatcher,
	)
	if err != nil {
		log.Fatal(err)
	}

	// configMapWatcher does not block, so start it first.
	logger.Info("Starting ConfigMap watcher")
	if err = configMapWatcher.Start(ctx.Done()); err != nil {
		logger.Fatal("Failed to start ConfigMap watcher", zap.Error(err))
	}

	// Start informers and wait for them to sync.
	logger.Info("Starting informers.")
	if err := controller.StartInformers(ctx.Done(), informers...); err != nil {
		logger.Fatal("Failed to start informers", zap.Error(err))
	}

	// After we started the informers, we can read our IntegrationSink CR and set up the proxies
	integrationSink, err := handler.lister.IntegrationSinks(env.IntegrationSinkNamespace).Get(env.IntegrationSinkName)
	if err != nil {
		logger.Fatal("Failed to get integration sink", zap.Error(err))
	}

	sinkAddress := eventingtls.GetHttpsAddress(integrationSink.Status.Addresses)
	if sinkAddress == nil {
		// no https address set, so take the one in address field
		sinkAddress = integrationSink.Status.Address
	}

	// setup reverse proxy (we need one which forwards to HTTP and one which forwards to HTTPS)
	httpProxy, httpsProxy, err := reverseProxies(sinkAddress, trustBundleConfigMapLister, env)
	if err != nil {
		log.Fatalf("Failed to create proxies: %v", err)
	}

	handler.httpProxy = httpProxy
	handler.httpsProxy = httpsProxy

	// Start the servers
	logger.Info("Starting...")
	if err = sm.StartServers(ctx); err != nil {
		logger.Fatal("StartServers() returned an error", zap.Error(err))
	}

	logger.Info("Exiting...")
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := h.withContext(r.Context())
	logger := logging.FromContext(ctx).Desugar()

	logger.Debug("Handling request", zap.String("URI", r.RequestURI))

	integrationSink, err := h.lister.IntegrationSinks(h.ref.Namespace).Get(h.ref.Name)
	if err != nil {
		logger.Warn("Failed to retrieve integration sink", zap.String("ref", h.ref.String()), zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = h.authVerifier.VerifyRequest(ctx, feature.FromContext(ctx), integrationSink.Status.Address.Audience, integrationSink.Namespace, integrationSink.Status.Policies, r, w)
	if err != nil {
		logger.Warn("Failed to verify AuthN and AuthZ.", zap.Error(err))
		return
	}

	if r.TLS == nil {
		logger.Debug("Passing to HTTP proxy")
		h.httpProxy.ServeHTTP(w, r)
	} else {
		logger.Debug("Passing to HTTPS proxy")
		h.httpsProxy.ServeHTTP(w, r)
	}
}

func reverseProxies(addressable *duckv1.Addressable, trustBundleConfigMapLister corev1listers.ConfigMapNamespaceLister, env envConfig) (*httputil.ReverseProxy, *httputil.ReverseProxy, error) {
	httpTarget := fmt.Sprintf("http://%s:%d", env.TargetHost, env.TargetHTTPPort)
	httpsTarget := fmt.Sprintf("https://%s:%d", env.TargetHost, env.TargetHTTPSPort)

	httpTargetURL, err := url.Parse(httpTarget)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse http target URL: %v", err)
	}

	httpsTargetURL, err := url.Parse(httpsTarget)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse https target URL: %v", err)
	}

	// Create reverse proxies
	httpProxy := httputil.NewSingleHostReverseProxy(httpTargetURL)
	httpsProxy := httputil.NewSingleHostReverseProxy(httpsTargetURL)

	httpsProxy.Director = func(req *http.Request) {
		// in case of https requests, we need to rewrite the request URL/host, as otherwise, we get a certificate validation error
		req.URL.Scheme = "https"
		req.URL.Host = httpsTargetURL.Host
		req.Host = addressable.URL.Host
	}

	if eventingtls.IsHttpsSink(addressable.URL.String()) {
		var base = http.DefaultTransport.(*http.Transport).Clone()
		clientConfig := eventingtls.ClientConfig{
			CACerts:                    addressable.CACerts,
			TrustBundleConfigMapLister: trustBundleConfigMapLister,
		}

		base.DialTLSContext = func(ctx context.Context, net, addr string) (net.Conn, error) {
			tlsConfig, err := eventingtls.GetTLSClientConfig(clientConfig)
			if err != nil {
				return nil, err
			}
			tlsConfig.ServerName = addressable.URL.Host

			return network.DialTLSWithBackOff(ctx, net, fmt.Sprintf("%s:%d", env.TargetHost, env.TargetHTTPSPort), tlsConfig)
		}

		httpsProxy.Transport = base
	}

	return httpProxy, httpsProxy, nil
}

func flush(logger *zap.SugaredLogger) {
	_ = logger.Sync()
	metrics.FlushExporter()
}

func getServerTLSConfig(ctx context.Context, ref types.NamespacedName) (*tls.Config, error) {
	secret := types.NamespacedName{
		Name:      certificates.CertificateName(ref.Name),
		Namespace: ref.Namespace,
	}

	serverTLSConfig := eventingtls.NewDefaultServerConfig()
	serverTLSConfig.GetCertificate = eventingtls.GetCertificateFromSecret(ctx, secretinformer.Get(ctx), kubeclient.Get(ctx), secret)
	return eventingtls.GetTLSServerConfig(serverTLSConfig)
}

/*func main() {
	//ctx := signals.NewContext()

	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Fatal("Failed to process env var", zap.Error(err))
	}

	// Parse the target URL
	targetURL, err := url.Parse(fmt.Sprintf("http://%s:%d", env.TargetHost, env.TargetHTTPPort))
	if err != nil {
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	log.Printf("Target URL: %s", targetURL)

	// Create a reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Modify request before sending to the target (optional)
	proxy.ModifyResponse = func(resp *http.Response) error {
		// You can modify the response here if needed
		return nil
	}

	// Start the server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("request uri: ", r.RequestURI)

		// Optionally modify the request here
		proxy.ServeHTTP(w, r)
	})

	log.Printf("Starting proxy server on :%d", env.ProxyHTTPPort)
	err = http.ListenAndServe(fmt.Sprintf(":%d", env.ProxyHTTPPort), nil)
	if err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}*/
