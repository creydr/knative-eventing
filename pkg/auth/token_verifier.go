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

package auth

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

type OIDCTokenVerifier struct {
	logger     *zap.SugaredLogger
	kubeClient kubernetes.Interface
}

func NewOIDCTokenVerifier(ctx context.Context) *OIDCTokenVerifier {
	tokenHandler := &OIDCTokenVerifier{
		logger:     logging.FromContext(ctx).With("component", "oidc-token-handler"),
		kubeClient: kubeclient.Get(ctx),
	}

	return tokenHandler
}

// VerifyJWT verifies the given JWT for the expected audience and returns the user info.
func (c *OIDCTokenVerifier) VerifyJWT(ctx context.Context, jwt, audience string) (*authv1.UserInfo, error) {
	tokenReview := authv1.TokenReview{
		// ObjectMeta: metav1.ObjectMeta{
		// 	Name: uuid.NewString(),
		// },
		Spec: authv1.TokenReviewSpec{
			Token: jwt,
			Audiences: []string{
				audience,
			},
		},
	}

	tokenReviewResult, err := c.kubeClient.AuthenticationV1().TokenReviews().Create(ctx, &tokenReview, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not create token review: %w", err)
	}

	if err := tokenReviewResult.Status.Error; err != "" {
		return nil, fmt.Errorf(err)
	}

	if !tokenReviewResult.Status.Authenticated {
		return nil, fmt.Errorf("token review status: user not authenticated")
	}

	return &tokenReview.Status.User, nil
}
