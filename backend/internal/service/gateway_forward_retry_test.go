//go:build unit

package service

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldRetryUpstreamError_OAuthRetriesTransientServerErrors(t *testing.T) {
	account := &Account{Type: AccountTypeOAuth}
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		require.True(t, (&GatewayService{}).shouldRetryUpstreamError(account, status), "status %d", status)
	}
	require.True(t, (&GatewayService{}).shouldRetryUpstreamError(account, http.StatusForbidden))
	require.False(t, (&GatewayService{}).shouldRetryUpstreamError(account, http.StatusTooManyRequests), "429 uses OAuth-specific failover/retry-window handling")
}

func TestShouldRetryUpstreamError_OAuthDoesNotRetryClientErrors(t *testing.T) {
	account := &Account{Type: AccountTypeOAuth}
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound} {
		require.False(t, (&GatewayService{}).shouldRetryUpstreamError(account, status), "status %d", status)
	}
}
