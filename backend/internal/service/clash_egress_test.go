package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestClashEgressManagerRotateAccount(t *testing.T) {
	var selected string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer local-secret", r.Header.Get("Authorization"))
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/proxies":
			alive := true
			dead := false
			_ = json.NewEncoder(w).Encode(clashProxiesResponse{Proxies: map[string]clashProxyState{
				"sub2api-openai-slot-1": {Type: "Selector", Now: "node-a", All: []string{"node-a", "node-b", "node-c"}},
				"sub2api-openai-slot-2": {Type: "Selector", Now: "node-b", All: []string{"node-a", "node-b", "node-c"}},
				"node-b":                {Type: "Hysteria2", Alive: &alive},
				"node-c":                {Type: "Vless", Alive: &alive},
				"node-dead":             {Type: "Vless", Alive: &dead},
			}})
		case r.Method == http.MethodPut && r.URL.Path == "/proxies/sub2api-openai-slot-1":
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			selected = body["name"]
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	manager := newClashEgressManager(&config.Config{ClashEgress: config.ClashEgressConfig{
		Enabled:            true,
		ControllerURL:      server.URL,
		Secret:             "local-secret",
		ManagedProxyPrefix: "clash-openai-slot-",
		SelectorPrefix:     "sub2api-openai-slot-",
	}})
	account := &Account{ID: 42, Proxy: &Proxy{Name: "clash-openai-slot-1"}}

	require.True(t, manager.RotateAccount(context.Background(), account))
	require.Equal(t, "node-c", selected, "node-b is already pinned to another account")
	require.True(t, manager.ConsumeRecentRotation(account.ID))
	require.False(t, manager.ConsumeRecentRotation(account.ID))
}

func TestNextClashEgressNodeSkipsUnavailable(t *testing.T) {
	alive := true
	dead := false
	states := map[string]clashProxyState{
		"node-b": {Alive: &dead},
		"node-c": {Alive: &alive},
	}
	require.Equal(t, "node-c", nextClashEgressNode(
		[]string{"node-a", "node-b", "node-c"}, "node-a", nil, states,
	))
}

func TestLeastUsedManagedProxy(t *testing.T) {
	proxies := []ProxyWithAccountCount{
		{Proxy: Proxy{ID: 4, Name: "ordinary"}, AccountCount: 0},
		{Proxy: Proxy{ID: 12, Name: "clash-openai-slot-2"}, AccountCount: 2},
		{Proxy: Proxy{ID: 11, Name: "clash-openai-slot-1"}, AccountCount: 1},
		{Proxy: Proxy{ID: 10, Name: "clash-openai-slot-3"}, AccountCount: 1},
	}

	selected := leastUsedManagedProxy(proxies, "clash-openai-slot-")
	require.NotNil(t, selected)
	require.Equal(t, int64(10), selected.ID)
	require.Nil(t, leastUsedManagedProxy(proxies, "missing-"))
}

type clashAssignmentProxyRepo struct {
	ProxyRepository
	proxies []ProxyWithAccountCount
}

func (r clashAssignmentProxyRepo) ListActiveWithAccountCount(context.Context) ([]ProxyWithAccountCount, error) {
	return r.proxies, nil
}

func TestClashEgressAssignManagedProxy(t *testing.T) {
	manager := &clashEgressManager{managedProxyPrefix: defaultClashManagedProxyPrefix}
	repo := clashAssignmentProxyRepo{proxies: []ProxyWithAccountCount{
		{Proxy: Proxy{ID: 21, Name: "clash-openai-slot-1"}, AccountCount: 2},
		{Proxy: Proxy{ID: 22, Name: "clash-openai-slot-2"}, AccountCount: 0},
	}}

	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.NoError(t, manager.assignManagedProxy(context.Background(), repo, oauth))
	require.NotNil(t, oauth.ProxyID)
	require.Equal(t, int64(22), *oauth.ProxyID)

	apiKey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	require.NoError(t, manager.assignManagedProxy(context.Background(), repo, apiKey))
	require.Nil(t, apiKey.ProxyID)

	require.ErrorIs(t,
		manager.assignManagedProxy(context.Background(), clashAssignmentProxyRepo{}, &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}),
		errNoManagedClashEgressProxy,
	)
}

func TestClashEgressManagerRejectsUnmanagedProxy(t *testing.T) {
	manager := &clashEgressManager{managedProxyPrefix: "clash-openai-slot-", selectorPrefix: "sub2api-openai-slot-"}
	account := &Account{ID: 42, Proxy: &Proxy{Name: "ordinary-proxy"}}
	require.False(t, manager.RotateAccount(context.Background(), account))
}

func TestClashEgressManagerReadsSecretFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller.secret")
	require.NoError(t, os.WriteFile(path, []byte("file-secret\n"), 0o600))
	manager := newClashEgressManager(&config.Config{ClashEgress: config.ClashEgressConfig{
		Enabled:       true,
		ControllerURL: "http://127.0.0.1:9097",
		SecretFile:    path,
	}})
	require.NotNil(t, manager)
	require.Equal(t, "file-secret", manager.secret)
}

func TestOpenAIIPAuthorizationErrorClassification(t *testing.T) {
	require.True(t, isOpenAIIPAuthorizationError([]byte(`{"error":{"message":"Your IP is not authorized to make this request"}}`)))
	require.True(t, isOpenAIIPAuthorizationError([]byte(`{"error":{"message":"IP address is not authorized"}}`)))
	require.False(t, isOpenAIIPAuthorizationError([]byte(`{"error":{"message":"token_invalidated"}}`)))
	require.False(t, isOpenAIIPAuthorizationError([]byte(`{"error":{"message":"Rate limit exceeded"}}`)))
}

func TestClashEgressRecentRotationExpires(t *testing.T) {
	manager := &clashEgressManager{}
	manager.recentRotations.Store(int64(9), time.Now().Add(-time.Minute))
	require.False(t, manager.ConsumeRecentRotation(9))
}
