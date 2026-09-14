package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const (
	clashEgressRequestTimeout      = 4 * time.Second
	defaultClashManagedProxyPrefix = "clash-openai-slot-"
	defaultClashSelectorPrefix     = "sub2api-openai-slot-"
)

var errNoManagedClashEgressProxy = errors.New("no active managed Clash egress proxy is available")
var errClashEgressUnavailable = errors.New("Clash egress is enabled but its controller configuration is unavailable")

var clashSlotPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type clashEgressManager struct {
	controllerURL      string
	secret             string
	managedProxyPrefix string
	selectorPrefix     string
	client             *http.Client
	locks              sync.Map
	recentRotations    sync.Map
}

type clashProxyState struct {
	Type  string   `json:"type"`
	Now   string   `json:"now"`
	All   []string `json:"all"`
	Alive *bool    `json:"alive,omitempty"`
}

type clashProxiesResponse struct {
	Proxies map[string]clashProxyState `json:"proxies"`
}

func newClashEgressManager(cfg *config.Config) *clashEgressManager {
	if cfg == nil || !cfg.ClashEgress.Enabled {
		return nil
	}
	controllerURL := strings.TrimRight(strings.TrimSpace(cfg.ClashEgress.ControllerURL), "/")
	secret := strings.TrimSpace(cfg.ClashEgress.Secret)
	if secret == "" && strings.TrimSpace(cfg.ClashEgress.SecretFile) != "" {
		blob, err := os.ReadFile(strings.TrimSpace(cfg.ClashEgress.SecretFile))
		if err != nil {
			slog.Warn("clash_egress_secret_file_failed", "error", err)
			return nil
		}
		secret = strings.TrimSpace(string(blob))
	}
	if controllerURL == "" || secret == "" {
		return nil
	}
	parsed, err := url.Parse(controllerURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil
	}
	managedProxyPrefix := strings.TrimSpace(cfg.ClashEgress.ManagedProxyPrefix)
	if managedProxyPrefix == "" {
		managedProxyPrefix = defaultClashManagedProxyPrefix
	}
	selectorPrefix := strings.TrimSpace(cfg.ClashEgress.SelectorPrefix)
	if selectorPrefix == "" {
		selectorPrefix = defaultClashSelectorPrefix
	}
	return &clashEgressManager{
		controllerURL:      controllerURL,
		secret:             secret,
		managedProxyPrefix: managedProxyPrefix,
		selectorPrefix:     selectorPrefix,
		client:             &http.Client{Timeout: clashEgressRequestTimeout},
	}
}

func (m *clashEgressManager) selectorForAccount(account *Account) (string, bool) {
	if m == nil || account == nil || account.Proxy == nil {
		return "", false
	}
	slot := strings.TrimPrefix(account.Proxy.Name, m.managedProxyPrefix)
	if slot == account.Proxy.Name || slot == "" || !clashSlotPattern.MatchString(slot) {
		return "", false
	}
	return m.selectorPrefix + slot, true
}

func (m *clashEgressManager) RotateAccount(ctx context.Context, account *Account) bool {
	selector, ok := m.selectorForAccount(account)
	if !ok {
		return false
	}
	lockValue, _ := m.locks.LoadOrStore(selector, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	states, err := m.listProxies(ctx)
	if err != nil {
		slog.Warn("clash_egress_list_failed", "selector", selector, "error", err)
		return false
	}
	target, ok := states[selector]
	if !ok || len(target.All) < 2 {
		return false
	}
	used := make(map[string]struct{})
	for name, state := range states {
		if strings.HasPrefix(name, m.selectorPrefix) && name != selector && state.Now != "" {
			used[state.Now] = struct{}{}
		}
	}
	next := nextClashEgressNode(target.All, target.Now, used, states)
	if next == "" {
		next = nextClashEgressNode(target.All, target.Now, nil, states)
	}
	if next == "" {
		return false
	}
	if err := m.selectProxy(ctx, selector, next); err != nil {
		slog.Warn("clash_egress_select_failed", "selector", selector, "error", err)
		return false
	}
	m.recentRotations.Store(account.ID, time.Now())
	return true
}

func nextClashEgressNode(all []string, current string, used map[string]struct{}, states map[string]clashProxyState) string {
	if len(all) == 0 {
		return ""
	}
	start := 0
	for i, name := range all {
		if name == current {
			start = i + 1
			break
		}
	}
	for offset := 0; offset < len(all); offset++ {
		candidate := strings.TrimSpace(all[(start+offset)%len(all)])
		if candidate == "" || candidate == current || candidate == "DIRECT" || candidate == "REJECT" {
			continue
		}
		if _, exists := used[candidate]; exists {
			continue
		}
		if state, exists := states[candidate]; exists && state.Alive != nil && !*state.Alive {
			continue
		}
		return candidate
	}
	return ""
}

// assignManagedProxy gives a newly-created OpenAI account the least-used active
// Clash slot. Stable ID ordering keeps concurrent-independent assignments
// deterministic; once all slots are occupied, counts naturally allow sharing.
func (m *clashEgressManager) assignManagedProxy(ctx context.Context, proxyRepo ProxyRepository, account *Account) error {
	if m == nil || proxyRepo == nil || account == nil || account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth || account.ProxyID != nil {
		return nil
	}
	proxies, err := proxyRepo.ListActiveWithAccountCount(ctx)
	if err != nil {
		return err
	}
	candidate := leastUsedManagedProxy(proxies, m.managedProxyPrefix)
	if candidate == nil {
		return errNoManagedClashEgressProxy
	}
	proxyID := candidate.ID
	account.ProxyID = &proxyID
	account.Proxy = &candidate.Proxy
	return nil
}

func leastUsedManagedProxy(proxies []ProxyWithAccountCount, prefix string) *ProxyWithAccountCount {
	candidates := make([]ProxyWithAccountCount, 0, len(proxies))
	for _, proxy := range proxies {
		if strings.HasPrefix(proxy.Name, prefix) {
			candidates = append(candidates, proxy)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].AccountCount != candidates[j].AccountCount {
			return candidates[i].AccountCount < candidates[j].AccountCount
		}
		return candidates[i].ID < candidates[j].ID
	})
	return &candidates[0]
}

func (m *clashEgressManager) ConsumeRecentRotation(accountID int64) bool {
	if m == nil {
		return false
	}
	value, ok := m.recentRotations.LoadAndDelete(accountID)
	if !ok {
		return false
	}
	rotatedAt, ok := value.(time.Time)
	return ok && time.Since(rotatedAt) <= 30*time.Second
}

func (m *clashEgressManager) listProxies(ctx context.Context) (map[string]clashProxyState, error) {
	var payload clashProxiesResponse
	if err := m.doJSON(ctx, http.MethodGet, "/proxies", nil, &payload); err != nil {
		return nil, err
	}
	return payload.Proxies, nil
}

func (m *clashEgressManager) selectProxy(ctx context.Context, selector, node string) error {
	return m.doJSON(ctx, http.MethodPut, "/proxies/"+url.PathEscape(selector), map[string]string{"name": node}, nil)
}

func (m *clashEgressManager) doJSON(ctx context.Context, method, path string, body any, dst any) error {
	var reader io.Reader
	if body != nil {
		blob, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(blob)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.controllerURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+m.secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return &clashControllerError{status: resp.StatusCode}
	}
	if dst == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(dst)
}

type clashControllerError struct{ status int }

func (e *clashControllerError) Error() string { return http.StatusText(e.status) }
