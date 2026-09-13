package handler

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"go.uber.org/zap"
)

// TempUnscheduler 用于 HandleFailoverError 中同账号重试耗尽后的临时封禁。
// GatewayService 隐式实现此接口。
type TempUnscheduler interface {
	TempUnscheduleRetryableError(ctx context.Context, accountID int64, failoverErr *service.UpstreamFailoverError)
}

// FailoverAction 表示 failover 错误处理后的下一步动作
type FailoverAction int

const (
	// FailoverContinue 继续循环（同账号重试或切换账号，调用方统一 continue）
	FailoverContinue FailoverAction = iota
	// FailoverExhausted 切换次数耗尽（调用方应返回错误响应）
	FailoverExhausted
	// FailoverCanceled context 已取消（调用方应直接 return）
	FailoverCanceled
)

const (
	// maxSameAccountRetries 同账号重试次数默认上限（针对 RetryableOnSameAccount 错误）。
	// 生产调用方通常传入账号级配置 account.GetPoolModeRetryCount()，该常量仅作兜底/测试默认值。
	maxSameAccountRetries = 3
	// sameAccountRetryDelay 同账号重试间隔
	sameAccountRetryDelay = 500 * time.Millisecond
	// maxRequestScopedRetryDelay 限制请求级瞬时错误的指数退避上限，避免高重试配置
	// 将单次请求拖入分钟级等待。
	maxRequestScopedRetryDelay = 8 * time.Second
	// singleAccountBackoffDelay 单账号分组 503 退避重试固定延时。
	// Service 层在 SingleAccountRetry 模式下已做充分原地重试（最多 3 次、总等待 30s），
	// Handler 层只需短暂间隔后重新进入 Service 层即可。
	singleAccountBackoffDelay = 2 * time.Second
	// accountSelectionRetryAttempts 选号阶段（尚未产生任何上游请求）的重试次数上限。
	// 单账号分组下，调度器的候选终检（快照刷新 / 阈值 / 流隔离 / DB 复核预算）
	// 可以在瞬时并发下把唯一候选挡掉，错误里只留下 selection_order_exhausted。
	// 此时既没有上游状态码也没有 failover 可走，Handler 直接回 503，而 Codex
	// 不重试 503，会话就此中断。有界重试把这类瞬时窗口吸收掉；持久性错误
	// （模型不支持等）由调用方的 ModelNotFound 判定提前排除。
	accountSelectionRetryAttempts = 3
	// maxProfitVetoAttempts 单次请求内允许的分组利润门终检否决次数上限。
	// 利润否决不产生上游请求，因此不会推进 SwitchCount；没有独立上限的话，
	// 「选号 → 终检否决 → 重选」在候选池与账号快照短暂不一致时可以空转很久。
	// 取值与 maxAccountSwitches 默认值一致：混合定价的大分组仍有充分重选机会，
	// 同时把整池越线时的无谓选号开销限制在常数级。
	maxProfitVetoAttempts = 10
)

// maxModelRateLimitSelectionWait 选号阶段愿意为「模型限流冷却」原地等待的上限。
//
// 上游 429 触发的冷却窗口实测为 60s，等一次就能让请求正常跑完，远好过回一个
// Codex 不会重试的 503。但同一套 model_rate_limits 也承载 30 分钟级的
// plan-gated 冷却（upstream_400_codex_plan_gated_model），那种必须立刻失败：
// 把客户端挂在连接上半小时，比明确报错更糟。90s 落在两者之间。
const maxModelRateLimitSelectionWait = 90 * time.Second

// modelRateLimitWaitBuffer 在冷却到期时间之上多等的一点余量，避免因为
// 毫秒级的时钟/取整误差刚好卡在到期前一瞬重新选号，白白浪费一次重试。
const modelRateLimitWaitBuffer = 500 * time.Millisecond

// accountSelectionWaitFor 决定本次选号失败后应当等待多久再重选。
//
// cooldown 是调用方查到的「分组内最快恢复的账号还要等多久」：已知且在上限内时
// 按它精确等待一次；未知（0）或过长时回退到固定指数退避，由重试次数收敛。
func accountSelectionWaitFor(cooldown time.Duration, attempt int) time.Duration {
	if cooldown > 0 && cooldown <= maxModelRateLimitSelectionWait {
		return cooldown + modelRateLimitWaitBuffer
	}
	return accountSelectionRetryDelay(attempt)
}

// accountSelectionWaitForSingleAccount keeps the request on the existing
// short retry cadence when the only candidate has just returned 429. The
// model-level cooldown is useful for choosing among multiple accounts, but
// waiting for its full window here makes a one-account request outlive the
// client's retry budget.
func accountSelectionWaitForSingleAccount(attempt int) time.Duration {
	return accountSelectionRetryDelay(attempt)
}

func accountSelectionRetryDelay(attempt int) time.Duration {
	delay := 500 * time.Millisecond
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	return delay
}

// selectionTransientRateLimitPattern 匹配调度失败摘要里「非零」的限流/冷却计数。
// 摘要（summarizeSelectionFailureStats / openAISelectionFilterStats.summary）总是
// 打印全部计数，包括 model_rate_limited=0；直接对 "rate_limited" 做子串匹配会把
// 「全部候选因模型不支持而落选」这类永远重试不好的失败也拖进退避，白白多等几秒。
var selectionTransientRateLimitPattern = regexp.MustCompile(`rate_limited=([1-9][0-9]*)`)

// shouldRetryAccountSelection 判断一次「选号失败」是否值得原地退避后重选。
//
// 只覆盖瞬时的调度窗口：
//   - selection_order_exhausted：候选进入了选择序列，却在终检阶段被逐个挡掉
//     （快照刷新、调度阈值、代理流隔离、DB 复核预算）。单账号分组下这等于
//     整池瞬时不可用，几百毫秒后通常自愈。
//   - rate_limited=N（N>0）：候选确因限流冷却落选。
//
// 明确不覆盖 selection_order_empty 与裸 ErrNoAvailableAccounts：前者说明候选序列
// 本就为空，后者不带任何可判定的瞬时信号，重试只会给客户端徒增延迟。
func shouldRetryAccountSelection(err error, modelNotFound bool, attempts int) bool {
	if err == nil || modelNotFound || attempts >= accountSelectionRetryAttempts || !errors.Is(err, service.ErrNoAvailableAccounts) {
		return false
	}
	reason := strings.ToLower(err.Error())
	return strings.Contains(reason, "selection_order_exhausted") ||
		selectionTransientRateLimitPattern.MatchString(reason)
}

// profitVetoExhaustedMessage 是利润否决次数耗尽时返回给客户端的文案。
// 语义上等同于「无可用账号」：候选账号都不满足分组的利润约束。
const profitVetoExhaustedMessage = "No available accounts: all candidates rejected by group profit control"

func sameAccountRetryDelayFor(failoverErr *service.UpstreamFailoverError, retryCount int) time.Duration {
	if failoverErr == nil {
		return sameAccountRetryDelay
	}
	if failoverErr.SameAccountRetryDelay > 0 {
		return failoverErr.SameAccountRetryDelay
	}
	if !failoverErr.RequestScopedTransient || retryCount <= 1 {
		return sameAccountRetryDelay
	}

	delay := sameAccountRetryDelay
	for i := 1; i < retryCount; i++ {
		if delay >= maxRequestScopedRetryDelay/2 {
			return maxRequestScopedRetryDelay
		}
		delay *= 2
	}
	return delay
}

func sameAccountRetryAllowed(failoverErr *service.UpstreamFailoverError, retryCount, retryLimit int) bool {
	if failoverErr == nil || !failoverErr.RetryableOnSameAccount {
		return false
	}
	if !sameAccountRetryDeadlineAllows(failoverErr) {
		return false
	}
	// Error-specific caps (Grok capacity/stream-idle) remain hard limits even
	// when the error also carries a freshly reconstructed deadline.
	if failoverErr.SameAccountRetryMax > 0 {
		if retryLimit <= 0 {
			return false
		}
		if failoverErr.SameAccountRetryMax < retryLimit {
			retryLimit = failoverErr.SameAccountRetryMax
		}
		return retryCount < retryLimit
	}
	// OAuth 429 explicitly opts into a deadline window. It is intentionally not
	// bounded by the ordinary/default pool retry count.
	if !failoverErr.SameAccountRetryDeadline.IsZero() {
		return true
	}
	return retryLimit > 0 && retryCount < retryLimit
}

// lastAccountWaitRetryableStatus 判断某个上游状态码在「池内已无其它候选」时
// 是否值得原地等待后重试同一个账号。
//
// 只收瞬时不可用：429（限流窗口会过期）与 502/503/504（上游网关抖动）。
// 明确不收 500 与其它 4xx —— 它们通常由请求本身决定，等多久都一样。
func lastAccountWaitRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// shouldWaitAndRetryLastAccount 判断是否应当清空排除集、等待后重试同一个账号。
//
// 场景：分组里只有一个可调度账号（或全部候选都已落入排除集）。上游失败后
// 该账号被加进排除集，下一轮选号必然落空，Handler 随即 handleFailoverExhausted
// 把上游状态码原样抛给客户端 —— maxAccountSwitches 配了 10 也没有意义，因为
// 根本没有第二个号可切。对 Codex 这类客户端，一次 502/503 就等于会话中断。
//
// 既然没有别的号可换，正确动作是在同一个号上等一会儿再试，而不是立刻放弃。
// 上界复用 switchCount/maxSwitches：每轮失败都会推进 switchCount，因此等待轮次
// 天然收敛，不需要再引入一个独立计数器。
//
// 凭证类失败（401/403 等 GatewayFailureStageAccountAuth）不在此列：等待不会
// 让一把坏 token 变好，只会把失败延迟几十秒后再报出来。
func shouldWaitAndRetryLastAccount(failoverErr *service.UpstreamFailoverError, switchCount, maxSwitches int) bool {
	if failoverErr == nil || !failoverErr.ShouldRetryNextAccount() {
		return false
	}
	if failoverErr.IsCredentialFailure() {
		return false
	}
	if switchCount > maxSwitches {
		return false
	}
	return lastAccountWaitRetryableStatus(failoverErr.StatusCode)
}

// lastAccountWaitDelay 单账号等待重试的间隔。
// 取 singleAccountBackoffDelay 与上游给出的 SameAccountRetryDelay（如 Retry-After
// 推导值）的较大者：既尊重上游要求的冷却时间，也保证不会比 2s 更频繁地空转。
func lastAccountWaitDelay(failoverErr *service.UpstreamFailoverError) time.Duration {
	delay := singleAccountBackoffDelay
	if failoverErr != nil && failoverErr.SameAccountRetryDelay > delay {
		delay = failoverErr.SameAccountRetryDelay
	}
	return delay
}

// sameAccountRetryDeadlineAllows prevents a retry from starting after the
// service-provided same-account retry window has elapsed.
func sameAccountRetryDeadlineAllows(failoverErr *service.UpstreamFailoverError) bool {
	return failoverErr == nil || failoverErr.SameAccountRetryDeadline.IsZero() || time.Now().Before(failoverErr.SameAccountRetryDeadline)
}

// effectiveSameAccountRetryLimit applies an error-specific cap without
// overriding an explicit account setting of zero (which disables retries).
func effectiveSameAccountRetryLimit(failoverErr *service.UpstreamFailoverError, account *service.Account) int {
	if account == nil {
		return 0
	}
	limit := account.GetPoolModeRetryCount()
	if limit > 0 && failoverErr != nil && failoverErr.SameAccountRetryMax > 0 && failoverErr.SameAccountRetryMax < limit {
		return failoverErr.SameAccountRetryMax
	}
	return limit
}

// FailoverState 跨循环迭代共享的 failover 状态
type FailoverState struct {
	SwitchCount           int
	MaxSwitches           int
	FailedAccountIDs      map[int64]struct{}
	SameAccountRetryCount map[int64]int
	LastFailoverErr       *service.UpstreamFailoverError
	ForceCacheBilling     bool
	hasBoundSession       bool

	// profitVetoedAccountIDs 记录被分组利润门终检否决的账号，是 FailedAccountIDs
	// 的子集。之所以单独维护：HandleSelectionExhausted 的 503 退避分支会清空
	// FailedAccountIDs，而利润否决在同一请求内的判定不会改变（下游倍率 D 已在
	// 请求开始冻结），被清空的账号会被立即重选并再次否决，形成没有任何上游请求、
	// SwitchCount 也不前进的活锁。清空后必须把它们放回排除集。
	profitVetoedAccountIDs map[int64]struct{}
	// profitVetoCount 本次请求累计的利润否决次数，用于 maxProfitVetoAttempts 上限。
	profitVetoCount int
}

// NewFailoverState 创建 failover 状态
func NewFailoverState(maxSwitches int, hasBoundSession bool) *FailoverState {
	return &FailoverState{
		MaxSwitches:            maxSwitches,
		FailedAccountIDs:       make(map[int64]struct{}),
		SameAccountRetryCount:  make(map[int64]int),
		hasBoundSession:        hasBoundSession,
		profitVetoedAccountIDs: make(map[int64]struct{}),
	}
}

// RecordProfitVeto 记录一次分组利润门终检否决：把账号加入排除列表（同时登记到
// 利润否决集，使其不被 503 退避分支清掉）并递增否决计数。
//
// 返回 FailoverContinue 表示调用方可以继续重选下一个账号；返回 FailoverExhausted
// 表示本次请求的利润否决次数已达上限，调用方应按「无可用账号」终止，
// 不得继续 continue。
func (s *FailoverState) RecordProfitVeto(accountID int64) FailoverAction {
	s.FailedAccountIDs[accountID] = struct{}{}
	if s.profitVetoedAccountIDs == nil {
		s.profitVetoedAccountIDs = make(map[int64]struct{})
	}
	s.profitVetoedAccountIDs[accountID] = struct{}{}
	s.profitVetoCount++
	if s.profitVetoCount >= maxProfitVetoAttempts {
		return FailoverExhausted
	}
	return FailoverContinue
}

// ProfitVetoCount 返回本次请求累计的利润否决次数（供日志使用）。
func (s *FailoverState) ProfitVetoCount() int { return s.profitVetoCount }

// allExclusionsAreProfitVetoed 判断排除列表是否已全部由利润门否决贡献。
// 此时清空 FailedAccountIDs 会被原样恢复，退避重试不会带来任何新候选。
func (s *FailoverState) allExclusionsAreProfitVetoed() bool {
	if len(s.profitVetoedAccountIDs) == 0 || len(s.FailedAccountIDs) == 0 {
		return false
	}
	for id := range s.FailedAccountIDs {
		if _, ok := s.profitVetoedAccountIDs[id]; !ok {
			return false
		}
	}
	return true
}

// HandleFailoverError 处理 UpstreamFailoverError，返回下一步动作。
// 包含：缓存计费判断、同账号重试、临时封禁、切换计数、Antigravity 延时。
func (s *FailoverState) HandleFailoverError(
	ctx context.Context,
	gatewayService TempUnscheduler,
	accountID int64,
	platform string,
	retryLimit int,
	failoverErr *service.UpstreamFailoverError,
) FailoverAction {
	// 客户端已断开：failover 只会用已取消的 context 重新选号并必然失败，
	// 不应再被当成账号耗尽处理（误报 502）。
	if ctx != nil && ctx.Err() != nil {
		return FailoverCanceled
	}
	s.LastFailoverErr = failoverErr
	if failoverErr == nil || !failoverErr.ShouldRetryNextAccount() {
		return FailoverExhausted
	}

	// 同账号重试不算切换账号，粘性会话仅在实际切换时强制缓存计费。
	retryCount := s.SameAccountRetryCount[accountID]
	sameAccountRetry := sameAccountRetryAllowed(failoverErr, retryCount, retryLimit)
	if needForceCacheBilling(s.hasBoundSession, failoverErr, sameAccountRetry) {
		s.ForceCacheBilling = true
	}

	// 同账号重试：对 RetryableOnSameAccount 的临时性错误，先在同一账号上重试。
	// 重试次数上限 retryLimit 由调用方传入（账号级 pool_mode_retry_count 配置）。
	if sameAccountRetry {
		s.SameAccountRetryCount[accountID]++
		retryDelay := sameAccountRetryDelayFor(failoverErr, s.SameAccountRetryCount[accountID])
		logger.FromContext(ctx).Warn("gateway.failover_same_account_retry",
			zap.Int64("account_id", accountID),
			zap.Int("upstream_status", failoverErr.StatusCode),
			zap.Int("same_account_retry_count", s.SameAccountRetryCount[accountID]),
			zap.Int("same_account_retry_max", retryLimit),
			zap.Duration("retry_delay", retryDelay),
		)
		if !sleepWithContext(ctx, retryDelay) {
			return FailoverCanceled
		}
		return FailoverContinue
	}

	// 同账号重试用尽，执行临时封禁
	if failoverErr.RetryableOnSameAccount {
		gatewayService.TempUnscheduleRetryableError(ctx, accountID, failoverErr)
	}

	// 加入失败列表
	s.FailedAccountIDs[accountID] = struct{}{}

	// 检查是否耗尽
	if s.SwitchCount >= s.MaxSwitches {
		return FailoverExhausted
	}

	// 递增切换计数
	s.SwitchCount++
	logger.FromContext(ctx).Warn("gateway.failover_switch_account",
		zap.Int64("account_id", accountID),
		zap.Int("upstream_status", failoverErr.StatusCode),
		zap.Int("switch_count", s.SwitchCount),
		zap.Int("max_switches", s.MaxSwitches),
	)

	// Antigravity 平台换号线性递增延时
	if platform == service.PlatformAntigravity {
		delay := time.Duration(s.SwitchCount-1) * time.Second
		if !sleepWithContext(ctx, delay) {
			return FailoverCanceled
		}
	}

	return FailoverContinue
}

// HandleSelectionExhausted 处理选号失败（所有候选账号都在排除列表中）时的退避重试决策。
// 针对 Antigravity 单账号分组的 503 (MODEL_CAPACITY_EXHAUSTED) 场景：
// 清除排除列表、等待退避后重新选号。
//
// 返回 FailoverContinue 时，调用方应设置 SingleAccountRetry context 并 continue。
// 返回 FailoverExhausted 时，调用方应返回错误响应。
// 返回 FailoverCanceled 时，调用方应直接 return。
func (s *FailoverState) HandleSelectionExhausted(ctx context.Context) FailoverAction {
	// 客户端已断开时选号失败是 context canceled 的必然结果，
	// 不代表账号耗尽，直接按取消终止。
	if ctx.Err() != nil {
		return FailoverCanceled
	}

	if s.LastFailoverErr != nil &&
		s.LastFailoverErr.StatusCode == http.StatusServiceUnavailable &&
		s.SwitchCount <= s.MaxSwitches {

		// 排除列表全由利润门否决贡献时，清空后会被原样恢复：退避重试拿不到
		// 任何新候选，而利润否决不推进 SwitchCount，退避条件将永远成立。
		// 这里直接判定耗尽，避免每 2s 空转一轮的活锁。
		if s.allExclusionsAreProfitVetoed() {
			logger.FromContext(ctx).Warn("gateway.failover_selection_exhausted_by_profit_veto",
				zap.Int("profit_veto_count", s.profitVetoCount),
				zap.Int("excluded_accounts", len(s.FailedAccountIDs)),
			)
			return FailoverExhausted
		}

		logger.FromContext(ctx).Warn("gateway.failover_single_account_backoff",
			zap.Duration("backoff_delay", singleAccountBackoffDelay),
			zap.Int("switch_count", s.SwitchCount),
			zap.Int("max_switches", s.MaxSwitches),
		)
		if !sleepWithContext(ctx, singleAccountBackoffDelay) {
			return FailoverCanceled
		}
		logger.FromContext(ctx).Warn("gateway.failover_single_account_retry",
			zap.Int("switch_count", s.SwitchCount),
			zap.Int("max_switches", s.MaxSwitches),
		)
		s.FailedAccountIDs = make(map[int64]struct{})
		// 利润门否决的账号不参与退避重试的解除：判定依据（冻结的下游倍率）在
		// 同一请求内不变，放它们回池只会被再次否决。
		for id := range s.profitVetoedAccountIDs {
			s.FailedAccountIDs[id] = struct{}{}
		}
		return FailoverContinue
	}
	return FailoverExhausted
}

// needForceCacheBilling 判断 failover 时是否需要强制缓存计费。
// 粘性会话实际切换账号、或上游明确标记时，将 input_tokens 转为 cache_read 计费。
func needForceCacheBilling(hasBoundSession bool, failoverErr *service.UpstreamFailoverError, sameAccountRetry bool) bool {
	return (hasBoundSession && !sameAccountRetry) || (failoverErr != nil && failoverErr.ForceCacheBilling)
}

// failoverClientGone 判断下游客户端是否已断开（请求 context 已取消）。
// 客户端断开后 failover 必须静默终止：用已取消的 context 重新选号只会得到
// context.Canceled，并被误报成账号耗尽（通用 502）；上游 detach 的在途请求
// 照常完成计费，但不再为无人接收的响应启动新的上游尝试。
// 响应尚未提交时把状态码标记为 499（client closed request），供访问日志归类。
func failoverClientGone(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.Context().Err() == nil {
		return false
	}
	// 先停 compact 心跳（接管 ResponseWriter，建立 happens-before），与
	// handleStreamingAwareError/errorResponse 等终结路径对齐，避免心跳
	// goroutine 与下面的状态标记并发触碰同一 writer。心跳已提交 200 时
	// 状态码已固化，不再标 499。
	if service.StopOpenAICompactSSEKeepaliveCommitted(c) {
		return true
	}
	if !c.Writer.Written() {
		c.Status(statusClientClosedRequest)
	}
	return true
}

// sleepWithContext 等待指定时长，返回 false 表示 context 已取消。
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
