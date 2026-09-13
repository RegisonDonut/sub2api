package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 单账号分组下，选号失败的错误串是排查 503 的唯一线索。终检阶段原本一律静默
// continue，错误里只剩一句 selection_order_exhausted —— 说不出是哪一道检查挡的。
// 这几个用例锁住终检归因，防止再退化成静默丢弃。
func TestFinishLoadBalanceSelectionFallbackAttributesOrderDrops(t *testing.T) {
	newScheduler := func(snapshot, latest *Account) *defaultOpenAIAccountScheduler {
		snapshotByID := map[int64]*Account{}
		if snapshot != nil {
			snapshotByID[snapshot.ID] = snapshot
		}
		latestByID := map[int64]*Account{}
		if latest != nil {
			latestByID[latest.ID] = latest
		}
		return &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{
			accountRepo:        &upstreamCostCountingAccountRepo{accounts: latestByID},
			schedulerSnapshot:  &SchedulerSnapshotService{cache: &openAISnapshotCacheStub{accountsByID: snapshotByID}},
			concurrencyService: NewConcurrencyService(&upstreamCostTrackingConcurrencyCache{}),
		}}
	}

	candidate := func(account *Account) []openAIAccountCandidateScore {
		return []openAIAccountCandidateScore{{
			account:   account,
			loadInfo:  &AccountLoadInfo{AccountID: account.ID},
			loadKnown: false,
		}}
	}

	t.Run("db recheck rejection is attributed", func(t *testing.T) {
		// 快照里账号还是可调度的，权威 DB 里已经停用：候选进得了选择序列，
		// 但过不了 DB 复核。这是「pool=1 却没有任何 filtered 计数」的典型来源。
		stale := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1}
		latest := *stale
		latest.Status = StatusDisabled

		scheduler := newScheduler(stale, &latest)
		_, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
			context.Background(),
			OpenAIAccountScheduleRequest{Platform: PlatformOpenAI},
			openAIAccountLoadSelectionAttempt{selectionOrder: candidate(stale)},
			newOpenAISelectionProbeBudget(),
			openAISelectionFilterStats{pool: 1},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrNoAvailableAccounts)
		require.Contains(t, err.Error(), "order_drop: db_recheck_rejected=1")
		require.Contains(t, err.Error(), "order_examined=1")
		require.Contains(t, err.Error(), "selection_order_exhausted")
	})

	t.Run("ineligible refreshed candidate carries the concrete reason", func(t *testing.T) {
		// 候选进入选择序列时用的是旧快照，重新解析拿到的新快照已不可调度：
		// 账号在本次选号过程中被并发停用。这类否决在终检阶段发生，
		// 初筛的 filtered 计数里看不到。
		//
		// 关键是必须落到具体原因（not_schedulable）而不是笼统的 not_eligible ——
		// 后者等于把已经算好的诊断信息丢掉。
		stale := &Account{ID: 9, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1}
		refreshed := *stale
		refreshed.Status = StatusDisabled

		scheduler := newScheduler(&refreshed, stale)
		_, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
			context.Background(),
			OpenAIAccountScheduleRequest{Platform: PlatformOpenAI},
			openAIAccountLoadSelectionAttempt{selectionOrder: candidate(stale)},
			newOpenAISelectionProbeBudget(),
			openAISelectionFilterStats{pool: 1},
		)

		require.Error(t, err)
		require.Contains(t, err.Error(), "order_drop: not_schedulable=1")
		require.Contains(t, err.Error(), "order_examined=1")
		require.NotContains(t, err.Error(), "not_eligible")
	})

	t.Run("model rate limit cooldown is attributed", func(t *testing.T) {
		// 生产实际命中的原因：上游 429 给该模型打了冷却窗口，账号本身
		// status/schedulable 都正常，所以初筛放行、终检拦下。
		const model = "gpt-5.6-sol"
		limited := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1}
		setAccountModelRateLimitSnapshot(limited, model, time.Now().Add(time.Minute), "upstream 429", time.Now())

		scheduler := newScheduler(limited, limited)
		_, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
			context.Background(),
			OpenAIAccountScheduleRequest{Platform: PlatformOpenAI, RequestedModel: model},
			openAIAccountLoadSelectionAttempt{selectionOrder: candidate(limited)},
			newOpenAISelectionProbeBudget(),
			openAISelectionFilterStats{pool: 1},
		)

		require.Error(t, err)
		require.Contains(t, err.Error(), "order_drop: model_rate_limited=1")
	})

	t.Run("empty selection order stays distinguishable", func(t *testing.T) {
		// 候选序列本就为空与「终检把候选全否了」是两种不同的故障，
		// 不能都塌缩成 selection_order_exhausted。
		scheduler := newScheduler(nil, nil)
		_, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
			context.Background(),
			OpenAIAccountScheduleRequest{Platform: PlatformOpenAI},
			openAIAccountLoadSelectionAttempt{},
			newOpenAISelectionProbeBudget(),
			openAISelectionFilterStats{pool: 1},
		)

		require.Error(t, err)
		require.Contains(t, err.Error(), "selection_order_empty")
		require.NotContains(t, err.Error(), "order_drop")
		require.NotContains(t, err.Error(), "order_examined")
	})
}
