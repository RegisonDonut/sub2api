package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type modelRateLimitWaitRepo struct {
	AccountRepository
	accounts []Account
}

func (r *modelRateLimitWaitRepo) ListSchedulableByGroupIDAndPlatform(_ context.Context, _ int64, platform string) ([]Account, error) {
	out := make([]Account, 0, len(r.accounts))
	for _, a := range r.accounts {
		if a.Platform == platform {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *modelRateLimitWaitRepo) ListSchedulableByPlatform(ctx context.Context, platform string) ([]Account, error) {
	return r.ListSchedulableByGroupIDAndPlatform(ctx, 0, platform)
}

// 单账号分组下，冷却剩余时间是「该不该等、等多久」的唯一依据。算错方向的代价
// 不对称：少等一点就回 503 把 Codex 会话打断，多等一点只是延迟。
func TestShortestOpenAIModelRateLimitWait(t *testing.T) {
	const model = "gpt-5.6-sol"
	groupID := int64(3)

	newAccount := func(id int64, cooldown time.Duration) Account {
		a := Account{
			ID: id, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Status: StatusActive, Schedulable: true, Concurrency: 1,
		}
		if cooldown > 0 {
			setAccountModelRateLimitSnapshot(&a, model, time.Now().Add(cooldown), "upstream 429", time.Now())
		}
		return a
	}

	svc := func(accounts ...Account) *OpenAIGatewayService {
		return &OpenAIGatewayService{accountRepo: &modelRateLimitWaitRepo{accounts: accounts}}
	}

	t.Run("returns the soonest recovery across the group", func(t *testing.T) {
		got := svc(newAccount(1, 5*time.Minute), newAccount(2, 45*time.Second)).
			ShortestOpenAIModelRateLimitWait(context.Background(), &groupID, PlatformOpenAI, model)
		require.Greater(t, got, 40*time.Second)
		require.LessOrEqual(t, got, 45*time.Second)
	})

	t.Run("zero when nothing is cooling down", func(t *testing.T) {
		// 账号落选另有原因，等待解决不了问题 —— 必须回 0 让调用方走固定退避，
		// 否则会把「模型不支持」这类永久失败也变成漫长等待。
		got := svc(newAccount(1, 0)).
			ShortestOpenAIModelRateLimitWait(context.Background(), &groupID, PlatformOpenAI, model)
		require.Zero(t, got)
	})

	t.Run("ignores accounts that cannot serve the model anyway", func(t *testing.T) {
		unschedulable := newAccount(1, 30*time.Second)
		unschedulable.Schedulable = false
		got := svc(unschedulable).
			ShortestOpenAIModelRateLimitWait(context.Background(), &groupID, PlatformOpenAI, model)
		require.Zero(t, got)
	})

	t.Run("zero for an empty model", func(t *testing.T) {
		got := svc(newAccount(1, 30*time.Second)).
			ShortestOpenAIModelRateLimitWait(context.Background(), &groupID, PlatformOpenAI, "")
		require.Zero(t, got)
	})
}
