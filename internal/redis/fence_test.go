package redis_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hookdeck/outpost/internal/redis"
	"github.com/hookdeck/outpost/internal/util/testinfra"
	"github.com/hookdeck/outpost/internal/util/testutil"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// The write fence against a real Redis: under a live fence writes behave as without one,
// once the fence has passed at the moment Redis runs the write nothing is written, and a
// write the fence does not know is refused. internal/services proves the same through the
// gated API with Redis paused.
func TestWriteFence(t *testing.T) {
	testutil.CheckIntegrationTest(t)
	cfg := testinfra.NewRedisStackConfig(t)
	ctx := context.Background()
	client, err := redis.New(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	raw := goredis.NewClient(&goredis.Options{Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)})
	t.Cleanup(func() { raw.Close() })
	fencer := redis.Fencer{Client: client}

	t.Run("under a live fence writes land with their usual replies", func(t *testing.T) {
		fctx, err := fencer.Fence(ctx, time.Now().Add(5*time.Second))
		require.NoError(t, err)
		require.Equal(t, int64(2), client.HSet(fctx, "live:h", "a", "1", "b", "2").Val())
		require.Equal(t, int64(0), client.HSet(fctx, "live:h", "a", "3").Val())
		require.True(t, client.SetNX(fctx, "live:k", "processing", 5*time.Second).Val())
		got := client.SetNX(fctx, "live:k", "processing", 5*time.Second)
		require.NoError(t, got.Err())
		require.False(t, got.Val())
		require.True(t, client.Expire(fctx, "live:h", time.Hour).Val())
		require.False(t, client.Expire(fctx, "live:missing", time.Hour).Val())
		require.Equal(t, "OK", client.Set(fctx, "live:s", "processed", time.Hour).Val())
		_, err = client.TxPipelined(fctx, func(p goredis.Pipeliner) error {
			p.HSet(fctx, "live:t", "x", "1")
			p.HDel(fctx, "live:h", "b")
			p.Persist(fctx, "live:h")
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), client.Del(fctx, "live:s").Val())
		require.Equal(t, map[string]string{"a": "3"}, raw.HGetAll(ctx, "live:h").Val())
		require.Equal(t, "1", raw.HGet(ctx, "live:t", "x").Val())
		require.Equal(t, time.Duration(-1), raw.TTL(ctx, "live:h").Val())
	})

	t.Run("a write that reaches Redis after the fence writes nothing", func(t *testing.T) {
		fctx, err := fencer.Fence(ctx, time.Now().Add(50*time.Millisecond))
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		require.ErrorIs(t, client.HSet(fctx, "late:h", "a", "1").Err(), redis.ErrFenced)
		require.ErrorIs(t, client.SetNX(fctx, "late:k", "processing", time.Second).Err(), redis.ErrFenced)
		_, err = client.TxPipelined(fctx, func(p goredis.Pipeliner) error {
			p.HSet(fctx, "late:t", "x", "1")
			p.Expire(fctx, "late:t", time.Hour)
			return nil
		})
		require.ErrorIs(t, err, redis.ErrFenced)
		require.Equal(t, int64(0), raw.Exists(ctx, "late:h", "late:k", "late:t").Val())
	})

	t.Run("a write the fence does not know is refused", func(t *testing.T) {
		require.NoError(t, raw.Set(ctx, "unknown:a", "1", 0).Err())
		fctx, err := fencer.Fence(ctx, time.Now().Add(5*time.Second))
		require.NoError(t, err)
		require.Error(t, client.Rename(fctx, "unknown:a", "unknown:b").Err())
		require.Equal(t, int64(1), raw.Exists(ctx, "unknown:a").Val())
	})

	t.Run("a paused Redis releases the caller at its context deadline", func(t *testing.T) {
		require.NoError(t, raw.Do(ctx, "CLIENT", "PAUSE", 1500, "WRITE").Err())
		tctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		fctx, err := fencer.Fence(tctx, time.Now().Add(200*time.Millisecond))
		require.NoError(t, err)
		start := time.Now()
		err = client.HSet(fctx, "paused:h", "a", "1").Err()
		require.True(t, errors.Is(err, context.DeadlineExceeded) || err != nil, "err %v", err)
		require.Less(t, time.Since(start), 700*time.Millisecond, "the write held its caller while Redis was paused")
	})
}
