package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/hookdeck/outpost/internal/models"
	r "github.com/redis/go-redis/v9"
)

// The write fence makes a Redis write that MB Wallet's deadline gate admitted commit before
// the gate's cut-off or not at all (Ospec add-agency-api-access, "Deadline gate"). The
// gate's TimeoutHandler only stops the response: a write queued behind a stalled Redis
// would otherwise land after the 503. With a fence on the context, every write command is
// sent inside a Lua script that reads Redis's own clock and refuses once the fence has
// passed, so the check happens where the write commits.
//
// The fence is in Redis server time. Fencer.Fence reads Redis TIME after admission and adds
// what is left of the gate's monotonic timer, measured after TIME answered, so the fence is
// never later than the cut-off. No host wall clock enters it.

// ErrFenced is the answer to a write that reached Redis after its call was cut off.
var ErrFenced = errors.New("redis write fence: the call was cut off at its request timeout; nothing was written")

type fenceKey struct{}

// fence is what a fenced call's context carries: the fence in Redis server microseconds,
// and a token naming the call's acceptance record (Acceptances).
type fence struct {
	until int64
	token string
}

// fenceOf returns the fence on ctx, in Redis server microseconds.
func fenceOf(ctx context.Context) (int64, bool) {
	f, ok := ctx.Value(fenceKey{}).(fence)
	return f.until, ok
}

// Fencer binds a call's Redis writes to its cut-off.
type Fencer struct{ Client Cmdable }

// Fence returns ctx carrying the fence for a call cut off at cutOff, which must carry a
// monotonic reading (a time.Now() plus a duration).
func (f Fencer) Fence(ctx context.Context, cutOff time.Time) (context.Context, error) {
	timeCtx, cancel := context.WithTimeout(ctx, time.Until(cutOff))
	defer cancel()
	now, err := f.Client.Time(timeCtx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis write fence: TIME: %w", err)
	}
	left := time.Until(cutOff)
	if left <= 0 {
		return nil, ErrFenced
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, fenceKey{}, fence{until: now.UnixMicro() + left.Microseconds(), token: hex.EncodeToString(token)}), nil
}

// fencedScript runs its commands only while Redis's clock is before the fence.
// ARGV[1] is the fence in microseconds; then each command as its argument count followed
// by its arguments. Each reply is returned in order, an error reply as an error element.
const fencedScript = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
if now >= tonumber(ARGV[1]) then
  return redis.error_reply('MBFENCE')
end
local replies = {}
local i = 2
while i <= #ARGV do
  local n = tonumber(ARGV[i])
  local args = {}
  for j = 1, n do args[j] = ARGV[i + j] end
  replies[#replies + 1] = redis.pcall(unpack(args))
  i = i + n + 1
end
return replies
`

// commandKind says how a command is treated under a fence: read commands pass through,
// write commands go through the fenced script with Args[1] as their key, and any command
// not listed is refused, so a write this table does not know cannot slip past the fence.
type commandKind struct {
	write   bool
	allKeys bool // every argument after the name is a key (DEL, UNLINK)
}

var fenceCommands = map[string]commandKind{
	// writes the gated API paths make (tenant store, idempotence) and their siblings
	"hset": {write: true}, "hdel": {write: true}, "hmset": {write: true}, "hsetnx": {write: true},
	"hincrby": {write: true}, "persist": {write: true}, "expire": {write: true}, "pexpire": {write: true},
	"expireat": {write: true}, "pexpireat": {write: true}, "set": {write: true}, "setex": {write: true},
	"psetex": {write: true}, "incr": {write: true}, "incrby": {write: true},
	"del": {write: true, allKeys: true}, "unlink": {write: true, allKeys: true},
	// reads
	"get": {}, "mget": {}, "hget": {}, "hmget": {}, "hgetall": {}, "hlen": {}, "hkeys": {}, "hvals": {},
	"hexists": {}, "exists": {}, "ttl": {}, "pttl": {}, "type": {}, "scan": {}, "hscan": {},
	"smembers": {}, "sismember": {}, "scard": {}, "zrange": {}, "zrangebyscore": {}, "zscore": {}, "zcard": {},
	"time": {}, "ping": {}, "echo": {}, "info": {}, "hello": {}, "auth": {}, "select": {}, "client": {},
	"ft.search": {}, "ft.aggregate": {}, "ft.info": {}, "ft._list": {},
}

func commandName(c r.Cmder) string { return strings.ToLower(c.Name()) }

type fenceHook struct{}

var _ r.Hook = fenceHook{}

func (fenceHook) DialHook(next r.DialHook) r.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (fenceHook) ProcessHook(next r.ProcessHook) r.ProcessHook {
	return func(ctx context.Context, cmd r.Cmder) error {
		until, fenced := fenceOf(ctx)
		if !fenced {
			return next(ctx, cmd)
		}
		writes, err := classify([]r.Cmder{cmd})
		if err != nil {
			return err
		}
		if !writes {
			return next(ctx, cmd)
		}
		eval := fencedEval(ctx, until, []r.Cmder{cmd})
		// go-redis sets a command's error from its hook's return, not inside next.
		if err := next(ctx, eval); err != nil {
			eval.SetErr(err)
		}
		return distribute(eval, []r.Cmder{cmd})
	}
}

func (fenceHook) ProcessPipelineHook(next r.ProcessPipelineHook) r.ProcessPipelineHook {
	return func(ctx context.Context, cmds []r.Cmder) error {
		until, fenced := fenceOf(ctx)
		if !fenced || len(cmds) == 0 {
			return next(ctx, cmds)
		}
		inner, tx := cmds, false
		if len(cmds) >= 2 && commandName(cmds[0]) == "multi" && commandName(cmds[len(cmds)-1]) == "exec" {
			inner, tx = cmds[1:len(cmds)-1], true
		}
		writes, err := classify(inner)
		if err != nil {
			return err
		}
		if !writes {
			return next(ctx, cmds)
		}
		// One script runs the whole pipeline atomically, so a transaction stays one.
		eval := fencedEval(ctx, until, inner)
		wire := []r.Cmder{eval}
		if tx {
			wire = []r.Cmder{cmds[0], eval, cmds[len(cmds)-1]}
		}
		if err := next(ctx, wire); err != nil && eval.Err() == nil {
			eval.SetErr(err)
		}
		return distribute(eval, inner)
	}
}

// classify reports whether cmds hold a write, and refuses every command (setting its error)
// when one is not in fenceCommands.
func classify(cmds []r.Cmder) (bool, error) {
	writes := false
	for _, c := range cmds {
		kind, known := fenceCommands[commandName(c)]
		if !known {
			err := fmt.Errorf("redis write fence: %q is not allowed in a gated call", commandName(c))
			for _, c := range cmds {
				c.SetErr(err)
			}
			return false, err
		}
		writes = writes || kind.write
	}
	return writes, nil
}

func fencedEval(ctx context.Context, until int64, cmds []r.Cmder) *r.Cmd {
	var keys []any
	seen := map[string]bool{}
	argv := []any{until}
	for _, c := range cmds {
		args := c.Args()
		kind := fenceCommands[commandName(c)]
		if kind.write && len(args) > 1 {
			last := 2
			if kind.allKeys {
				last = len(args)
			}
			for _, k := range args[1:last] {
				if s := fmt.Sprint(k); !seen[s] {
					seen[s] = true
					keys = append(keys, s)
				}
			}
		}
		argv = append(argv, len(args))
		argv = append(argv, args...)
	}
	evalArgs := append([]any{"eval", fencedScript, len(keys)}, keys...)
	return r.NewCmd(ctx, append(evalArgs, argv...)...)
}

// distribute hands each command its reply from the script, typed as the caller asked for.
func distribute(eval *r.Cmd, cmds []r.Cmder) error {
	vals, err := eval.Slice()
	if err == nil && len(vals) != len(cmds) {
		err = fmt.Errorf("redis write fence: %d replies for %d commands", len(vals), len(cmds))
	}
	if err != nil {
		if strings.Contains(err.Error(), "MBFENCE") {
			err = ErrFenced
		}
		for _, c := range cmds {
			c.SetErr(err)
		}
		return err
	}
	var first error
	for i, c := range cmds {
		if e := setReply(c, vals[i]); e != nil {
			c.SetErr(e)
			if first == nil {
				first = e
			}
		}
	}
	return first
}

func setReply(c r.Cmder, v any) error {
	if e, ok := v.(error); ok {
		return e
	}
	switch c := c.(type) {
	case *r.IntCmd:
		n, ok := v.(int64)
		if !ok {
			return fmt.Errorf("redis write fence: %s replied %T, want an integer", c.Name(), v)
		}
		c.SetVal(n)
	case *r.BoolCmd:
		switch x := v.(type) {
		case int64:
			c.SetVal(x == 1)
		case string:
			c.SetVal(x == "OK")
		case nil:
			c.SetVal(false)
		default:
			return fmt.Errorf("redis write fence: %s replied %T, want a boolean", c.Name(), v)
		}
	case *r.StatusCmd:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("redis write fence: %s replied %T, want a status", c.Name(), v)
		}
		c.SetVal(s)
	case *r.Cmd:
		c.SetVal(v)
	default:
		return fmt.Errorf("redis write fence: no reply mapping for %T (%s)", c, c.Name())
	}
	return nil
}

// acceptanceTTL bounds how long an acceptance record is kept: longer than any delivery
// queue backlog this service is expected to have.
const acceptanceTTL = 7 * 24 * time.Hour

// Acceptances writes and reads the acceptance records of gated publishes
// (models.Acceptance).
type Acceptances struct{ Client Cmdable }

// Pending returns the acceptance the tasks of the fenced call on ctx carry, or nil when
// ctx carries no fence.
func (Acceptances) Pending(ctx context.Context) *models.Acceptance {
	f, ok := ctx.Value(fenceKey{}).(fence)
	if !ok {
		return nil
	}
	return &models.Acceptance{Key: "mbgate:accepted:" + f.token, Fence: f.until}
}

// Accept writes the call's acceptance record. ctx carries the call's fence, so the write
// is refused once the fence has passed.
func (a Acceptances) Accept(ctx context.Context) error {
	p := a.Pending(ctx)
	if p == nil {
		return nil
	}
	return a.Client.Set(ctx, p.Key, "1", acceptanceTTL).Err()
}

// awaitScript answers whether the record exists and Redis's clock, in one atomic read.
const awaitScript = `
local t = redis.call('TIME')
return {redis.call('EXISTS', KEYS[1]), tonumber(t[1]) * 1000000 + tonumber(t[2])}
`

// Await reports whether a task's call was accepted. It waits while the record is missing
// and the fence has not passed (the publish may still be writing it), and answers false
// once Redis's clock has passed the fence with no record: from then on none can be
// written.
func (a Acceptances) Await(ctx context.Context, acc models.Acceptance) (bool, error) {
	for {
		vals, err := a.Client.Eval(ctx, awaitScript, []string{acc.Key}).Slice()
		if err != nil {
			return false, err
		}
		exists, _ := vals[0].(int64)
		now, _ := vals[1].(int64)
		if exists == 1 {
			return true, nil
		}
		if now >= acc.Fence {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
