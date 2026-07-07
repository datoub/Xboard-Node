package limiter

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
	"golang.org/x/time/rate"
)

// SpeedTrackerLogCallback is called when bucket updates occur.
type SpeedTrackerLogCallback func(msg string)

type dynamicSpeedState struct {
	overFor   time.Duration
	underFor  time.Duration
	limited   bool
	lastMbps  float64
	updatedAt time.Time
}

// SpeedTracker manages per-user token-bucket rate limiters.
// It does NOT wrap connections itself — instead, ConnTracker consults it
// via GetLimiter to embed rate limiting in the same tracked connection
// wrapper that does byte counting.
type SpeedTracker struct {
	limiter *Limiter
	mu      sync.RWMutex
	buckets map[int]*rate.Limiter // userID -> shared rate limiter
	uuidMap map[string]int        // UUID -> userID
	users   map[int]model.UserSpec
	dynamic map[int]*dynamicSpeedState

	// Fast-path: when no users have a speed limit, GetLimiter returns nil
	// immediately without any map lookup.
	hasLimits atomic.Bool

	// Optional callback for logging.
	logFunc SpeedTrackerLogCallback
}

// NewSpeedTracker creates a bucket manager for per-user bandwidth throttling.
func NewSpeedTracker(l *Limiter) *SpeedTracker {
	return &SpeedTracker{
		limiter: l,
		buckets: make(map[int]*rate.Limiter),
		uuidMap: make(map[string]int),
		users:   make(map[int]model.UserSpec),
		dynamic: make(map[int]*dynamicSpeedState),
	}
}

// SetLogCallback sets the logging callback.
func (t *SpeedTracker) SetLogCallback(f SpeedTrackerLogCallback) {
	t.logFunc = f
}

// UpdateBuckets updates the UUID->userID mapping and syncs existing limiters.
func (t *SpeedTracker) UpdateBuckets() {
	currentUsers := make([]model.UserSpec, 0, 32)
	t.limiter.mu.RLock()
	for _, u := range t.limiter.users {
		currentUsers = append(currentUsers, u)
	}
	t.limiter.mu.RUnlock()

	func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		newUUIDMap := make(map[string]int, len(currentUsers))
		newUsers := make(map[int]model.UserSpec, len(currentUsers))
		activeIDs := make(map[int]struct{}, len(currentUsers))

		for _, user := range currentUsers {
			activeIDs[user.ID] = struct{}{}
			newUsers[user.ID] = user
			if user.UUID != "" {
				newUUIDMap[user.UUID] = user.ID
			}

			if lim, ok := t.buckets[user.ID]; ok {
				if mbps := t.effectiveLimitLocked(user); mbps > 0 {
					setLimiterRate(lim, mbps)
				} else {
					delete(t.buckets, user.ID)
				}
			}
		}

		for id := range t.buckets {
			if _, ok := activeIDs[id]; !ok {
				delete(t.buckets, id)
				delete(t.dynamic, id)
			}
		}
		for id := range t.dynamic {
			if _, ok := activeIDs[id]; !ok {
				delete(t.dynamic, id)
			}
		}

		t.uuidMap = newUUIDMap
		t.users = newUsers
		t.hasLimits.Store(len(t.buckets) > 0)
	}()

	if t.logFunc != nil {
		t.logFunc("buckets updated")
	}
}

// GetLimiter returns the rate limiter for the given user UUID, or nil if no
// limit applies. Creates limiter on-demand if needed. Thread-safe.
func (t *SpeedTracker) GetLimiter(user string) *rate.Limiter {
	t.mu.RLock()
	uid, exists := t.uuidMap[user]
	if !exists {
		t.mu.RUnlock()
		return nil
	}
	if lim, ok := t.buckets[uid]; ok {
		t.mu.RUnlock()
		return lim
	}
	u, userExists := t.users[uid]
	effective := 0
	if userExists {
		effective = t.effectiveLimitLocked(u)
	}
	t.mu.RUnlock()

	if !userExists || effective <= 0 {
		return nil
	}

	return t.getOrCreateLimiter(uid, effective)
}

func (t *SpeedTracker) getOrCreateLimiter(uid, mbps int) *rate.Limiter {
	lim := newRateLimiter(mbps)

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.buckets[uid]; ok {
		return existing
	}
	t.buckets[uid] = lim
	t.hasLimits.Store(true)
	return lim
}

// ObserveTraffic updates dynamic speed-limit states from the latest per-user
// traffic deltas. interval is the sampling period that produced the deltas.
func (t *SpeedTracker) ObserveTraffic(delta map[int][2]int64, interval time.Duration, now time.Time) {
	if interval <= 0 || len(delta) == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for uid, d := range delta {
		user, ok := t.users[uid]
		if !ok || user.DynamicSpeedLimit == nil || !user.DynamicSpeedLimit.Enabled {
			continue
		}
		policy := user.DynamicSpeedLimit
		if !policyActive(policy, now) || policy.ThresholdMbps <= 0 || policy.TriggerSeconds <= 0 || policy.LimitMbps <= 0 {
			t.clearDynamicLimitLocked(uid)
			continue
		}

		state := t.dynamic[uid]
		if state == nil {
			state = &dynamicSpeedState{}
			t.dynamic[uid] = state
		}

		bytes := d[0] + d[1]
		mbps := float64(bytes*8) / interval.Seconds() / 1_000_000
		state.lastMbps = mbps
		state.updatedAt = now

		if mbps >= float64(policy.ThresholdMbps) {
			state.overFor += interval
			state.underFor = 0
			if state.overFor >= time.Duration(policy.TriggerSeconds)*time.Second {
				state.limited = true
			}
		} else {
			state.overFor = 0
			if state.limited {
				state.underFor += interval
				recovery := policy.RecoverySeconds
				if recovery <= 0 {
					recovery = policy.TriggerSeconds
				}
				if state.underFor >= time.Duration(recovery)*time.Second {
					state.limited = false
					state.underFor = 0
				}
			}
		}

		if lim, ok := t.buckets[uid]; ok {
			if mbps := t.effectiveLimitLocked(user); mbps > 0 {
				setLimiterRate(lim, mbps)
			} else {
				delete(t.buckets, uid)
			}
		}
	}

	t.hasLimits.Store(len(t.buckets) > 0)
}

func (t *SpeedTracker) effectiveLimitLocked(user model.UserSpec) int {
	effective := user.SpeedLimit
	if state := t.dynamic[user.ID]; state != nil && state.limited && user.DynamicSpeedLimit != nil && user.DynamicSpeedLimit.LimitMbps > 0 {
		if effective <= 0 || user.DynamicSpeedLimit.LimitMbps < effective {
			effective = user.DynamicSpeedLimit.LimitMbps
		}
	}
	return effective
}

func (t *SpeedTracker) clearDynamicLimitLocked(uid int) {
	if state := t.dynamic[uid]; state != nil {
		state.limited = false
		state.overFor = 0
		state.underFor = 0
	}
}

func newRateLimiter(mbps int) *rate.Limiter {
	bytesPerSec := mbps * 1_000_000 / 8
	burst := bytesPerSec
	if burst < 64*1024 {
		burst = 64 * 1024
	}
	if cap4s := bytesPerSec * 4; cap4s > 64*1024 && burst > cap4s {
		burst = cap4s
	}
	return rate.NewLimiter(rate.Limit(bytesPerSec), burst)
}

func setLimiterRate(lim *rate.Limiter, mbps int) {
	next := newRateLimiter(mbps)
	lim.SetLimit(next.Limit())
	lim.SetBurst(next.Burst())
}

func policyActive(policy *model.DynamicSpeedPolicy, now time.Time) bool {
	if policy == nil || len(policy.TimeRanges) == 0 {
		return true
	}
	cur := now.Format("15:04")
	for _, r := range policy.TimeRanges {
		if r.Start == "" || r.End == "" {
			continue
		}
		if r.Start <= r.End {
			if cur >= r.Start && cur <= r.End {
				return true
			}
		} else if cur >= r.Start || cur <= r.End {
			return true
		}
	}
	return false
}

// HasLimits returns true if any user currently has a speed limit configured.
func (t *SpeedTracker) HasLimits() bool {
	return t.hasLimits.Load()
}

// LimitedUserCount returns the number of users with active speed limiters.
func (t *SpeedTracker) LimitedUserCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.buckets)
}

func (t *SpeedTracker) DynamicLimitedUserCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var count int
	for _, state := range t.dynamic {
		if state.limited {
			count++
		}
	}
	return count
}
