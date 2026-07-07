package limiter

import (
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestSpeedTracker_UpdateBuckets(t *testing.T) {
	l := New()
	st := NewSpeedTracker(l)

	users := []model.UserSpec{
		{ID: 1, UUID: "u1", SpeedLimit: 3, DeviceLimit: 0},  // 3 Mbps
		{ID: 2, UUID: "u2", SpeedLimit: 0, DeviceLimit: 0},  // unlimited
		{ID: 3, UUID: "u3", SpeedLimit: 10, DeviceLimit: 2}, // 10 Mbps
	}
	l.UpdateUsers(users)
	st.UpdateBuckets()

	// User 1: 3 Mbps = 375000 bytes/sec
	b1 := st.GetLimiter("u1")
	if b1 == nil {
		t.Fatal("expected bucket for user 1")
	}
	expectedRate := float64(3 * 1000000 / 8)
	if float64(b1.Limit()) != expectedRate {
		t.Errorf("user 1 rate: got %v, want %v", float64(b1.Limit()), expectedRate)
	}
	// Burst should be max(bytesPerSec, 64KB) = max(375000, 65536) = 375000
	expectedBurst := 3 * 1000000 / 8
	if b1.Burst() != expectedBurst {
		t.Errorf("user 1 burst: got %v, want %v", b1.Burst(), expectedBurst)
	}

	// User 2: no speed limit, no bucket
	b2 := st.GetLimiter("u2")
	if b2 != nil {
		t.Fatal("expected no bucket for user 2")
	}

	// User 3: 10 Mbps = 1250000 bytes/sec
	b3 := st.GetLimiter("u3")
	if b3 == nil {
		t.Fatal("expected bucket for user 3")
	}
	expectedRate3 := float64(10 * 1000000 / 8)
	if float64(b3.Limit()) != expectedRate3 {
		t.Errorf("user 3 rate: got %v, want %v", float64(b3.Limit()), expectedRate3)
	}
	// Burst should be max(1250000, 64KB) = 1250000
	expectedBurst3 := 10 * 1000000 / 8
	if b3.Burst() != expectedBurst3 {
		t.Errorf("user 3 burst: got %v, want %v", b3.Burst(), expectedBurst3)
	}
}

func TestSpeedTracker_UpdateBuckets_PreservesExisting(t *testing.T) {
	l := New()
	st := NewSpeedTracker(l)

	users := []model.UserSpec{
		{ID: 1, UUID: "u1", SpeedLimit: 3, DeviceLimit: 0},
	}
	l.UpdateUsers(users)
	st.UpdateBuckets()

	b1 := st.GetLimiter("u1")
	if b1 == nil {
		t.Fatal("expected bucket for user 1")
	}

	// Update same user with different speed — should reuse same *rate.Limiter pointer
	users2 := []model.UserSpec{
		{ID: 1, UUID: "u1", SpeedLimit: 5, DeviceLimit: 0},
	}
	l.UpdateUsers(users2)
	st.UpdateBuckets()

	b1After := st.GetLimiter("u1")
	if b1After == nil {
		t.Fatal("expected bucket for user 1 after update")
	}
	if b1 != b1After {
		t.Error("expected same rate.Limiter instance to be reused")
	}

	// Rate should be updated
	expectedRate := float64(5 * 1000000 / 8)
	if float64(b1After.Limit()) != expectedRate {
		t.Errorf("user 1 updated rate: got %v, want %v", float64(b1After.Limit()), expectedRate)
	}
}

func TestSpeedTracker_UpdateBuckets_RemovesUsers(t *testing.T) {
	l := New()
	st := NewSpeedTracker(l)

	users := []model.UserSpec{
		{ID: 1, UUID: "u1", SpeedLimit: 3, DeviceLimit: 0},
		{ID: 2, UUID: "u2", SpeedLimit: 5, DeviceLimit: 0},
	}
	l.UpdateUsers(users)
	st.UpdateBuckets()

	if st.GetLimiter("u1") == nil || st.GetLimiter("u2") == nil {
		t.Fatal("expected buckets for both users")
	}

	// Remove user 2
	users2 := []model.UserSpec{
		{ID: 1, UUID: "u1", SpeedLimit: 3, DeviceLimit: 0},
	}
	l.UpdateUsers(users2)
	st.UpdateBuckets()

	if st.GetLimiter("u1") == nil {
		t.Fatal("expected bucket for user 1")
	}
	if st.GetLimiter("u2") != nil {
		t.Error("expected no bucket for removed user 2")
	}
}

// Regression: log callback must not run while UpdateBuckets holds t.mu.Lock,
// otherwise callbacks that call LimitedUserCount() self-deadlock on RWMutex.
func TestSpeedTracker_UpdateBuckets_LogCallbackMayCallLimitedUserCount(t *testing.T) {
	l := New()
	st := NewSpeedTracker(l)
	l.UpdateUsers([]model.UserSpec{{ID: 1, UUID: "u1", SpeedLimit: 1}})
	st.SetLogCallback(func(msg string) {
		_ = st.LimitedUserCount()
	})

	done := make(chan struct{})
	go func() {
		st.UpdateBuckets()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: UpdateBuckets did not finish (log callback vs RWMutex)")
	}
}

func TestSpeedTracker_DynamicSpeedLimitTriggersAndRecovers(t *testing.T) {
	l := New()
	st := NewSpeedTracker(l)
	l.UpdateUsers([]model.UserSpec{{
		ID:         1,
		UUID:       "u1",
		SpeedLimit: 0,
		DynamicSpeedLimit: &model.DynamicSpeedPolicy{
			Enabled:         true,
			ThresholdMbps:   10,
			TriggerSeconds:  20,
			LimitMbps:       2,
			RecoverySeconds: 20,
		},
	}})
	st.UpdateBuckets()

	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	// 20 Mbps for two 10s samples crosses the 20s trigger window.
	heavy10s := map[int][2]int64{1: {0, 25_000_000}}
	st.ObserveTraffic(heavy10s, 10*time.Second, now)
	if lim := st.GetLimiter("u1"); lim != nil {
		t.Fatal("dynamic limiter should not trigger before trigger_seconds")
	}
	st.ObserveTraffic(heavy10s, 10*time.Second, now.Add(10*time.Second))
	lim := st.GetLimiter("u1")
	if lim == nil {
		t.Fatal("expected dynamic limiter after sustained high bandwidth")
	}
	if got, want := float64(lim.Limit()), float64(2*1_000_000/8); got != want {
		t.Fatalf("dynamic limiter rate = %v, want %v", got, want)
	}

	idle10s := map[int][2]int64{1: {0, 1}}
	st.ObserveTraffic(idle10s, 10*time.Second, now.Add(20*time.Second))
	if st.GetLimiter("u1") == nil {
		t.Fatal("dynamic limiter should remain during recovery window")
	}
	st.ObserveTraffic(idle10s, 10*time.Second, now.Add(30*time.Second))
	if st.GetLimiter("u1") != nil {
		t.Fatal("dynamic limiter should recover after low bandwidth recovery window")
	}
}

func TestSpeedTracker_DynamicSpeedLimitRespectsTimeRanges(t *testing.T) {
	l := New()
	st := NewSpeedTracker(l)
	l.UpdateUsers([]model.UserSpec{{
		ID:   1,
		UUID: "u1",
		DynamicSpeedLimit: &model.DynamicSpeedPolicy{
			Enabled:        true,
			ThresholdMbps:  1,
			TriggerSeconds: 10,
			LimitMbps:      1,
			TimeRanges:     []model.TimeRange{{Start: "23:00", End: "23:59"}},
		},
	}})
	st.UpdateBuckets()

	st.ObserveTraffic(map[int][2]int64{1: {0, 10_000_000}}, 10*time.Second, time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC))
	if st.GetLimiter("u1") != nil {
		t.Fatal("dynamic limiter should not trigger outside policy time range")
	}
}
