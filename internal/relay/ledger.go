package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"
)

const Micro int64 = 1_000_000

var (
	ErrInsufficientFunds = errors.New("insufficient quota")
	ErrFrozen            = errors.New("quota ledger frozen after upstream deficit")
)

// Snapshot contains upstream balances in millionths of their native units.
type Snapshot struct {
	At                time.Time                  `json:"at"`
	Active            bool                       `json:"active"`
	Tier              int                        `json:"tier"`
	ExpiresAt         int64                      `json:"expires_at"`
	Normal            int64                      `json:"normal"`
	PurchasedOpus     int64                      `json:"purchased_opus"`
	Fixed             int64                      `json:"fixed"`
	PurchasedAnlas    int64                      `json:"purchased_anlas"`
	SecondsPerPercent int64                      `json:"seconds_per_percent"`
	UsageNegative     bool                       `json:"usage_negative,omitempty"`
	Raw               map[string]json.RawMessage `json:"raw,omitempty"`
}

type Quote struct{ Opus, Anlas int64 }

type PoolBalances struct {
	Normal         int64 `json:"normal"`
	PurchasedOpus  int64 `json:"purchased_opus"`
	Fixed          int64 `json:"fixed"`
	PurchasedAnlas int64 `json:"purchased_anlas"`
}

type QuotaView struct {
	User                  int              `json:"user"`
	Available             PoolBalances     `json:"available"`
	Reserved              PoolBalances     `json:"reserved"`
	NormalCap             int64            `json:"normal_cap"`
	FixedCap              int64            `json:"fixed_cap"`
	Frozen                bool             `json:"frozen"`
	Initialized           bool             `json:"initialized"`
	RefillAt              time.Time        `json:"refill_at"`
	Snapshot              Snapshot         `json:"snapshot"`
	NormalRefillPerSecond float64          `json:"normal_refill_per_second"`
	Spendable             PoolSpendability `json:"spendable"`
}

type PoolSpendability struct {
	Normal         bool `json:"normal"`
	PurchasedOpus  bool `json:"purchased_opus"`
	Fixed          bool `json:"fixed"`
	PurchasedAnlas bool `json:"purchased_anlas"`
}

// Settlement is a bounded audit record; it carries no request body or credentials.
type Settlement struct {
	ID      string       `json:"id"`
	User    int          `json:"user"`
	Amounts PoolBalances `json:"amounts"`
	At      time.Time    `json:"at"`
	Outcome string       `json:"outcome"`
}

type quotaUser struct{ Available, Held [4]int64 }
type reservation struct {
	User    int
	Amounts [4]int64
	State   string
}

// Ledger is serialized by the server mutex. All arithmetic uses fixed-point units.
type Ledger struct {
	Users           []quotaUser
	ReservePercent  float64
	Policy          string
	Initialized     bool
	Frozen          bool
	Latest          Snapshot
	LastRefill      time.Time
	RefillRemainder int64
	Physical        [4]int64
	// PendingDelta tracks estimated changes not yet reflected by a changed snapshot.
	// Keeping it across identical observations prevents rounded balances refunding spend.
	PendingDelta  [4]int64
	Deficits      [4]int64
	Pending       map[string]reservation
	Settlements   []Settlement
	AwaitingFresh bool          `json:"-"`
	FreshWindow   time.Duration `json:"-"`
}

func NewLedger(n int, reserve float64, policy string) *Ledger {
	if n < 0 {
		n = 0
	}
	if policy == "" {
		policy = "proportional"
	}
	return &Ledger{Users: make([]quotaUser, n), ReservePercent: reserve, Policy: policy, Pending: make(map[string]reservation)}
}

func (l *Ledger) validate() error {
	if len(l.Users) == 0 {
		return errors.New("ledger requires at least one user")
	}
	if math.IsNaN(l.ReservePercent) || math.IsInf(l.ReservePercent, 0) || l.ReservePercent < 0 || l.ReservePercent >= 100 {
		return errors.New("reserve must be in [0,100)")
	}
	if l.Policy != "proportional" && l.Policy != "freeze" {
		return fmt.Errorf("unknown deficit policy %q", l.Policy)
	}
	return nil
}

func validateSnapshot(s Snapshot) error {
	if s.At.IsZero() || s.Normal < 0 || s.PurchasedOpus < 0 || s.Fixed < 0 || s.PurchasedAnlas < 0 || s.SecondsPerPercent < 0 {
		return errors.New("invalid upstream snapshot")
	}
	return nil
}

func snapshotPools(s Snapshot) [4]int64 {
	return [4]int64{s.Normal, s.PurchasedOpus, s.Fixed, s.PurchasedAnlas}
}
func poolView(v [4]int64) PoolBalances  { return PoolBalances{v[0], v[1], v[2], v[3]} }
func (l *Ledger) reserveUnits() int64   { return int64(math.Round(l.ReservePercent * float64(Micro))) }
func (l *Ledger) normalCapacity() int64 { return (100*Micro - l.reserveUnits()) / int64(len(l.Users)) }
func tierCapacity(tier int) int64 {
	switch tier {
	case 1, 2:
		return 1000 * Micro
	case 3:
		return 10000 * Micro
	default:
		return 0
	}
}
func (l *Ledger) fixedCapacity() int64 { return tierCapacity(l.Latest.Tier) / int64(len(l.Users)) }

func cloneSnapshot(s Snapshot) Snapshot {
	if s.Raw != nil {
		raw := make(map[string]json.RawMessage, len(s.Raw))
		for k, v := range s.Raw {
			raw[k] = append(json.RawMessage(nil), v...)
		}
		s.Raw = raw
	}
	return s
}

func (l *Ledger) Current() Snapshot { return cloneSnapshot(l.Latest) }

func (l *Ledger) Initialize(s Snapshot) error {
	if err := l.validate(); err != nil {
		return err
	}
	if l.Initialized {
		return errors.New("ledger already initialized")
	}
	if err := validateSnapshot(s); err != nil {
		return err
	}
	l.Latest = cloneSnapshot(s)
	l.LastRefill = s.At
	l.Physical = snapshotPools(s)
	l.Initialized = true
	for p, total := range l.Physical {
		if p == 0 {
			total = max(int64(0), total-l.reserveUnits())
		}
		l.distribute(p, total)
	}
	return nil
}

// distribute assigns an observed purchase once, including indivisible remainder units.
func (l *Ledger) distribute(pool int, amount int64) {
	if amount <= 0 {
		return
	}
	n := int64(len(l.Users))
	each, rem := amount/n, amount%n
	for i := range l.Users {
		l.Users[i].Available[pool] += each
		if int64(i) < rem {
			l.Users[i].Available[pool]++
		}
	}
}

// fill shares replenishment among users with room, without storing discarded overflow.
func (l *Ledger) fill(pool int, amount, cap int64) int64 {
	credited := int64(0)
	for amount > 0 {
		eligible := make([]int, 0, len(l.Users))
		for i, u := range l.Users {
			if u.Available[pool]+u.Held[pool] < cap {
				eligible = append(eligible, i)
			}
		}
		if len(eligible) == 0 {
			break
		}
		each, rem := amount/int64(len(eligible)), amount%int64(len(eligible))
		used := int64(0)
		for j, i := range eligible {
			add := each
			if int64(j) < rem {
				add++
			}
			room := cap - l.Users[i].Available[pool] - l.Users[i].Held[pool]
			add = min(add, room)
			l.Users[i].Available[pool] += add
			used += add
		}
		if used == 0 {
			break
		}
		amount -= used
		credited += used
	}
	return credited
}

func (l *Ledger) normalGrantLimit(amount int64) int64 {
	liability, held := int64(0), int64(0)
	for _, u := range l.Users {
		liability += u.Available[0] + u.Held[0]
		held += u.Held[0]
	}
	return min(amount, max(int64(0), l.Physical[0]+held-l.reserveUnits()-liability))
}

// Advance models fractional regeneration only while the server considers Latest fresh.
func (l *Ledger) Advance(at time.Time) error {
	if !l.Initialized {
		return errors.New("ledger not initialized")
	}
	if l.AwaitingFresh {
		return nil
	}
	// The server can impose a shorter freshness window. Never extrapolate an old
	// observation indefinitely, including across a long idle period.
	freshWindow := l.FreshWindow
	if freshWindow <= 0 {
		freshWindow = 120 * time.Second
	}
	if limit := l.Latest.At.Add(freshWindow); at.After(limit) {
		at = limit
	}
	if !at.After(l.LastRefill) {
		return nil
	}
	start, end := l.LastRefill, at
	l.LastRefill = at
	if !l.Latest.Active || l.Latest.Tier != 3 || l.Latest.SecondsPerPercent <= 0 {
		l.RefillRemainder = 0
		return nil
	}
	if l.Latest.ExpiresAt > 0 {
		exp := time.Unix(l.Latest.ExpiresAt, 0)
		if !start.Before(exp) {
			l.RefillRemainder = 0
			return nil
		}
		if end.After(exp) {
			end = exp
		}
	}
	// One micro-percent takes SecondsPerPercent * 1000 nanoseconds.
	denom := new(big.Int).Mul(big.NewInt(l.Latest.SecondsPerPercent), big.NewInt(1000))
	num := new(big.Int).Add(big.NewInt(end.Sub(start).Nanoseconds()), big.NewInt(l.RefillRemainder))
	quot, rem := new(big.Int), new(big.Int)
	quot.QuoRem(num, denom, rem)
	amount := int64(100 * Micro)
	if quot.IsInt64() {
		amount = min(amount, quot.Int64())
	}
	amount = min(amount, max(int64(0), 100*Micro-l.Physical[0]))
	// At the physical ceiling, past time cannot become future quota credit.
	l.RefillRemainder = 0
	if l.Physical[0]+amount < 100*Micro && rem.IsInt64() {
		l.RefillRemainder = rem.Int64()
	}
	l.Physical[0] += amount
	l.PendingDelta[0] += amount
	grant := l.payDeficit(0, amount)
	// A boolean overdraft marker does not expose the distance to zero. Retain
	// estimated recovery, but wait for actual nonnegative backing before crediting users.
	if !l.Latest.UsageNegative {
		l.fill(0, l.normalGrantLimit(grant), l.normalCapacity())
	}
	return nil
}

func (l *Ledger) Sync(s Snapshot) error {
	if !l.Initialized {
		return l.Initialize(s)
	}
	if err := validateSnapshot(s); err != nil {
		return err
	}
	if s.At.Before(l.Latest.At) {
		return errors.New("upstream snapshot is older than current snapshot")
	}
	if l.AwaitingFresh {
		l.LastRefill = s.At
		l.RefillRemainder = 0
		l.AwaitingFresh = false
	} else if err := l.Advance(s.At); err != nil {
		return err
	}
	if s.At.After(l.LastRefill) {
		l.LastRefill = s.At
		l.RefillRemainder = 0
	}
	old, newPools := snapshotPools(l.Latest), snapshotPools(s)
	clearedOverdraft := l.Latest.UsageNegative && !s.UsageNegative
	renewed := s.Active && s.ExpiresAt > l.Latest.ExpiresAt && l.Latest.ExpiresAt > 0
	l.Latest = cloneSnapshot(s)
	if clearedOverdraft {
		held := int64(0)
		for _, u := range l.Users {
			held += u.Held[0]
		}
		l.Physical[0] = max(int64(0), s.Normal-held)
		l.PendingDelta[0] = 0
	}
	for p, value := range newPools {
		if p == 2 && renewed {
			continue
		}
		delta := value - old[p]
		if delta == 0 {
			continue
		}
		if delta < 0 && l.PendingDelta[p] < 0 {
			ack := min(-delta, -l.PendingDelta[p])
			delta += ack
			l.PendingDelta[p] += ack
		}
		if delta > 0 && l.PendingDelta[p] > 0 {
			ack := min(delta, l.PendingDelta[p])
			delta -= ack
			l.PendingDelta[p] -= ack
		}
		if delta < 0 {
			l.deficit(p, -delta)
			l.Physical[p] = max(int64(0), l.Physical[p]+delta)
		}
		if delta > 0 {
			previous := l.Physical[p]
			if p != 0 || !clearedOverdraft {
				l.Physical[p] += delta
			}
			grant := l.payDeficit(p, delta)
			switch p {
			case 0:
				grant = l.normalGrantLimit(grant)
				bonus := min(grant, max(int64(0), l.Physical[p]-100*Micro)-max(int64(0), previous-100*Micro))
				l.fill(p, grant-bonus, l.normalCapacity())
				l.distribute(p, bonus)
			case 2:
				l.fill(p, grant, l.fixedCapacity())
			default:
				l.distribute(p, grant)
			}
		}
	}
	if renewed {
		// A changed subscription period is the upstream event that replenishes fixed Anlas.
		// Use observed backing, so a renewal cannot mint funds absent upstream.
		l.renewFixed(newPools[2])
	}
	return nil
}

func (l *Ledger) renewFixed(backing int64) {
	total := int64(0)
	for _, u := range l.Users {
		total += u.Available[2] + u.Held[2]
	}
	held := int64(0)
	for _, u := range l.Users {
		held += u.Held[2]
	}
	// A new subscription grant replaces the previous period's estimate. Pending
	// reservations still occupy capacity and remain withheld from physical funds.
	l.Physical[2] = max(int64(0), backing-held)
	l.PendingDelta[2] = 0
	l.Deficits[2] = max(int64(0), total-backing)
	l.fill(2, max(int64(0), backing-total), l.fixedCapacity())
	l.updateFrozen()
}

func (l *Ledger) updateFrozen() {
	l.Frozen = false
	for _, debt := range l.Deficits {
		if debt > 0 {
			l.Frozen = true
		}
	}
}

func (l *Ledger) payDeficit(pool int, amount int64) int64 {
	paid := min(amount, l.Deficits[pool])
	l.Deficits[pool] -= paid
	l.updateFrozen()
	return amount - paid
}

func (l *Ledger) deficit(pool int, amount int64) {
	if l.Policy == "freeze" {
		l.Deficits[pool] += amount
		l.updateFrozen()
		return
	}
	total := int64(0)
	for _, u := range l.Users {
		total += u.Available[pool]
	}
	if amount > total {
		l.Deficits[pool] += amount - total
		l.updateFrozen()
	}
	if total == 0 {
		return
	}
	amount = min(amount, total)
	type part struct {
		user      int
		remainder *big.Int
	}
	parts := make([]part, 0, len(l.Users))
	taken := int64(0)
	for i := range l.Users {
		num := new(big.Int).Mul(big.NewInt(l.Users[i].Available[pool]), big.NewInt(amount))
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(num, big.NewInt(total), r)
		l.Users[i].Available[pool] -= q.Int64()
		taken += q.Int64()
		parts = append(parts, part{i, r})
	}
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].remainder.Cmp(parts[j].remainder) > 0 })
	for _, p := range parts {
		if taken == amount {
			break
		}
		if l.Users[p.user].Available[pool] > 0 {
			l.Users[p.user].Available[pool]--
			taken++
		}
	}
}

// Reserve follows upstream pool priority and never spends a different user's share.
func (l *Ledger) Reserve(user int, q Quote, id string) error {
	if !l.Initialized {
		return errors.New("ledger not initialized")
	}
	if (q.Opus > 0 && (l.Deficits[0] > 0 || l.Deficits[1] > 0)) || (q.Anlas > 0 && (l.Deficits[2] > 0 || l.Deficits[3] > 0)) {
		return ErrFrozen
	}
	if user < 0 || user >= len(l.Users) || q.Opus < 0 || q.Anlas < 0 || id == "" {
		return errors.New("invalid reservation")
	}
	if _, ok := l.Pending[id]; ok {
		return errors.New("duplicate reservation id")
	}
	for _, done := range l.Settlements {
		if done.ID == id {
			return errors.New("reservation id already settled")
		}
	}
	amounts := [4]int64{}
	for _, pair := range []struct {
		cost               int64
		primary, secondary int
	}{{q.Opus, 0, 1}, {q.Anlas, 2, 3}} {
		first := min(pair.cost, l.Physical[pair.primary])
		second := pair.cost - first
		if first > l.Users[user].Available[pair.primary] || second > l.Users[user].Available[pair.secondary] || second > l.Physical[pair.secondary] {
			return ErrInsufficientFunds
		}
		amounts[pair.primary] = first
		amounts[pair.secondary] = second
	}
	for p, a := range amounts {
		l.Users[user].Available[p] -= a
		l.Users[user].Held[p] += a
		l.Physical[p] -= a
	}
	l.Pending[id] = reservation{User: user, Amounts: amounts, State: "prepared"}
	return nil
}

func (l *Ledger) MarkSent(id string) error {
	r, ok := l.Pending[id]
	if !ok {
		return errors.New("unknown reservation")
	}
	if r.State != "sent" {
		for p, a := range r.Amounts {
			l.PendingDelta[p] -= a
		}
		r.State = "sent"
		l.Pending[id] = r
	}
	return nil
}

func (l *Ledger) Settle(id string, refund bool) error {
	outcome := "success"
	if refund {
		outcome = "refunded"
	}
	return l.SettleOutcome(id, outcome)
}

// SettleOutcome releases holds exactly once and retains transport uncertainty.
func (l *Ledger) SettleOutcome(id, outcome string) error {
	if outcome != "success" && outcome != "unknown" && outcome != "refunded" {
		return errors.New("invalid settlement outcome")
	}
	for _, done := range l.Settlements {
		if done.ID == id {
			if done.Outcome == outcome {
				return nil
			}
			return errors.New("reservation already settled with a different outcome")
		}
	}
	r, ok := l.Pending[id]
	if !ok {
		return errors.New("unknown reservation")
	}
	refund := outcome == "refunded"
	if !refund && r.State != "sent" {
		if err := l.MarkSent(id); err != nil {
			return err
		}
		r = l.Pending[id]
	}
	for p, a := range r.Amounts {
		l.Users[r.User].Held[p] -= a
		if refund {
			l.Users[r.User].Available[p] += a
			l.Physical[p] += a
			if r.State != "prepared" {
				l.PendingDelta[p] += a
			}
		}
	}
	delete(l.Pending, id)
	l.Settlements = append(l.Settlements, Settlement{ID: id, User: r.User, Amounts: poolView(r.Amounts), At: time.Now().UTC(), Outcome: outcome})
	if len(l.Settlements) > 1000 {
		l.Settlements = append([]Settlement(nil), l.Settlements[len(l.Settlements)-1000:]...)
	}
	return nil
}

// RecoverPending refunds unsent work; any request that may have reached upstream stays charged.
func (l *Ledger) RecoverPending() error {
	for id, r := range l.Pending {
		outcome := "unknown"
		if r.State == "prepared" {
			outcome = "refunded"
		}
		if err := l.SettleOutcome(id, outcome); err != nil {
			return err
		}
	}
	return nil
}

// Reconfigure changes allocation rules without resetting the balances or purchase history.
func (l *Ledger) Reconfigure(reserve float64, policy string) error {
	if policy == "" {
		policy = "proportional"
	}
	oldReserve, oldPolicy := l.ReservePercent, l.Policy
	l.ReservePercent, l.Policy = reserve, policy
	if err := l.validate(); err != nil {
		l.ReservePercent, l.Policy = oldReserve, oldPolicy
		return err
	}
	if !l.Initialized {
		return nil
	}
	if policy == "proportional" {
		for p, debt := range l.Deficits {
			l.Deficits[p] = 0
			l.deficit(p, debt)
		}
	}
	if reserve != oldReserve {
		liability, held := int64(0), int64(0)
		for _, u := range l.Users {
			liability += u.Available[0] + u.Held[0]
			held += u.Held[0]
		}
		backing := max(int64(0), l.Physical[0]+held-l.reserveUnits())
		if liability > backing {
			l.Deficits[0] = 0
			l.deficit(0, liability-backing)
		} else {
			l.Deficits[0] = 0
			// Only release the changed reserve. Previously discarded regeneration
			// must not reappear merely because configuration was edited.
			oldUnits := int64(math.Round(oldReserve * float64(Micro)))
			l.distribute(0, min(backing-liability, max(int64(0), oldUnits-l.reserveUnits())))
		}
	}
	l.updateFrozen()
	return nil
}

func (l *Ledger) View(user int) QuotaView {
	v := QuotaView{User: user, Frozen: l.Frozen, Initialized: l.Initialized, RefillAt: l.LastRefill, Snapshot: l.Current()}
	if len(l.Users) > 0 {
		v.NormalCap = l.normalCapacity()
		v.FixedCap = l.fixedCapacity()
	}
	if user >= 0 && user < len(l.Users) {
		v.Available = poolView(l.Users[user].Available)
		v.Reserved = poolView(l.Users[user].Held)
		if l.Initialized && !l.AwaitingFresh {
			opusFrozen := l.Deficits[0] > 0 || l.Deficits[1] > 0
			anlasFrozen := l.Deficits[2] > 0 || l.Deficits[3] > 0
			v.Spendable = PoolSpendability{
				Normal:         !opusFrozen && !l.Latest.UsageNegative && l.Physical[0] > 0 && v.Available.Normal > 0,
				PurchasedOpus:  !opusFrozen && !l.Latest.UsageNegative && l.Physical[0] == 0 && l.Physical[1] > 0 && v.Available.PurchasedOpus > 0,
				Fixed:          !anlasFrozen && l.Physical[2] > 0 && v.Available.Fixed > 0,
				PurchasedAnlas: !anlasFrozen && l.Physical[2] == 0 && l.Physical[3] > 0 && v.Available.PurchasedAnlas > 0,
			}
			if !opusFrozen && l.Latest.Active && l.Latest.Tier == 3 && !l.Latest.UsageNegative && l.Latest.SecondsPerPercent > 0 && v.Available.Normal+v.Reserved.Normal < v.NormalCap && (l.Latest.ExpiresAt == 0 || l.LastRefill.Before(time.Unix(l.Latest.ExpiresAt, 0))) {
				eligible := 0
				for _, u := range l.Users {
					if u.Available[0]+u.Held[0] < v.NormalCap {
						eligible++
					}
				}
				v.NormalRefillPerSecond = 1 / (float64(l.Latest.SecondsPerPercent) * float64(eligible))
			}
		}
	}
	return v
}
