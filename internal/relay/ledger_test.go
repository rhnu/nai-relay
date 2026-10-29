package relay

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func ledgerSnapshot() Snapshot {
	return Snapshot{At: time.Unix(1_800_000_000, 0), Active: true, Tier: 3, ExpiresAt: 1_900_000_000, Normal: 100 * Micro, PurchasedOpus: 20 * Micro, Fixed: 1000 * Micro, PurchasedAnlas: 100 * Micro, SecondsPerPercent: 100}
}

func initializedLedger(t *testing.T, n int, reserve float64, policy string, s Snapshot) *Ledger {
	t.Helper()
	l := NewLedger(n, reserve, policy)
	if err := l.Initialize(s); err != nil {
		t.Fatal(err)
	}
	return l
}

func spend(t *testing.T, l *Ledger, user int, q Quote, id string) {
	t.Helper()
	if err := l.Reserve(user, q, id); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkSent(id); err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(id, false); err != nil {
		t.Fatal(err)
	}
}

func TestInitialAllocationPreservesBonusAndReserve(t *testing.T) {
	s := ledgerSnapshot()
	s.Normal = 120 * Micro
	l := initializedLedger(t, 2, 10, "proportional", s)
	if got := l.View(0); got.Available.Normal != 55*Micro || got.NormalCap != 45*Micro || got.Available.Fixed != 500*Micro {
		t.Fatalf("unexpected initial allocation: %+v", got)
	}
	s.Normal = 5 * Micro
	l = initializedLedger(t, 2, 10, "proportional", s)
	if err := l.Advance(s.At.Add(100 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := l.View(0).Available.Normal; got != 0 {
		t.Fatalf("regeneration spent protected reserve: %d", got)
	}
}

func TestRefillRedistributesOnlyToUsersWithRoom(t *testing.T) {
	s := ledgerSnapshot()
	l := initializedLedger(t, 2, 0, "proportional", s)
	spend(t, l, 0, Quote{Opus: 10 * Micro}, "a")
	if err := l.Advance(s.At.Add(50 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 40*Micro+Micro/2 || l.View(1).Available.Normal != 50*Micro {
		t.Fatal("fractional refill did not flow to the unfilled share")
	}
	// The observed increase acknowledges the modeled credit without granting it twice.
	s.At = s.At.Add(50 * time.Second)
	s.Normal = 90*Micro + Micro/2
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if got := l.View(0).Available.Normal; got != 40*Micro+Micro/2 {
		t.Fatalf("snapshot double credited refill: %d", got)
	}
}

func TestDuplicateIntegerSnapshotsDoNotRefundEstimatedSpend(t *testing.T) {
	s := ledgerSnapshot()
	s.SecondsPerPercent = 0
	l := initializedLedger(t, 2, 0, "proportional", s)
	spend(t, l, 0, Quote{Opus: Micro / 2, Anlas: 3 * Micro}, "a")
	for i := 0; i < 3; i++ {
		s.At = s.At.Add(time.Second)
		if err := l.Sync(s); err != nil {
			t.Fatal(err)
		}
	}
	if got := l.View(0).Available; got.Normal != 49*Micro+Micro/2 || got.Fixed != 497*Micro {
		t.Fatalf("identical snapshots refunded cost: %+v", got)
	}
	s.Normal -= Micro / 2
	s.Fixed -= 3 * Micro
	s.At = s.At.Add(time.Second)
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if l.PendingDelta[0] != 0 || l.PendingDelta[2] != 0 {
		t.Fatal("observed spend was not acknowledged")
	}
}

func TestExternalDeficitProportionalAndFreezeRecovery(t *testing.T) {
	for _, policy := range []string{"proportional", "freeze"} {
		t.Run(policy, func(t *testing.T) {
			s := ledgerSnapshot()
			s.SecondsPerPercent = 0
			l := initializedLedger(t, 2, 0, policy, s)
			spend(t, l, 0, Quote{Opus: 20 * Micro}, "a")
			s.At = s.At.Add(time.Second)
			s.Normal = 60 * Micro
			if err := l.Sync(s); err != nil {
				t.Fatal(err)
			}
			if policy == "proportional" {
				if l.View(0).Available.Normal != 225*Micro/10 || l.View(1).Available.Normal != 375*Micro/10 {
					t.Fatalf("deficit allocation: %+v %+v", l.View(0), l.View(1))
				}
			} else {
				if !l.Frozen || l.Deficits[0] != 20*Micro {
					t.Fatal("external deficit did not freeze ledger")
				}
				if err := l.Reserve(0, Quote{Opus: 1}, "b"); !errors.Is(err, ErrFrozen) {
					t.Fatalf("expected frozen error, got %v", err)
				}
				s.At = s.At.Add(time.Second)
				s.Normal = 70 * Micro
				if err := l.Sync(s); err != nil {
					t.Fatal(err)
				}
				if !l.Frozen || l.View(0).Available.Normal != 30*Micro {
					t.Fatal("partial debt repayment minted credit or unfroze ledger")
				}
				s.At = s.At.Add(time.Second)
				s.Normal = 80 * Micro
				if err := l.Sync(s); err != nil {
					t.Fatal(err)
				}
				if l.Frozen || l.View(0).Available.Normal != 30*Micro {
					t.Fatal("debt repayment did not unfreeze without new grants")
				}
			}
		})
	}
}

func TestPhysicalPoolPriorityAndMixedBoundary(t *testing.T) {
	s := ledgerSnapshot()
	s.Normal = 2 * Micro
	s.Fixed = 2 * Micro
	l := initializedLedger(t, 2, 0, "proportional", s)
	if err := l.Reserve(0, Quote{Opus: 2 * Micro}, "a"); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("borrowed other user's normal share: %v", err)
	}
	spend(t, l, 1, Quote{Opus: Micro, Anlas: Micro}, "b")
	if err := l.Reserve(0, Quote{Opus: 2 * Micro, Anlas: 2 * Micro}, "c"); err != nil {
		t.Fatal(err)
	}
	if got := l.View(0).Reserved; got.Normal != Micro || got.PurchasedOpus != Micro || got.Fixed != Micro || got.PurchasedAnlas != Micro {
		t.Fatalf("wrong mixed debit: %+v", got)
	}
	if err := l.Settle("c", true); err != nil {
		t.Fatal(err)
	}
	if got := l.View(0); got.Available.Normal != Micro || got.Available.PurchasedOpus != 10*Micro || got.Reserved.Normal != 0 {
		t.Fatalf("refund failed: %+v", got)
	}
}

func TestRestartRecoversPreparedButKeepsSentAndUnknownCharges(t *testing.T) {
	s := ledgerSnapshot()
	l := initializedLedger(t, 2, 0, "proportional", s)
	for _, id := range []string{"prepared", "sent", "unknown"} {
		if err := l.Reserve(0, Quote{Anlas: Micro}, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.MarkSent("sent"); err != nil {
		t.Fatal(err)
	}
	r := l.Pending["unknown"]
	r.State = "unknown"
	l.Pending["unknown"] = r
	data, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	restored := new(Ledger)
	if err = json.Unmarshal(data, restored); err != nil {
		t.Fatal(err)
	}
	if err = restored.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	if len(restored.Pending) != 0 || restored.View(0).Available.Fixed != 498*Micro || restored.View(0).Reserved.Fixed != 0 || restored.PendingDelta[2] != -2*Micro {
		t.Fatalf("incorrect recovery: %+v", restored)
	}
}

func TestRestartAndFreshnessNeverAccrueDowntime(t *testing.T) {
	s := ledgerSnapshot()
	l := initializedLedger(t, 2, 0, "proportional", s)
	spend(t, l, 0, Quote{Opus: 10 * Micro}, "a")
	l.AwaitingFresh = true
	s.At = s.At.Add(24 * time.Hour)
	s.Normal = 90 * Micro
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 40*Micro {
		t.Fatal("restart granted unobserved downtime refill")
	}
	if err := l.Advance(s.At.Add(24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := l.View(0).Available.Normal; got != 412*Micro/10 {
		t.Fatalf("freshness limit not applied: %d", got)
	}
	// Refresh after the stale interval must not make that skipped interval eligible later.
	s.At = s.At.Add(24 * time.Hour)
	s.Normal = 90 * Micro
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	before := l.View(0).Available.Normal
	if err := l.Advance(s.At.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != before+Micro/100 {
		t.Fatal("skipped stale interval became future credit")
	}
}

func TestPurchasesAndSubscriptionRenewal(t *testing.T) {
	s := ledgerSnapshot()
	l := initializedLedger(t, 2, 0, "proportional", s)
	spend(t, l, 0, Quote{Anlas: 100 * Micro}, "a")
	s.At = s.At.Add(time.Second)
	s.ExpiresAt += 30 * 86400
	s.Fixed = 10000 * Micro
	s.PurchasedAnlas += 10 * Micro
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if got := l.View(0).Available; got.Fixed != 5000*Micro || got.PurchasedAnlas != 55*Micro {
		t.Fatalf("renewal or purchase not allocated: %+v", got)
	}
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.PurchasedAnlas != 55*Micro {
		t.Fatal("purchase was credited twice")
	}
}

func TestReconfigureReserveAndPolicy(t *testing.T) {
	s := ledgerSnapshot()
	l := initializedLedger(t, 2, 0, "freeze", s)
	if err := l.Reconfigure(10, "proportional"); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 45*Micro {
		t.Fatal("higher reserve was not withheld")
	}
	if err := l.Reconfigure(0, "proportional"); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 50*Micro {
		t.Fatal("lower reserve did not release backing")
	}
	s.SecondsPerPercent = 0
	s.Normal = 80 * Micro
	s.At = s.At.Add(time.Second)
	l.Policy = "freeze"
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if err := l.Reconfigure(0, "proportional"); err != nil {
		t.Fatal(err)
	}
	if l.Frozen || l.View(0).Available.Normal != 40*Micro {
		t.Fatal("changed deficit policy did not reconcile retained debt")
	}
}

func TestSettlementIsIdempotentAndRetainsUnknownOutcome(t *testing.T) {
	l := initializedLedger(t, 2, 0, "proportional", ledgerSnapshot())
	if err := l.Reserve(0, Quote{Anlas: 2 * Micro}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkSent("a"); err != nil {
		t.Fatal(err)
	}
	if err := l.SettleOutcome("a", "unknown"); err != nil {
		t.Fatal(err)
	}
	if err := l.SettleOutcome("a", "unknown"); err != nil {
		t.Fatal(err)
	}
	if len(l.Settlements) != 1 || l.Settlements[0].Outcome != "unknown" || l.View(0).Available.Fixed != 498*Micro {
		t.Fatal("unknown settlement was lost or charged twice")
	}
	if err := l.SettleOutcome("a", "refunded"); err == nil {
		t.Fatal("conflicting repeated outcome was accepted")
	}
	if err := l.Reserve(0, Quote{}, "a"); err == nil {
		t.Fatal("settled reservation identifier was reused")
	}
}

func TestFreezeOnlyBlocksAffectedResource(t *testing.T) {
	s := ledgerSnapshot()
	s.SecondsPerPercent = 0
	l := initializedLedger(t, 2, 0, "freeze", s)
	s.At = s.At.Add(time.Second)
	s.Normal -= 10 * Micro
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if err := l.Reserve(0, Quote{Anlas: Micro}, "anlas"); err != nil {
		t.Fatalf("Opus deficit blocked unrelated Anlas: %v", err)
	}
	if err := l.Reserve(0, Quote{Opus: Micro}, "opus"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("Opus deficit did not block Opus: %v", err)
	}
	if !l.View(0).Spendable.Fixed || l.View(0).Spendable.Normal {
		t.Fatal("spendability flags ignore resource-specific freeze")
	}
}

func TestRefillNeverBanksRoomOccupiedByReservation(t *testing.T) {
	s := ledgerSnapshot()
	l := initializedLedger(t, 2, 0, "proportional", s)
	if err := l.Reserve(0, Quote{Opus: 10 * Micro}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkSent("a"); err != nil {
		t.Fatal(err)
	}
	if err := l.Advance(s.At.Add(50 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal+l.View(0).Reserved.Normal != 50*Micro {
		t.Fatal("held balance did not occupy refill capacity")
	}
	if err := l.Settle("a", false); err != nil {
		t.Fatal(err)
	}
	if err := l.Advance(s.At.Add(100 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 40*Micro+Micro/2 {
		t.Fatal("discarded refill became a later grant")
	}
}

func TestObservedBonusDoesNotDisappearOnRefund(t *testing.T) {
	s := ledgerSnapshot()
	s.SecondsPerPercent = 0
	l := initializedLedger(t, 2, 0, "proportional", s)
	s.At = s.At.Add(time.Second)
	s.Normal = 120 * Micro
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 60*Micro {
		t.Fatal("observed real bonus was clamped")
	}
	if err := l.Reserve(0, Quote{Opus: Micro}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := l.Settle("a", true); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 60*Micro {
		t.Fatal("refund removed bonus balance")
	}
}

func TestOverdraftRecoveryNeedsObservedNonnegativeBacking(t *testing.T) {
	s := ledgerSnapshot()
	s.Normal = 0
	s.UsageNegative = true
	l := initializedLedger(t, 2, 0, "proportional", s)
	if err := l.Advance(s.At.Add(100 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if l.View(0).Available.Normal != 0 {
		t.Fatal("boolean overdraft created spendable credit")
	}
	s.At = s.At.Add(100 * time.Second)
	s.UsageNegative = false
	if err := l.Sync(s); err != nil {
		t.Fatal(err)
	}
	if l.Physical[0] != 0 || l.View(0).Available.Normal != 0 {
		t.Fatal("unobserved overdraft recovery survived zero crossing")
	}
}
