package compaction

import "fmt"

// CostModel prices R2 operations. The defaults are illustrative published
// list prices (USD) and are surfaced as an *approximation*, not a bill:
//
//   - Class A operations (PUT/COPY/LIST): $4.50 per million
//   - Class B operations (GET/HEAD): $0.36 per million
//   - Storage: $0.015 per GB-month
//   - Egress: $0 within Cloudflare; not modelled.
//
// The compactor's own traffic is dominated by one Class A PUT plus a small
// number of Class B verify/list operations per segment.
type CostModel struct {
	ClassAPerMillion float64
	ClassBPerMillion float64
	StoragePerGBMon  float64
}

// DefaultCostModel returns the published list prices used by the trials.
func DefaultCostModel() CostModel {
	return CostModel{
		ClassAPerMillion: 4.50,
		ClassBPerMillion: 0.36,
		StoragePerGBMon:  0.015,
	}
}

// Cost is an approximate USD cost.
type Cost struct {
	ClassAOps      int64   `json:"class_a_ops"`
	ClassBOps      int64   `json:"class_b_ops"`
	StorageBytes   int64   `json:"storage_bytes"`
	ClassACost     float64 `json:"class_a_cost_usd"`
	ClassBCost     float64 `json:"class_b_cost_usd"`
	StorageCostMon float64 `json:"storage_cost_usd_per_month"`
	TotalPerRun    float64 `json:"total_cost_usd_per_run"`
}

// EstimateCost converts operation counts into an approximate cost.
func (m CostModel) EstimateCost(calls StoreCalls, storedBytes int64) Cost {
	classA := calls.Put + calls.Delete + calls.List
	classB := calls.Get + calls.Head
	c := Cost{
		ClassAOps:    classA,
		ClassBOps:    classB,
		StorageBytes: storedBytes,
	}
	c.ClassACost = m.ClassAPerMillion * float64(classA) / 1_000_000
	c.ClassBCost = m.ClassBPerMillion * float64(classB) / 1_000_000
	c.StorageCostMon = m.StoragePerGBMon * float64(storedBytes) / (1024 * 1024 * 1024)
	c.TotalPerRun = c.ClassACost + c.ClassBCost
	return c
}

// String renders a compact, human-readable cost line.
func (c Cost) String() string {
	return fmt.Sprintf("R2 approx: %d ClassA ($%.6f) + %d ClassB ($%.6f) = $%.6f/run; storage %.3f GiB ($%.6f/mo)",
		c.ClassAOps, c.ClassACost, c.ClassBOps, c.ClassBCost, c.TotalPerRun,
		float64(c.StorageBytes)/(1024*1024*1024), c.StorageCostMon)
}
