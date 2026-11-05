# Urban Loom — Replenishment Engine Rules

This document describes how the Urban Loom replenishment engine decides, every morning, whether a
store gets more stock of a product. It is the reference your agent uses to explain a decision.

One **product** is one barcode (EAN): a style, in one colour, in one size.
The engine looks at every product a store carries and makes one decision for it.

## Inputs the engine reads

| Input | Meaning |
|---|---|
| Store status | `Open`, `Hold` or `Closed` |
| Store grade | `A` (best), `B` or `C` |
| Depth target | How many units of a size the store should hold. Depends on grade and on whether the size is pivotal |
| Stock on hand | Units on the store floor at the start of the day |
| In transit | Units already dispatched to the store but not yet received |
| Serving warehouse | `WH-SOUTH` serves South stores, `WH-NORTH` serves everyone else |
| Warehouse available | Units free in the store's serving warehouse |
| Style holds | Styles blocked from replenishment, with a reason |
| Clearance list | End-of-season styles being sold off |
| Store capacity | Maximum units a store can hold per category |

## Depth targets

| Grade | Pivotal size | Non-pivotal size |
|---|---|---|
| A | 3 | 2 |
| B | 2 | 1 |
| C | 1 | 1 |

**Pivotal sizes** are the core sizes every store must keep: `M` and `L` for tops and jackets,
`32` and `34` for trousers and denim.

## Units needed

```
units needed = max(0, depth target − stock on hand − in transit)
```

## The decision — first matching rule wins

The rules are checked **in this order**. The first one that applies is the reason.

1. **Store not open.** The store's status is `Hold` or `Closed`. No stock is sent.
   Reason `STORE_NOT_OPEN`.
2. **Style on hold.** The style is on the hold list. No stock is sent. Reason `STYLE_ON_HOLD`.
3. **Depth already met.** Stock on hand is at or above the depth target. Reason `DEPTH_MET`.
4. **Covered by stock in transit.** Stock on hand plus stock in transit reaches the depth target.
   Reason `COVERED_BY_IN_TRANSIT`.
5. **Store capacity full.** The store already holds as many units of this category as its capacity
   allows. Reason `CAPACITY_FULL`.
6. **Warehouse replenishment.** The serving warehouse has stock. Stores share it **in grade order**
   — all A stores first, then B, then C (store code breaks ties). Each store takes up to what it
   needs from whatever is left.
   - The store got at least one unit → decision `REPLENISH_WH`, reason `WH_REPLENISH`.
     The quantity can be less than the need if stock ran short.
   - Stores ahead of it took everything → no stock sent, reason `WH_ALLOCATED_TO_HIGHER_GRADE`.
7. **Clearance style with an empty warehouse.** The warehouse has 0 and the style is on the
   clearance list. Clearance stock is never moved between stores or replaced. Reason
   `EOSS_NO_TRANSFER`.
8. **Inter-store transfer.** The warehouse has 0, so the engine looks for a **donor store**:
   - same **state** as the receiving store (a tax rule — never cross state lines)
   - status `Open`
   - holding at least **2 units more than its own depth target** of the same product

   The donor with the largest surplus wins (store code breaks ties). The transfer is the smaller
   of the need and the donor's surplus. Decision `IST`, reason `IST_FROM_DONOR`.
   *Simplification:* donors are not reduced as they give stock, so one donor can serve several
   stores in the same run.
9. **Replacement candidate.** No warehouse stock and no donor, and the size is **pivotal**. A
   planner must choose a substitute style. Decision `REPLACE_CANDIDATE`, reason
   `REPLACE_CANDIDATE`.
10. **No source for a non-pivotal size.** No warehouse stock and no donor, and the size is
    **non-pivotal**. Non-pivotal sizes are never replaced, so nothing happens. Reason
    `NO_SOURCE_NON_PIVOTAL`.

## Decisions and quantities

| Decision | Quantity |
|---|---|
| `REPLENISH_WH` | Units allocated from the warehouse |
| `IST` | Units transferred from the donor |
| `REPLACE_CANDIDATE` | 0 (a planner decides) |
| `NO_ACTION` | 0 |
