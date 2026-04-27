# Leader Lagging Screening Upgrade Technical Specification

- status: implemented V2 technical contract
- as_of: 2026-04-22
- scope: implemented leader/follower V2 runtime, additive source context, and diagnostics

## Code Surface

Implementation remains scoped to `screeners/leader_lagging/`, `screeners/signals/source_registry.py`, and regression tests. Signal engine BUY/SELL public contracts are unchanged.

`market-intel-core` remains canonical owner for broader regime/group truth. This repo owns tactical leader/follower overlay and additive source context.

## Source Policy Enum

Source policy vocabulary:

- `adopt`
- `adapt`
- `watchlist-only`
- `reference-only`
- `excluded-existing-screener`
- `excluded_existing_screener` as the storage-safe equivalent if enum names cannot contain hyphens.

## Excluded Existing Screener Mapping

| Family | Policy | Local ownership | Allowed carryover |
| --- | --- | --- | --- |
| Mark Minervini | `excluded-existing-screener` | existing Mark Minervini subsystem | Generic RS, 52-week high, and structure terms only. |
| Weinstein / Weinstain | `excluded-existing-screener` | existing Weinstein Stage2 subsystem | Generic benchmark-relative RS line only; no Mansfield field. |
| Mansfield | `excluded-existing-screener` | treated as Weinstein-family direct source | Generic `benchmark_relative_strength` and `rs_slope` only. |
| Qullamaggie | `excluded-existing-screener` | existing Qullamaggie subsystem | Generic momentum/structure concepts only. |

## Additive Field Contract

### Leader Fields

| Field | Meaning | Source lineage |
| --- | --- | --- |
| `weighted_rs_score` | 3/6/9/12 month weighted performance | RS Rating / IBD-style adapted logic. |
| `rs_rank_true` | Same-market universe percentile | Local cross-sectional rank. |
| `rs_rank_proxy` | Historical/Pine/IBD-style proxy rank | TradingView RS Rating caveat. |
| `rs_line` | `close / benchmark_close` | Generic RS line. |
| `rs_line_slope` | Short-window slope of RS line | Generic RS line. |
| `rs_new_high_before_price` | RS line high before price high | IBD/O'Neil and RS screener lineage. |
| `rs_quality_score` | rank + RS line slope + RS high quality | composite. |
| `leadership_freshness_score` | early RS leadership and group improvement | RS New High Before Price. |
| `momentum_persistence_score` | 3-12 month momentum persistence | Jegadeesh-Titman. |
| `near_high_leadership_score` | 52-week high proximity | George-Hwang. |
| `hidden_rs_score` | benchmark down-day resilience | Hidden RS adapted logic. |
| `leader_rs_state` | `rising`, `stable`, `fading`, `weakening`, `unknown` | RS-state model. |
| `fading_risk_score` | declining RS/structure risk | RS-state logic. |
| `structure_readiness_score` | box/support readiness | Darvas/pivotal breakout. |
| `breakout_confirmation_score` | close above structure with volume | Darvas/pivotal breakout. |
| `extension_risk_score` | late chase risk | generic extension logic. |

### Follower Fields

| Field | Meaning | Source lineage |
| --- | --- | --- |
| `peer_link_score` | same group/industry/economic peer linkage | group metadata and lead-lag evidence. |
| `peer_lead_score` | lagged leader/follower return relation | Hou, Lo-MacKinlay. |
| `underreaction_score` | catch-up room after leader move | PEAD / limited attention. |
| `propagation_ratio` | follower move divided by leader move | lead-lag pair profile. |
| `rs_inflection_score` | follower RS rank/slope turn | generic RS line. |
| `structure_preservation_score` | no breakdown, box/support intact | Darvas/pivotal structure. |
| `sympathy_freshness_score` | event/catch-up window without overextension | PEAD / DellaVigna-Pollet. |

## Source Registry Fields

| Field | Example |
| --- | --- |
| `source_id` | `academic:hou-lead-lag` |
| `source_family` | `academic`, `github`, `pine`, `trader-method`, `leader-attribute-report`, `excluded-existing-screener` |
| `source_policy` | `adopt`, `adapt`, `watchlist-only`, `reference-only`, `excluded-existing-screener` |
| `source_url` | `https://ssl.pstatic.net/imgstock/upload/research/invest/1539740471781.pdf` |
| `logic_extracted` | `prior leaders rarely repeat immediately` |
| `algorithm_target` | `reference-only/deferred` |
| `as_of_date` | `2026-04-22` |
| `confidence_level` | `high`, `medium`, `low` |

## Source Registry Mapping

| Source | Policy | Algorithm target |
| --- | --- | --- |
| IBD/O'Neil RS line | `adapt` | `rs_new_high_before_price`, `leadership_freshness_score`. |
| RS Rating 1-99 / RS Screener repos | `adapt` | `weighted_rs_score`, `rs_rank_proxy`, `rs_rank_true` reference design. |
| Jegadeesh-Titman | `adopt` | `momentum_persistence_score`. |
| George-Hwang | `adopt` | `near_high_leadership_score`. |
| Hou / Lo-MacKinlay | `adopt` | `peer_lead_score`, `lead_lag_profile`. |
| Bernard-Thomas / DellaVigna-Pollet | `adapt` | `underreaction_score`, `sympathy_freshness_score`. |
| Hidden RS | `adapt` | `hidden_rs_score`. |
| RS state | `adapt` | `leader_rs_state`, `fading_risk_score`. |
| Darvas / pivotal breakout | `adapt` | `structure_readiness_score`, `breakout_confirmation_score`. |
| Macrend Invest `주도주의 생로병사` | `watchlist-only` | reference-only/deferred; no runtime score field in V2. |
| Mark Minervini / Weinstein / Weinstain / Mansfield / Qullamaggie | `excluded-existing-screener` | exclusion metadata only. |

## Compatibility Rules

- Do not change public BUY/SELL filenames.
- Do not change today-only behavior.
- Do not make follower labels automatic BUY.
- Do not expose raw source URLs in public BUY/SELL rows.
- Do not use generic broker portals, Top Pick pages, 산업분류, or industry classification as algorithm truth.
- Excluded broker discovery terms: FnGuide, Hana, NH Research, Kiwoom, Top Pick, 산업분류, industry classification.

## Regression Test Cases

| Test | Expected result |
| --- | --- |
| RS new high before price | `leadership_freshness_score` rises while price has not made same-window high. |
| True rank vs proxy | `rs_rank_true` and `rs_rank_proxy` are both present and labeled differently. |
| Prior leader exhaustion | direct report evidence remains reference-only/deferred and cannot trigger BUY. |
| Leader lifecycle phase | no runtime field in V2; revisit only with sufficient prior-cycle history. |
| Hidden RS | weak benchmark days produce positive resilience only when stock holds up. |
| RS state | `rising`, `stable`, `fading`, `weakening` are classified from RS/structure. |
| Lead-lag follower | positive `peer_lead_score` requires lagged pair evidence. |
| Excluded families | existing 3대 screener names appear only in exclusion metadata. |

## Non-Goals

- No further implementation in this validation pass unless regression tests expose a leader-lagging/source-registry defect.
- No full CANSLIM clone.
- No direct Mark Minervini, Weinstein/Weinstain, Mansfield, or Qullamaggie rule reuse.
- No general broker report ingestion.
