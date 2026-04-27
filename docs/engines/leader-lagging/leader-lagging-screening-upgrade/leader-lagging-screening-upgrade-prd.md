# Leader Lagging Screening Upgrade PRD

- status: implemented V2 runtime contract
- as_of: 2026-04-22
- scope: implemented leader/follower V2 behavior, additive diagnostics, and source-context contract

## Purpose

이 문서는 리더/래깅 스크리너 고도화의 제품 요구사항을 기존 3대 스크리너 직접 계보에서 분리해 다시 정의한다. 별도 subsystem으로 이미 존재하는 3대 스타일을 반복 구현하지 않고, 남는 독립 로직만으로 `주도주 속성`, `상대강도`, `leader lifecycle`, `lead-lag follower`를 강화한다.

핵심 목적은 두 가지다.

- early/confirmed leader: 주도주 자체의 속성, 중기 모멘텀, RS line, 52주 고점 근접성, hidden RS, rotation state가 함께 확인되는 후보.
- linked follower: 리더와 경제적/시장적 연결성이 있고, 아직 반응이 덜 왔지만 구조와 RS inflection이 살아 있는 후보.

## Product Thesis

리더는 `RS-only` 종목이 아니다. 리더는 `상대강도 + 중기 모멘텀 + 52주 고점 근접성 + 가격 구조 + 거래량 + 생애주기 위치 + 그룹/peer 맥락`이 함께 맞아야 한다.

팔로워는 약한 laggard가 아니다. follower는 `leader connection + underreaction + structure preservation + RS inflection`이 동시에 있는 linked catch-up 후보이다.

증권사/리포트 evidence는 특정 종목 추천이나 Top Pick을 가져오는 경로가 아니다. 이 문서에서 허용하는 리포트는 `주도주의 생로병사`처럼 주도주 자체의 형성, 상승, 쇠퇴, 교체, 공통분모를 설명하는 자료뿐이다.

## Excluded Existing Screener Boundary

The existing Mark Minervini, Weinstein/Weinstain, Mansfield RS, and Qullamaggie families are `excluded-existing-screener` / `excluded_existing_screener` for this upgrade. They may be referenced only to explain that they are already covered by separate local subsystems, not as direct algorithm sources here.

Excluded direct source families:

- Mark Minervini Trend Template, SEPA, and VCP.
- Weinstein/Weinstain Stage Analysis and Mansfield RS.
- Qullamaggie Breakout, Episodic Pivot, ADR, and parabolic extension workflows.

Allowed carryover despite the exclusion:

- Generic `RS line`, `RS Rating`, `RS New High`, `RS New High Before Price`, `benchmark_relative_strength`, and `rs_slope`.
- Generic structure preservation and pivotal breakout concepts when not imported from the excluded systems.

## Source Policy

| Policy | Meaning | Examples in this upgrade |
| --- | --- | --- |
| `adopt` | 독립 검증 근거가 강하고 로컬 OHLCV/metadata로 직접 구현 가능 | Jegadeesh-Titman momentum, George-Hwang 52-week high, Hou lead-lag, Lo-MacKinlay lead-lag. |
| `adapt` | 특정 platform/tool의 아이디어를 로컬 deterministic screener로 변환 | IBD/O'Neil RS line, RS Rating proxy, Hidden RS, RS rotation state, Darvas/pivotal breakout. |
| `watchlist-only` | 주도주 속성 또는 attention/catalyst 맥락을 설명하지만 signal truth는 아님 | Macrend Invest `주도주의 생로병사`, analyst coverage, limited attention/catalyst notes. |
| `reference-only` | 제약, taxonomy, public prior art | TradingView/Pine limitations and repainting caveats. |
| `excluded-existing-screener` | 별도 local subsystem이 이미 담당하므로 이번 upgrade의 direct source가 아님 | Mark Minervini, Weinstein/Weinstain, Mansfield, Qullamaggie. |

## Reference Register

| Source family | Source | Policy | Extracted logic | Product impact |
| --- | --- | --- | --- | --- |
| RS implementation | [`iArpanK/RS-Screener`](https://github.com/iArpanK/RS-Screener), [`arpankundu4/rs-line`](https://github.com/arpankundu4/rs-line) | `adapt` | RS New High, RS New High Before Price, RS line alerting | `leadership_freshness_score` must detect RS leadership before obvious price breakout. |
| RS percentile / proxy | [`skyte/relative-strength`](https://github.com/skyte/relative-strength), [TradingView RS Rating 1-99](https://www.tradingview.com/script/vYn2nyIW-RS-Rating-1-99/) | `adapt` | Weighted 3/6/9/12 month performance and 1-99 style ranking | Separate `rs_rank_true` from `rs_rank_proxy`. |
| IBD/O'Neil independent concepts | [IBD RS line / RS Rating discussion](https://www.investors.com/how-to-invest/investors-corner/stock-market-leaders-bullish-relative-strength-lines/) | `adapt` | Relative strength leadership, RS line new high, new high leadership | Use only RS/new-high/group-confirmation concepts, not a full CANSLIM clone. |
| Momentum evidence | [Jegadeesh & Titman](https://ideas.repec.org/a/bla/jfinan/v48y1993i1p65-91.html) | `adopt` | 3-12 month winner persistence | Leader horizon must include intermediate momentum. |
| 52-week high evidence | [George & Hwang](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1104491) | `adopt` | 52-week high proximity and anchoring | Add `near_high_leadership_score` while keeping extension risk separate. |
| Industry/group momentum | [Moskowitz & Grinblatt](https://ideas.repec.org/a/bla/jfinan/v54y1999i4p1249-1290.html) | `adopt` | Group/industry momentum explains stock momentum | Group context remains a gate/context, not a broker industry-classification import. |
| Lead-lag evidence | [Hou](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1151155), [Lo & MacKinlay](https://academic.oup.com/rfs/article/3/2/175/1595488) | `adopt` | Visible/large leaders can lead related peers | Follower scoring uses 1/2/3/5 day lagged correlation. |
| PEAD / limited attention | [Bernard & Thomas](https://ideas.repec.org/a/bla/joares/v27y1989ip1-36.html), [DellaVigna & Pollet](https://www.nber.org/papers/w11683) | `adapt` | Event underreaction and delayed attention | `sympathy_freshness_score` can raise review priority, not BUY. |
| Hidden RS | Local Pine taxonomy and public RS leader references | `adapt` | Stocks holding up on benchmark down days | `hidden_rs_score` identifies early leader evidence during weak windows. |
| RS state | AG Pro RS Rotation Map style state vocabulary | `adapt` | `rising`, `stable`, `fading`, `weakening`, `unknown` | `leader_rs_state` and `fading_risk_score` replace excluded stage names. |
| Darvas / pivotal breakout | [Darvas Box Theory](https://www.investopedia.com/terms/d/darvasboxtheory.asp) | `adapt` | Box high/low, volume-confirmed breakout, stop/invalidation anchor | `structure_readiness_score`, `breakout_confirmation_score`; never standalone BUY. |
| Direct leader attribute report | [Macrend Invest `주도주의 생로병사` PDF](https://ssl.pstatic.net/imgstock/upload/research/invest/1539740471781.pdf) | `watchlist-only` | Prior leaders rarely repeat immediately, price momentum can precede earnings, leaders have lifecycle phases | Reference-only/deferred; no runtime score field until 504+ trading-day history is available. |

## Broker Research Policy

Allowed broker/direct-report evidence:

- 주도주 자체의 속성, 공통분모, 형성기/상승기/소멸기, 로테이션을 설명하는 자료.
- 현재 확인된 핵심 자료: Macrend Invest `주도주의 생로병사`.
- Output fields: `leader_lifecycle_evidence`, `rotation_evidence`, `attribute_evidence`.

Excluded broker discovery terms: FnGuide, Hana, NH Research, Kiwoom, Top Pick, 산업분류, industry classification. These can appear only as excluded/non-algorithm source-discovery examples. They are not direct evidence for this upgrade.

## User Jobs

- Operator: 오늘의 early leader, confirmed leader, fading leader, linked follower를 한 번에 본다.
- Research reviewer: 주도주 속성 evidence가 가격/RS/lead-lag logic과 어떻게 연결되는지 검토한다.
- Signal developer: 기존 public BUY/SELL 계약을 바꾸지 않고 additive context fields만 설계한다.

## Score Families

### Leader Scores

- `rs_quality_score`: true universe percentile, RS proxy, RS line slope, RS new high.
- `leadership_freshness_score`: RS New High Before Price, group rank improvement, early momentum acceleration.
- `momentum_persistence_score`: 3/6/9/12 month weighted performance and intermediate-term rank.
- `near_high_leadership_score`: 52-week high proximity without conflating with extension.
- `hidden_rs_score`: benchmark down-day and weak-window resilience.
- `leader_rs_state`: `rising`, `stable`, `fading`, `weakening`, `unknown`.
- `leader_confidence_score`, `confidence_bucket`, and reason-code fields: explain why a candidate is accepted, rejected, or extended.
- Deferred/reference-only: prior-cycle and lifecycle ideas from `주도주의 생로병사`; no `leader_lifecycle_phase`, `prior_cycle_exclusion_score`, or `rotation_candidate_score` runtime field.
- `structure_readiness_score`: Darvas/pivotal box preservation and support integrity.
- `breakout_confirmation_score`: volume-confirmed pivotal breakout evidence.
- `extension_risk_score`: excessive short-term move, distance from recent structure/MA, chase risk.

### Follower Scores

- `peer_link_score`: same industry/group/peer relationship first; optional economically linked narrative second.
- `peer_lead_score`: leader return leads follower return over 1/2/3/5 day lags.
- `underreaction_score`: catch-up room after leader move.
- `rs_inflection_score`: follower RS rank/RS line slope improves.
- `structure_preservation_score`: follower has not broken support/box/major structure.
- `sympathy_freshness_score`: PEAD/limited-attention catch-up window without overextension.

## Output Impact

| Output | Additive fields | Requirement |
| --- | --- | --- |
| `leaders` | `early_leader_score`, `rs_quality_score`, `momentum_persistence_score`, `near_high_leadership_score`, `hidden_rs_score`, `leader_rs_state`, `leader_tier`, `entry_suitability`, `leader_confidence_score`, `confidence_bucket`, `extension_risk_score` | Separate fresh leadership from extended or fading leadership. |
| `followers` | `peer_lead_score`, `underreaction_score`, `rs_inflection_score`, `structure_preservation_score`, `sympathy_freshness_score`, `follower_confidence_score`, `pair_evidence_confidence`, `propagation_state`, `link_evidence_tags` | Follower remains BUY-capable but not automatic BUY. |
| `leader_follower_pairs` | `lead_lag_profile`, `propagation_ratio`, `propagation_state`, `connection_type`, `pair_confidence`, `pair_evidence_confidence` | Explain why a follower is linked to a specific leader. |
| `quality artifacts` | `leader_quality_diagnostics`, `leader_quality_summary`, `leader_candidate_quality_diagnostics`, `leader_candidate_quality_summary`, `leader_threshold_tuning_report` | Final leaders, pre-final candidate diagnostics, and conservative tuning recommendations are separated. |
| `source_context` | score/state/reason-code summaries only | Keep direct report evidence internal/additive and do not expose raw URLs or report text. |

## Acceptance Criteria

- The five docs use the same source-policy vocabulary, including `excluded-existing-screener`.
- Direct Mark Minervini, Weinstein/Weinstain, Mansfield, and Qullamaggie algorithm rows are removed or isolated as excluded existing subsystems.
- RS/RS line/RS Rating remains as independent relative-strength logic.
- Broker references are narrowed to direct leader-attribute material; generic portals, industry classification, and Top Pick notes are excluded.
- The algorithm doc contains `Remaining Method-To-Algorithm Matrix` and `Score-To-Output Field Matrix`.
