# Documentation Baseline Audit

Date: `2026-04-16`

## 범위

- 본 audit는 `README.md`, 기존 `docs/*.md`, `screeners/**/*.md`, `docs/archive/raw-sources/PRD/**/*.md`, `docs/archive/raw-sources/Reference/**/*.md`를 기준선으로 조사했다.
- 새 stable docs는 `docs/runtime/**`와 `docs/engines/**`에 영문, 날짜 없는 파일명으로 생성했다.
- `docs/archive/raw-sources/PRD/`와 `docs/archive/raw-sources/Reference/`는 repo 규칙상 삭제하거나 재배치하지 않고 operator/reference material로 유지한다.

## 방법

- 기준선은 `docs/audits/2026-04-16-documentation-baseline-matrix.csv`에 기록했다.
- 문서-코드-테스트 연결은 `docs/audits/2026-04-16-documentation-traceability-matrix.csv`에 기록했다.
- 각 stable 문서는 가장 가까운 live code path와 regression test를 직접 명시한다.
- audit는 설계를 바꾸지 않고 현재 코드 기준 정합성만 기록한다.
- code-grounded behavior, stable design, higher-end reference target을 분리해서 적는다.

## 결과 요약

- legacy markdown inventory: `69`
- `absorbed`: `9`
- `archived`: `5`
- `reference-only`: `55`
- encoding drift 또는 console mojibake 관측: `3`

## 핵심 발견

- 기존에는 `docs/README.md`가 없어 source-of-truth 순서가 고정돼 있지 않았다.
- root `README.md`는 stable doc hierarchy, signals/augment, compatibility boundary를 충분히 설명하지 못했다.
- `screeners/**/*.md` raw notes는 코드 옆에 있었지만 canonical runtime spec 역할을 하기엔 위치와 encoding 상태가 불안정했다.
- `docs/archive/raw-sources/PRD/`와 `docs/archive/raw-sources/Reference/`에는 high-end algorithm intent가 남아 있지만, live runtime contract와 implementation fidelity를 분리하지 않으면 drift가 커진다.
- `signals`, `augment`, `market-intel-core compatibility`는 코드와 테스트는 있었지만 local stable docs가 없었다.

## disposition 규칙

- `absorbed`: legacy 문서의 핵심 intent를 stable docs가 흡수했고 더 이상 source of truth가 아니다.
- `archived`: 기존 위치에서 제거하고 `docs/archive/raw-sources/**` 또는 `docs/audits/archive/**`로 재배치했다.
- `reference-only`: 남겨두되 current runtime contract를 정의하지 않는 참고 자료다.
- `docs/archive/raw-sources/PRD/`와 `docs/archive/raw-sources/Reference/`는 repo 계약 때문에 원위치 유지가 기본이다.

## 알고리즘 구현충실도와 High-End 기준

- `implemented`: 현재 코드와 테스트가 직접 고정한 동작이다.
- `heuristic`: 동작은 구현돼 있지만 스코어링, 상태 분류, 우선순위 결정이 규칙 기반 근사치다.
- `proxy-backed`: 외부 데이터나 파생 지표가 목표 개념을 대리 표현한다.
- `high-end deferred`: PRD나 reference에는 있으나 현재 코드와 테스트 기준으로는 아직 live contract가 아니다.
- 모든 `runtime-audit`는 live code, stable design, higher-end reference target을 함께 기록한다.
- 모든 `runtime-status`는 `implemented`, `heuristic`, `proxy-backed`, `high-end deferred` 같은 표현으로 현재 상태를 요약한다.

## 문서군 정리 결과

- runtime families: `pipeline-and-collection`, `market-intel-core-compatibility`
- engine families: `markminervini`, `weinstein-stage2`, `leader-lagging`, `qullamaggie`, `tradingview`, `signals`
- 각 family는 subsystem `prd`, `system-design`, `runtime-audit`, `runtime-status`를 가진다.
- 각 module은 `prd`, `design`, `technical-specification`, `runtime-spec`, `erd`를 가진다.

## 현재 기준선에서 보이는 후속 backlog

- `pipeline-and-collection`: task registry와 provider/readiness lineage는 아직 코드 조건문과 관례 의존성이 크다.
- `market-intel-core-compatibility`: `leader_core_v1`, `market_context_v1` consume seam은 존재하지만 manifest-style version handshake는 없다.
- `signals`: event/state regression coverage는 강하지만 family arbitration, calibration lineage, semantic status 표준화가 더 필요하다.
- `markminervini`, `weinstein-stage2`, `leader-lagging`, `qullamaggie`: 전략 PRD가 묘사하는 higher-end semantics가 현재 구현보다 풍부하다.
- `tradingview`: preset metric 계산은 명확하지만 preset catalog와 provenance는 코드에 고정돼 있다.

## 산출물

- canonical docs guide: `docs/README.md`
- baseline matrix: `docs/audits/2026-04-16-documentation-baseline-matrix.csv`
- traceability matrix: `docs/audits/2026-04-16-documentation-traceability-matrix.csv`
- stable runtime docs: `docs/runtime/**`
- stable engine docs: `docs/engines/**`
- archived raw sources: `docs/archive/raw-sources/**`, `docs/audits/archive/**`
