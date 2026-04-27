# Docs Guide

## Purpose

`docs/` is the stable documentation plane for `invest-prototype-main`. It explains live runtime behavior, ownership boundaries, implementation fidelity, and higher-end reference targets without letting raw notes override code.

## Source Of Truth Priority

1. `subsystem-system-design.md`
2. module `*-design.md`
3. module `*-technical-specification.md`, `*-runtime-spec.md`, `*-erd.md`
4. subsystem `*-runtime-audit.md`
5. subsystem `*-runtime-status.md`
6. `docs/audits/*.md` and `docs/archive/raw-sources/**`
7. `docs/archive/raw-sources/PRD/**` and `docs/archive/raw-sources/Reference/**`

## Stable Document Set

Each family owns:

- `pipeline-and-collection-subsystem-prd.md`, `pipeline-and-collection-subsystem-system-design.md`, `pipeline-and-collection-runtime-audit.md`, `pipeline-and-collection-runtime-status.md`
- `market-intel-core-compatibility-subsystem-prd.md`, `market-intel-core-compatibility-subsystem-system-design.md`, `market-intel-core-compatibility-runtime-audit.md`, `market-intel-core-compatibility-runtime-status.md`
- `markminervini-subsystem-prd.md`, `markminervini-subsystem-system-design.md`, `markminervini-runtime-audit.md`, `markminervini-runtime-status.md`
- `weinstein-stage2-subsystem-prd.md`, `weinstein-stage2-subsystem-system-design.md`, `weinstein-stage2-runtime-audit.md`, `weinstein-stage2-runtime-status.md`
- `leader-lagging-subsystem-prd.md`, `leader-lagging-subsystem-system-design.md`, `leader-lagging-runtime-audit.md`, `leader-lagging-runtime-status.md`
- `qullamaggie-subsystem-prd.md`, `qullamaggie-subsystem-system-design.md`, `qullamaggie-runtime-audit.md`, `qullamaggie-runtime-status.md`
- `tradingview-subsystem-prd.md`, `tradingview-subsystem-system-design.md`, `tradingview-runtime-audit.md`, `tradingview-runtime-status.md`
- `signals-subsystem-prd.md`, `signals-subsystem-system-design.md`, `signals-runtime-audit.md`, `signals-runtime-status.md`

Each module owns `*-prd.md`, `*-design.md`, `*-technical-specification.md`, `*-runtime-spec.md`, and `*-erd.md`.

## Algorithm Fidelity And High-End Rule

- stable docs must separate current code-grounded behavior from higher-end reference intent
- `runtime-status` names what is implemented, heuristic, proxy-backed, or high-end deferred
- higher-end reference content is not live until code and tests move

## Legacy And Archive Rules

- `docs/archive/raw-sources/PRD/**` and `docs/archive/raw-sources/Reference/**` remain operator/reference material
- absorbed markdown from old `docs/*.md` and `screeners/**/*.md` is rehomed under `docs/archive/raw-sources/**` or `docs/audits/archive/**`
- archived raw material is traceability-only and is not source of truth

## ERD Rule

ERD files in this repo describe artifact / producer / consumer / lineage relationships, not database schemas.

## Current Layout

- `runtime/pipeline-and-collection`
- `runtime/market-intel-core-compatibility`
- `engines/markminervini`
- `engines/weinstein-stage2`
- `engines/leader-lagging`
- `engines/qullamaggie`
- `engines/tradingview`
- `engines/signals`
- `audits/`: dated Korean baseline audits and matrices
- `archive/raw-sources/`: archived raw markdown absorbed by stable docs
- `superpowers/`: plan artifacts; reference-only, not stable runtime docs
