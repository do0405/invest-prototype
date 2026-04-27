# Cycle Store And Writers ERD

## Entities

- cycle-store-and-writers
- output artifacts described below

## Producers

- `screeners/signals/cycle_store.py`
- `screeners/signals/writers.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| cycle-store-and-writers outputs | `screeners/signals/cycle_store.py, screeners/signals/writers.py` | `signal_history.csv`, `signal_state_history.csv`, `peg_event_history.csv`, `open_family_cycles.csv`, plus writer-emitted v2 artifact files. | operators/tests |

## Closest Tests

- `tests/test_signal_engine_restoration.py`
