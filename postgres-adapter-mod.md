# PostgreSQL Adapter Branch - Summary of Modifications

## Overview

This document summarizes all modifications made to the DBA Bandits project in the `postgres-adapter` branch. The branch adds PostgreSQL support via HypoPG adapter for hypothetical indexes and improves the experiment output folder structure.

---

## Commit History

### Latest Commits (Most Recent First)

| Commit | Description |
|--------|-------------|
| `3d09416` | fix: Improve experiment folder naming convention |
| `c1e717e` | docs: Update README with index phase design and structured output folders |
| `f63b911` | fix: Remove logging setup from BaseSimulator.__init__ to prevent empty folder creation |
| `dfde937` | feat: Add --use-real-indexes flag to control index type in rounds phase |
| `0f47279` | feat: Add structured experiment output folder with timestamp |
| `f7fb7e6` | chore: Update .gitignore to exclude Python bytecode and editor files |
| `7df933d` | chore: Add Python, IDE, and OS entries to .gitignore |
| `5e41e13` | feat: Add --real-only and --all-hypothetical CLI shortcuts (later removed) |
| `9edc62d` | fix: Align PostgreSQL HypoPG with MSSQL hypothetical index logic |
| `57a9fd8` | fix: Extract proper OID from hypopg_create_index result |
| `7427a96` | fix: Remove buggy fallback in HypoPG drop_index |
| `961dabd` | docs: Update README with CLI arguments and PostgreSQL setup |
| `77936b6` | feat: Add CLI arguments to override experiment config values |
| `d038e75` | Add PostgreSQL TPC-H primary and foreign key constraints script |
| `04503f3` | fix: Use forward slashes for cross-platform path compatibility |
| `76d2d08` | fix: Add benchmark type extraction for non-standard database names |

---

## Key Features Implemented

### 1. PostgreSQL Support with HypoPG

- **HypoPG Adapter**: Database adapter using HypoPG extension for hypothetical indexes
- **Database Adapter Abstraction**: New abstraction layer supporting multiple database types
- **OID Extraction Fix**: Fixed proper OID extraction from `hypopg_create_index()` result
- **Fallback Removal**: Removed buggy fallback in HypoPG drop_index method

### 2. CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--db-type` | Database type (postgresql, mssql) | postgresql |
| `--db-server` | Database server hostname | from config |
| `--db-name` | Database name | from config |
| `--hyp-rounds` | Hypothetical rounds | from config (default: 0) |
| `--use-real-indexes` | Use real indexes in rounds phase | False |
| `--rounds` | Number of rounds | from config |
| `--reps` | Number of repetitions | from config |
| `--workload` | Workload file path | from config |
| `--alpha` | C3UCB alpha parameter | from config |
| `--lambda` | C3UCB lambda parameter | from config |
| `--max-memory` | Max memory (MB) | from config |

### 3. Structured Experiment Output

New folder structure to organize experiment results:

```
experiments/
└── <experiment_id>__<mode>__rounds-<N>__reps-<N>__alpha-<X>__lambda-<Y>__workload-<name>__db-<type>/
    └── <timestamp>/
        ├── <experiment_id>.log
        ├── comparison_table.csv
        └── *.png (plots)
```

**Mode Naming Convention:**

| Command | Mode |
|---------|------|
| `--hyp-rounds 0` | `no_hyp_explore` |
| `--hyp-rounds 0 --use-real-indexes` | `no_hyp_explore_real` |
| `--hyp-rounds 5` | `hyp_explore_5` |
| `--hyp-rounds 5 --use-real-indexes` | `hyp_explore_5_real` |

### 4. Index Phase Design

The experiment has two phases:

| Phase | Index Type | Purpose |
|-------|------------|---------|
| `hyp_rounds` | Hypothetical (HypoPG) | Fast exploration, learn bandit weights |
| `rounds` | Real (default) or Hypothetical | Actual performance measurement |

**Total iterations** = `hyp_rounds` + `rounds`

- Without `--use-real-indexes`: rounds phase uses hypothetical indexes (faster)
- With `--use-real-indexes`: rounds phase uses real indexes (accurate but slower)

---

## New Files Created

| File | Description |
|------|-------------|
| `resources/tpch_pk_fk_postgres.sql` | PostgreSQL PK/FK constraints and indexes |
| `resources/workloads/tpc_h_optimized_postgresql.json` | Optimized workload with CTEs |
| `resources/workloads/tpc_h_static_100_postgresql_no_2_17_20.json` | Workload excluding templates 2, 17, 20 |
| `resources/workloads/test_query_15.json` | Single Q15 test workload |
| `scripts/test_query_15.py` | Test script for Q15 query handling |

---

## Modified Files

| File | Changes |
|------|---------|
| `simulation/sim_c3ucb_vR.py` | Major: Added adapter support, new CLI args, structured output, new naming |
| `database/adapters/hypopg_adapter.py` | New: HypoPG adapter for hypothetical indexes |
| `database/adapters/postgresql_adapter.py` | New: PostgreSQL adapter |
| `database/adapters/mssql_adapter.py` | New: MSSQL adapter |
| `database/base.py` | Updated: Database interface |
| `database/factory.py` | New: Database adapter factory |
| `shared/helper.py` | Added: get_config_folder_path, get_experiment_run_path |
| `shared/configs_v2.py` | Made hyp_rounds default to 0 |
| `constants.py` | Minor updates |
| `README.md` | Major: Complete rewrite with new documentation |
| `.gitignore` | Added: __pycache__, *.pyc, .vscode/settings.json |

---

## Configuration Files

### Database Config (`config/db.conf`)

```ini
[SYSTEM]
db_type = postgresql

[POSTGRESQL]
server = /tmp
database = indexselection_tpch___1
username = sclai
password = 
schema = public
port = 51204
```

### Experiment Config (`config/exp.conf`)

```ini
[tpc_h_postgres_hypo]
reps = 1
rounds = 25
hyp_rounds = 5
workload_file = /resources/workloads/tpc_h_static_100_postgresql.json
max_memory = 25000
input_alpha = 1
input_lambda = 0.5
# ... additional parameters
```

---

## Usage Examples

### Quick Test (Hypothetical Indexes in Rounds Phase)
```bash
python simulation/sim_c3ucb_vR.py --db-name indexselection_tpch___1pk
```

### With Real Indexes in Rounds Phase
```bash
python simulation/sim_c3ucb_vR.py --db-name indexselection_tpch___1pk --use-real-indexes
```

### Custom Configuration
```bash
python simulation/sim_c3ucb_vR.py \
    --db-name indexselection_tpch___1pk \
    --hyp-rounds 5 \
    --rounds 25 \
    --reps 3 \
    --alpha 2.0 \
    --lambda 0.3 \
    --max-memory 50000
```

---

## Breaking Changes from Previous Version

1. **Folder Structure**: Results now saved in structured folders instead of simple `<experiment_id>/`
2. **CLI Arguments**: 
   - `--real-only` and `--all-hypothetical` removed (use `--use-real-indexes` instead)
   - `--hyp-rounds` now defaults to 0 if not specified
3. **Index Type in Rounds Phase**: Default is now hypothetical (use `--use-real-indexes` for real)

---

## Notes

- HypoPG extension must be installed in PostgreSQL: `CREATE EXTENSION IF NOT EXISTS hypopg;`
- The `hyp_rounds` and `rounds` parameters are **additive** - total iterations = hyp_rounds + rounds
- `--use-real-indexes` flag affects only the rounds (measurement) phase, not the hyp_rounds (exploration) phase

---

*Generated: March 2026*