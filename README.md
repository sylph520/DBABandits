# DBA Bandits

We propose a self-driving approach to online index selection that eschews the DBA and query optimiser, and instead learns the benefits of viable structures through strategic exploration and direct performance observation.

**Cite:** R Malinga Perera, Bastian Oetomo, Benjamin IP Rubinstein, and Renata Borovica-Gajic. DBA bandits: Self-driving index tuning under ad-hoc, analytical workloads with safety guarantees. In 2021 IEEE 37th International Conference on Data Engineering (ICDE), pages 600–611. IEEE, 2021.

## Quick Start

### PostgreSQL (Recommended)
```bash
python simulation/sim_c3ucb_vR.py
```

### MSSQL (Legacy)
```bash
python simulation/sim_c3ucb_vR.py --db-type mssql
```

### Override Parameters via CLI
```bash
# Quick test run (5 rounds)
python simulation/sim_c3ucb_vR.py --rounds 5 --reps 1

# Use actual query execution (slower but real metrics)
python simulation/sim_c3ucb_vR.py --no-optimizer-costs

# Use real indexes for rounds phase (default: uses hypothetical)
python simulation/sim_c3ucb_vR.py --use-real-indexes

# Custom bandit parameters
python simulation/sim_c3ucb_vR.py --alpha 2.0 --lambda 0.3 --max-memory 50000

# Custom workload
python simulation/sim_c3ucb_vR.py --workload /resources/workloads/tpc_h_static_100_postgresql.json
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--db-type` | Database type: `postgresql`, `mssql` | `postgresql` |
| `--db-server` | Database server hostname | from `config/db.conf` |
| `--db-name` | Database name | from `config/db.conf` |
| `--db-user` | Database username | from `config/db.conf` |
| `--db-password` | Database password | from `config/db.conf` |
| `--db-port` | Database port | from `config/db.conf` |
| `--db-schema` | Database schema | from `config/db.conf` |
| `--experiment` | Experiment ID from `config/exp.conf` | from config |
| `--no-optimizer-costs` | Use actual query execution (vs EXPLAIN costs) | False |
| `--hyp-rounds` | Number of hypothetical rounds (HypoPG) | from config (default: 0) |
| `--use-real-indexes` | Use real indexes for rounds phase | False (uses hypothetical) |
| `--rounds` | Number of actual rounds | from config |
| `--reps` | Number of repetitions | from config |
| `--workload` | Workload file path | from config |
| `--alpha` | C3UCB alpha parameter | from config |
| `--lambda` | C3UCB lambda parameter | from config |
| `--max-memory` | Max memory for indexes (MB) | from config |

## Index Phase Design

The experiment has two phases:

| Phase | Index Type | Purpose |
|-------|------------|---------|
| `hyp_rounds` | Hypothetical (HypoPG) | Fast exploration, learn bandit weights |
| `rounds` | Real (default) or Hypothetical | Actual performance measurement |

**Total iterations** = `hyp_rounds` + `rounds`

**When to use `--use-real-indexes`:**
- Use for final validation and production recommendations
- Default (without flag) uses hypothetical indexes in rounds phase for faster iteration

**Examples:**

| Command | Total Rounds | Behavior |
|---------|--------------|----------|
| `--hyp-rounds 5 --rounds 25` | 30 | 5 hypo + 25 hypothetical |
| `--hyp-rounds 5 --rounds 25 --use-real-indexes` | 30 | 5 hypo + 25 real |
| `--hyp-rounds 0 --use-real-indexes` | 25 | 25 real only |
| `--hyp-rounds 5` (default rounds=25) | 30 | 5 hypo + 25 hypothetical |

## How to run

### 1. Setting up the environment 
1. Generate data for your benchmark (TPC-H, TPC-DS, etc.)
2. Generate queries for your benchmark
3. Create a workload file for your benchmark (example workload files are in `resources/workloads/`)
4. Add DB connection details to `config/db.conf`

### 2. Setting up your experiment

Edit `config/exp.conf` to configure experiments. See examples in the file.

### 3. Run the simulation

The main entry point is `simulation/sim_c3ucb_vR.py`:

```bash
# Default run with PostgreSQL (uses EXPLAIN optimizer costs for speed)
python simulation/sim_c3ucb_vR.py

# Run with actual query execution for real metrics
python simulation/sim_c3ucb_vR.py --no-optimizer-costs

# Override experiment config via CLI
python simulation/sim_c3ucb_vR.py --rounds 25 --reps 3 --hyp-rounds 5 --max-memory 50000

# Run specific experiment from config
python simulation/sim_c3ucb_vR.py --experiment tpc_h_postgres_hypo
```

### 4. Results

Results are saved in `experiments/<config_name>/<timestamp>/`:

| File | Description |
|------|-------------|
| `<experiment_id>.log` | Detailed run log |
| `comparison_table.csv` | Aggregated results |
| `*.png` | Performance plots |

**Example folder structure:**
```
experiments/tpc_h_postgres_hypo__mixed-hyp-5__rounds-25__reps-1__alpha-1.0__lambda-0.5__workload-tpc_h_static_100__db-postgresql/
└── 20260328_143052/
    ├── tpc_h_postgres_hypo.log
    ├── comparison_table.csv
    └── Batch Time Comparison.png
```

## Available Workloads

Workload files are in `resources/workloads/`:

| File | Description |
|------|-------------|
| `tpc_h_static_100_postgresql.json` | 2100 TPC-H queries (100 variants × 21 templates) |
| `tpc_h_optimized_postgresql.json` | Optimized queries with CTE transformations |
| `tpc_h_static_100_postgresql_no_2_17_20.json` | Excludes templates 2, 17, 20 |
| `test_query_15.json` | Single Q15 test workload |

**Workload file format** (JSONL - one JSON object per line):
```json
{"id": 1, "query_string": "SELECT ...", "predicates": {...}, "payload": {...}, "group_by": {}, "order_by": {}}
```

## PostgreSQL Setup

### Enable HypoPG (for hypothetical indexes)

PostgreSQL supports hypothetical indexes via the HypoPG extension:

```sql
-- Check if HypoPG is installed
SELECT * FROM hypopg();

-- Install HypoPG (if not installed)
CREATE EXTENSION IF NOT EXISTS hypopg;
```

### Create Primary and Foreign Keys

After loading TPC-H data, run:
```bash
psql -d <database> -f resources/tpch_pk_fk_postgres.sql
```

This creates:
- Primary keys on all tables
- Foreign key constraints
- Recommended indexes for TPC-H query performance

### Update Statistics

After creating constraints and indexes:
```sql
ANALYZE VERBOSE;
```

## Experiment Config Explained

### Example

```ini
[tpc_h_postgres_hypo]
reps = 1
rounds = 25
hyp_rounds = 5
workload_shifts = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
queries_start = [0, 21, 42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252, 273, 294, 315, 336, 357, 378, 399, 420, 441, 462, 483, 504]
queries_end = [21, 42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252, 273, 294, 315, 336, 357, 378, 399, 420, 441, 462, 483, 504, 525]
ta_runs = [1]
ta_workload = optimal
workload_file = /resources/workloads/tpc_h_static_100_postgresql.json
config_shifts = [0]
config_start = [0]
config_end = [20]
max_memory = 25000
input_alpha = 1
input_lambda = 0.5
time_weight = 5
memory_weight = 0
components = ["MAB"]
mab_versions = ["simulation.sim_c3ucb_vR"]
```

### Parameters

| # | Parameter | Description |
|---|-----------|-------------|
| 1 | Name | Experiment name |
| 2 | reps | Number of times to run the experiment |
| 3 | rounds | Number of bandit rounds |
| 4 | hyp_rounds | Hypothetical rounds (HypoPG for PostgreSQL). Default: 0 |
| 5 | workload_shifts | Workload shift points |
| 6 | queries_start | Query start indices per round |
| 7 | queries_end | Query end indices per round |
| 8 | ta_runs | Rounds to invoke PDTool |
| 9 | ta_workload | `optimal` or `last_run` |
| 10 | workload_file | Path to workload JSON file |
| 11-13 | config_* | Config shift parameters |
| 14 | max_memory | Memory limit in MB |
| 15 | input_alpha | Bandit alpha parameter |
| 16 | input_lambda | Bandit lambda parameter |
| 17-18 | *_weight | Cost weighting parameters |
| 19 | components | Components to compare: `MAB`, `TA_OPTIMAL`, `NO_INDEX`, etc. |
| 20 | mab_versions | MAB version file name |
