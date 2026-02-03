# PostgreSQL-Compatible TPC-H Workload

This directory contains PostgreSQL-compatible versions of the TPC-H workload queries.

## Files

- **tpc_h_static_100.json** - Original MSSQL workload (100 query instances)
- **tpc_h_static_100_postgresql.json** - Converted PostgreSQL workload

## Usage

To use the PostgreSQL workload, update your experiment config:

```ini
[tpc_h_postgres_test]
reps = 1
rounds = 25
hyp_rounds = 0
workload_shifts = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
queries_start = [0, 21, 42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252, 273, 294, 315, 336, 357, 378, 399, 420, 441, 462, 483, 504]
queries_end = [21, 42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252, 273, 294, 315, 336, 357, 378, 399, 420, 441, 462, 483, 504, 525]
workload_file = resources/workloads/tpc_h_static_100_postgresql.json
components = ["MAB"]
mab_versions = ["simulation.sim_c3ucb_vR"]
```

Then run:

```bash
python simulation/sim_c3ucb_vR.py --experiment tpc_h_postgres_test
```

## Conversion Script

To convert your own MSSQL workload:

```bash
python scripts/convert_workload_to_postgres.py
```

This converts:
- `DATEADD(dd, -N, date)` → `(date - INTERVAL 'N days')`
- `DATEADD(mm, N, date)` → `(date + INTERVAL 'N months')`
- `DATEADD(yy, N, date)` → `(date + INTERVAL 'N years')`
- `SUBSTRING(col, 1, 2)` → `SUBSTRING(col FROM 1 FOR 2)`
- `YEAR(date)` → `EXTRACT(YEAR FROM date)`

## Notes

- PostgreSQL workload **requires** `hyp_rounds = 0` (no hypothetical indexes)
- Query selectivity calculation will work properly with PostgreSQL syntax
- All TPC-H query templates are supported
