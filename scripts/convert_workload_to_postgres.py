#!/usr/bin/env python3
"""
Convert MSSQL TPC-H workload to PostgreSQL-compatible format.

Converts MSSQL-specific syntax to PostgreSQL:
- DATEADD(dd, -112, date) → (date - INTERVAL '112 days')
- DATEADD(mm, 3, date) → (date + INTERVAL '3 months')
- DATEADD(yy, 1, date) → (date + INTERVAL '1 year')
- SUBSTRING(col, 1, 2) → SUBSTRING(col FROM 1 FOR 2)
- YEAR(date) → EXTRACT(YEAR FROM date)
- Subquery aliases: AS required in PostgreSQL
"""

import re
import json

def convert_mssql_to_postgresql(query):
    """Convert MSSQL query syntax to PostgreSQL."""
    original = query
    
    # 1. DATEADD with negative days (DATEADD(dd, -N, date))
    query = re.sub(
        r'DATEADD\s*\(\s*dd\s*,\s*-([0-9]+)\s*,\s*((?:[^()]|\([^)]*\))*)\s*\)',
        r"(\2 - INTERVAL '\1 days')",
        query
    )
    
    # 2. DATEADD with months (DATEADD(mm, N, date))
    query = re.sub(
        r'DATEADD\s*\(\s*mm\s*,\s*([0-9]+)\s*,\s*((?:[^()]|\([^)]*\))*)\s*\)',
        r"(\2 + INTERVAL '\1 months')",
        query
    )
    
    # 3. DATEADD with years (DATEADD(yy, N, date))
    query = re.sub(
        r'DATEADD\s*\(\s*yy\s*,\s*([0-9]+)\s*,\s*((?:[^()]|\([^)]*\))*)\s*\)',
        r"(\2 + INTERVAL '\1 years')",
        query
    )
    
    # 4. DATEADD with positive days (DATEADD(dd, N, date))
    query = re.sub(
        r'DATEADD\s*\(\s*dd\s*,\s*([0-9]+)\s*,\s*((?:[^()]|\([^)]*\))*)\s*\)',
        r"(\2 + INTERVAL '\1 days')",
        query
    )
    
    # 5. SUBSTRING(col, start, len) → SUBSTRING(col FROM start FOR len)
    query = re.sub(
        r'SUBSTRING\s*\(\s*(\w+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*\)',
        r'SUBSTRING(\1 FROM \2 FOR \3)',
        query
    )
    
    # 6. YEAR(date) → EXTRACT(YEAR FROM date)
    query = re.sub(
        r'YEAR\s*\(\s*((?:[^()]|\([^)]*\))*)\s*\)',
        r'EXTRACT(YEAR FROM \1)',
        query
    )
    
    # 7. Fix subquery aliases like "c_orders (col1, col2)" → "c_orders AS c_orders (col1, col2)"
    # Pattern: ) as alias (col1, col2) → ) as alias (col1, col2) [PostgreSQL needs the AS]
    query = re.sub(
        r'\) as (\w+) \((.+?)\)',
        r') AS \1 (\2)',
        query,
        flags=re.IGNORECASE
    )
    
    return query

def convert_workload(input_file, output_file):
    """Convert entire workload file."""
    converted = []
    conversion_count = 0
    
    with open(input_file, 'r') as f:
        for line in f:
            if not line.strip():
                continue
                
            try:
                query_obj = json.loads(line)
                original = query_obj['query_string']
                converted_query = convert_mssql_to_postgresql(original)
                
                if original != converted_query:
                    conversion_count += 1
                
                query_obj['query_string'] = converted_query
                converted.append(json.dumps(query_obj))
                
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print(f"Line: {line[:100]}")
                continue
    
    with open(output_file, 'w') as f:
        for line in converted:
            f.write(line + '\n')
    
    print(f"Converted {conversion_count} queries")
    print(f"Output written to: {output_file}")

if __name__ == "__main__":
    # Convert the static workload
    convert_workload(
        "resources/workloads/tpc_h_static_100.json",
        "resources/workloads/tpc_h_static_100_postgresql.json"
    )
