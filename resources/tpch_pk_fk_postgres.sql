-- TPC-H Reference Integrity for PostgreSQL
-- Generated from dss.ri (TPCD Benchmark Version 8.0)
-- Schema: public (lowercase table/column names to match workload files)
-- Run after loading TPC-H data

BEGIN;

-- ============================================================
-- PRIMARY KEYS
-- ============================================================

-- Region: Primary Key
ALTER TABLE region ADD PRIMARY KEY (r_regionkey);

-- Nation: Primary Key
ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);

-- Part: Primary Key
ALTER TABLE part ADD PRIMARY KEY (p_partkey);

-- Supplier: Primary Key
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);

-- Partsupp: Primary Key (composite)
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);

-- Customer: Primary Key
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);

-- Orders: Primary Key
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);

-- Lineitem: Primary Key (composite)
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);

-- ============================================================
-- FOREIGN KEYS
-- ============================================================

-- Nation -> Region (N_REGIONKEY)
ALTER TABLE nation ADD CONSTRAINT nation_fk_region
    FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey);

-- Supplier -> Nation (S_NATIONKEY)
ALTER TABLE supplier ADD CONSTRAINT supplier_fk_nation
    FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey);

-- Partsupp -> Part (PS_PARTKEY)
ALTER TABLE partsupp ADD CONSTRAINT partsupp_fk_part
    FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey);

-- Partsupp -> Supplier (PS_SUPPKEY)
ALTER TABLE partsupp ADD CONSTRAINT partsupp_fk_supplier
    FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey);

-- Customer -> Nation (C_NATIONKEY)
ALTER TABLE customer ADD CONSTRAINT customer_fk_nation
    FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey);

-- Orders -> Customer (O_CUSTKEY)
ALTER TABLE orders ADD CONSTRAINT orders_fk_customer
    FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey);

-- Lineitem -> Orders (L_ORDERKEY)
ALTER TABLE lineitem ADD CONSTRAINT lineitem_fk_orders
    FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey);

-- Lineitem -> Partsupp (L_PARTKEY, L_SUPPKEY)
ALTER TABLE lineitem ADD CONSTRAINT lineitem_fk_partsupp
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey);

COMMIT;

-- ============================================================
-- RECOMMENDED INDEXES FOR TPC-H QUERIES
-- These are NOT created by default but significantly improve
-- TPC-H query performance by accelerating joins
-- ============================================================

-- Indexes on foreign keys (for join performance)
CREATE INDEX IF NOT EXISTS idx_nation_region ON nation (n_regionkey);
CREATE INDEX IF NOT EXISTS idx_supplier_nation ON supplier (s_nationkey);
CREATE INDEX IF NOT EXISTS idx_partsupp_part ON partsupp (ps_partkey);
CREATE INDEX IF NOT EXISTS idx_partsupp_supplier ON partsupp (ps_suppkey);
CREATE INDEX IF NOT EXISTS idx_customer_nation ON customer (c_nationkey);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (o_custkey);
CREATE INDEX IF NOT EXISTS idx_lineitem_orders ON lineitem (l_orderkey);
CREATE INDEX IF NOT EXISTS idx_lineitem_partsupp ON lineitem (l_partkey, l_suppkey);

-- Indexes on commonly filtered/sorted columns (TPC-H specific)
CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate ON lineitem (l_shipdate);
CREATE INDEX IF NOT EXISTS idx_lineitem_orderkey ON lineitem (l_orderkey);
CREATE INDEX IF NOT EXISTS idx_orders_orderdate ON orders (o_orderdate);
CREATE INDEX IF NOT EXISTS idx_part_type ON part (p_type);
CREATE INDEX IF NOT EXISTS idx_part_brand ON part (p_brand);
CREATE INDEX IF NOT EXISTS idx_part_size ON part (p_size);
CREATE INDEX IF NOT EXISTS idx_customer_mktsegment ON customer (c_mktsegment);

-- Composite indexes for TPC-H query patterns
CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate_discount ON lineitem (l_shipdate, l_discount);
CREATE INDEX IF NOT EXISTS idx_lineitem_partkey_shipdate ON lineitem (l_partkey, l_shipdate);
CREATE INDEX IF NOT EXISTS idx_orders_custkey_orderdate ON orders (o_custkey, o_orderdate);

-- ============================================================
-- STATISTICS UPDATE
-- After creating constraints and indexes, update statistics
-- ============================================================
ANALYZE VERBOSE;

-- Show created constraints
\dt
\di
