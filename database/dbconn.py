import os
import re
import time
import copy
import datetime
import logging
import subprocess
import traceback
from collections import defaultdict
from typing import Dict, Tuple, List

import psycopg2
import pyodbc
import sqlglot

import constants
from database.query_v5 import Query
from database.query_plan import QueryPlan_MSSQL, QueryPlanPG
from database.column import Column
from database.table import Table
from bandits.bandit_arm import BanditArm


class DBConnection():
    def __init__(self, db_conf_dict) -> None:
        """
        db_conf_dict keys
        db_type:
        database:
        server: hostname, for sqlserver
        host:
        port:
        """
        self.db_conf_dict = db_conf_dict
        self.connection = self.get_sql_connection(db_conf_dict)
        self.db_type = self.get_connection_type(self.connection)
        self.hypo_idx = db_conf_dict['hypo_idx']
        if self.hypo_idx:
            self.set_up_hypopg()
        self.pk_columns_dict = {}
        self.tables_global = self.get_tables()

        self.sel_store = {}
        self.bandit_arm_store: Dict[str, BanditArm] = {}

        self.database = db_conf_dict['database']
        # self.benchmark_type = self.database[:-4].upper()
        if 'tpch' in self.database:
            self.benchmark_type = 'TPCH'
        elif 'tpcds' in self.database:
            self.benchmark_type = 'TPCDS'
        elif 'job' in self.database:
            self.benchmark_type = 'JOB'
        elif 'SSB' in self.database:
            self.benchmark_type = 'SSB'
        else:
            raise

        self.table_scan_times_hyp = copy.deepcopy(constants.TABLE_SCAN_TIMES[self.benchmark_type])
        self.conn_table_scan_time_dict = copy.deepcopy(constants.TABLE_SCAN_TIMES[self.benchmark_type])
        self.conn_cluster_idx_scan_time_dict = copy.deepcopy(constants.TABLE_SCAN_TIMES[self.benchmark_type])
        self.conn_noncluster_idx_scan_time_dict = copy.deepcopy(constants.TABLE_SCAN_TIMES[self.benchmark_type])

    def close_sql_connection(self):
        """
        Take care of the closing process of the SQL connection
        :param connection: sql_connection
        :return: operation status
        """
        return self.connection.close()

    def set_up_hypopg(self):
        self.hypopg_v = self.get_hypopg_version()

        hypopg_list_indexes_fn = "hypopg_list_indexes"
        if self.hypopg_v > '1.1.3':
            pass
        else:
            hypopg_list_indexes_fn += "()"
        self.hypopg_list_indexes_fn = hypopg_list_indexes_fn

    def get_hypopg_version(self) -> bool:
        cur = self.connection.cursor()
        cur.execute("SELECT default_version FROM pg_available_extensions WHERE name ='hypopg';")
        version = cur.fetchone()[0]
        return version

    def get_current_index(self):
        query = f"SELECT * FROM {self.hypopg_list_indexes_fn};"
        cursor = self.connection.cursor()
        cursor.execute(query)
        indexes = cursor.fetchall()
        return indexes

    def create_index_v1(self, schema_name, tbl_name, col_names,
                        idx_name, include_cols=()):
        """
        Create an index on the given table.
        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param tbl_name: name of the database table
        :param col_names: string list of column names
        :param idx_name: name of the index
        :param include_cols: columns that needed to added as includes
        """
        cursor = self.connection.cursor()
        if self.db_type == "MSSQL":
            if include_cols:
                query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)})" \
                        f" INCLUDE ({', '.join(include_cols)})"
            else:
                query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)})"
            cursor.execute("SET STATISTICS XML ON")
            cursor.execute(query)
            stat_xml = cursor.fetchone()[0]
            cursor.execute("SET STATISTICS XML OFF")
            self.connection.commit()

            logging.info(f"Added: {idx_name}")
            # Return the current reward
            query_plan = QueryPlan_MSSQL(stat_xml)
            if constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_ELAPSED_TIME:
                return float(query_plan.stmt_elapsed_time)
            elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_CPU_TIME:
                return float(query_plan.cpu_time)
            elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_SUB_TREE_COST:
                return float(query_plan.est_statement_cost)
            else:
                return float(query_plan.est_statement_cost)
        elif self.db_type == "postgresql":
            if self.hypo_idx:
                if include_cols:
                    idx_query = f"CREATE INDEX ON {tbl_name} ({', '.join(col_names)})" \
                            f" INCLUDE ({', '.join(include_cols)})"
                else:
                    idx_query = f"CREATE INDEX ON {tbl_name} ({', '.join(col_names)})"
                query = f"SELECT * FROM hypopg_create_index('{idx_query}');"
                cursor.execute(query)
                res = cursor.fetchone()
                self.connection.commit()
                oid, hypo_idx_name = res
                hypo_exp_qry = "explain (format json) " + query
                cursor.execute(hypo_exp_qry)
                res2 = cursor.fetchone()
                self.connection.commit()
                est_hypopg_idx_creation_cost = res2[0][0]["Plan"]['Total Cost']
                logging.info(f"Added: {idx_name}")
                return est_hypopg_idx_creation_cost, oid, hypo_idx_name
            else:
                if include_cols:
                    query = f"CREATE INDEX \"{idx_name}\" ON {tbl_name} ({', '.join(col_names)})" \
                            f" INCLUDE ({', '.join(include_cols)})"
                else:
                    query = f"CREATE INDEX \"{idx_name}\" ON {tbl_name} ({', '.join(col_names)})"
                idx_creation_start = time.time()
                cursor.execute(query)
                self.connection.commit()
                idx_creation_end = time.time()
                index_creation_time = idx_creation_end - idx_creation_start
                return index_creation_time, -1, ''
        else:
            raise NotImplementedError

    """Below 2 functions are used by DTARunner"""

    def create_index_v2(self, query):
        """
        Create an index on the given table.
        :param connection: sql_connection
        :param query: query for index creation
        """
        cursor = self.connection.cursor()
        cursor.execute("SET STATISTICS XML ON")
        cursor.execute(query)
        stat_xml = cursor.fetchone()[0]
        cursor.execute("SET STATISTICS XML OFF")
        self.connection.commit()
        query_plan = QueryPlan_MSSQL(stat_xml)

        if constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_ELAPSED_TIME:
            return float(query_plan.stmt_elapsed_time)
        elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_CPU_TIME:
            return float(query_plan.cpu_time)
        elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_SUB_TREE_COST:
            return float(query_plan.est_statement_cost)
        else:
            return float(query_plan.est_statement_cost)

    def create_statistics(self, query):
        """
        Create an index on the given table.
        :param connection: sql_connection
        :param query: query for index creation
        """
        cursor = self.connection.cursor()
        start_time_execute = datetime.datetime.now()
        cursor.execute(query)
        self.connection.commit()
        end_time_execute = datetime.datetime.now()
        time_apply = (end_time_execute - start_time_execute).total_seconds()
        # Return the current reward
        return time_apply

    def bulk_create_indexes(self, schema_name, bandit_arm_list, db_type="postgresql") -> Dict[str, float]:
        """
        This uses create_index method to create multiple indexes at once.
        This is used when a super arm is pulled.
        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: list of BanditArm objects
        :return: cost (regret)
        """
        idx_creation_cost_dict = {}
        for index_name, bandit_arm in bandit_arm_list.items():
            if db_type == "MSSQL":
                idx_creation_cost_dict[index_name] = self.create_index_v1(schema_name,
                                                                          bandit_arm.table_name,
                                                                          bandit_arm.index_cols,
                                                                          bandit_arm.index_name,
                                                                          bandit_arm.include_cols)
                self.set_arm_size(bandit_arm)
            elif db_type == "postgresql":
                idx_creation_cost_dict[index_name], oid, hypo_idx_name = self.create_index_v1(schema_name,
                                                                                              bandit_arm.table_name,
                                                                                              bandit_arm.index_cols,
                                                                                              bandit_arm.index_name,
                                                                                              bandit_arm.include_cols)
                bandit_arm.oid = oid
                bandit_arm.hypopg_idx_name = hypo_idx_name
                self.set_arm_size(bandit_arm)
            else:
                raise
        return idx_creation_cost_dict

    def drop_all_indexes(self):
        cur = self.connection.cursor()
        if self.db_type == 'postgresql':
            if not self.hypo_idx:
                query = "select drop_all_indexes();"
            else:
                query = "select hypopg_reset();"
            try:
                cur.execute(query)
                self.connection.commit()
            except psycopg2.OperationalError:
                raise "error executing drop_all_indexes() in pg"
        elif self.db_type == 'MSSQL':
            query = "EXEC dbo.DropAllSecondaryIndexes;"
            try:
                cur.execute(query)
                self.connection.commit()
            except pyodbc.DatabaseError:
                raise "error executing drop_all_indexes() in sql server"
        else:
            raise NotImplementedError(f"drop_all_indexes() for {self.db_type} not implemented")

    def drop_index(self, schema_name, bandit_arm, db_type="postgresql"):
        """
        Drops the index on the given table with given name.
        :param connection: sql_connection
        :param schema_name: name of the database schema
        :return:
        """
        tbl_name = bandit_arm.table_name
        idx_name = bandit_arm.index_name
        oid = bandit_arm.oid

        if db_type == "MSSQL":
            query = f"DROP INDEX {schema_name}.{tbl_name}.{idx_name}"
        elif db_type == "postgresql":
            if self.hypo_idx:
                query = f"SELECT * FROM hypopg_drop_index({oid})"
            else:
                query = f"drop index \"{idx_name}\";"

        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        logging.info(f"removed: {idx_name}")
        logging.debug(query)

    def bulk_drop_index(self, schema_name, bandit_arm_list):
        """
        Drops the index for all given bandit arms.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: list of bandit arms
        :return:
        """
        for _, bandit_arm in bandit_arm_list.items():
            self.drop_index(schema_name, bandit_arm)

    def simple_execute(self, query):
        """
        :param connection: sql_connection
        :param query: query to execute
        :return:
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        logging.debug(query)

    def execute_query_v1(self, query) -> Tuple[float, dict, dict]:
        """
        This executes the given query and return the time took to run the query.
        This Clears the cache and executes the query and return the time taken to run the query.
        This return the "elapsed time" by default.
        However its possible to get the cpu time by setting the is_cpu_time to True.

        :param connection: sql_connection
        :param query: query that need to be executed
        :return: time taken for the query
        """
        cost_type_current_execution = constants.COST_TYPE_CURRENT_CREATION
        try:
            cursor = self.connection.cursor()
            # get query plan instance wrt db_type
            if self.db_type == "MSSQL":
                cursor.execute("CHECKPOINT;")
                cursor.execute("DBCC DROPCLEANBUFFERS;")
                cursor.execute("SET STATISTICS XML ON")
                cursor.execute(query)
                cursor.nextset()
                stat_xml = cursor.fetchone()[0]
                cursor.execute("SET STATISTICS XML OFF")
                query_plan = QueryPlan_MSSQL(stat_xml)
            elif self.db_type == "postgresql":
                query = self.fix_tsql_to_psql(query)
                if self.hypo_idx:
                    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
                else:
                    cursor.execute(f"EXPLAIN (FORMAT JSON, ANALYZE) {query}")
                stat_xml = cursor.fetchone()[0][0]
                query_plan = QueryPlanPG(stat_xml, self)
                cost_type_current_execution = constants.COST_TYPE_SUB_TREE_COST
            else:
                raise NotImplementedError(f"database type {self.db_type} not supported yet.")

            # return wrt chosen cost type
            if cost_type_current_execution == constants.COST_TYPE_ELAPSED_TIME:
                qcost = query_plan.stmt_elapsed_time
            elif cost_type_current_execution == constants.COST_TYPE_CPU_TIME:
                qcost = query_plan.cpu_time
            elif cost_type_current_execution == constants.COST_TYPE_SUB_TREE_COST:
                qcost = query_plan.est_stmt_cost
            else:
                qcost = query_plan.est_statement_cost
            return float(qcost), query_plan.non_clustered_index_usage, query_plan.clustered_index_usage, query_plan.table_scans
        except psycopg2.OperationalError:
            print("Exception when executing query: ", query)
            traceback.print_exc()
            # return 0, [], []
            raise

    def get_table_row_count(self, schema_name, tbl_name):
        if self.db_type == "MSSQL":
            row_query = f"""SELECT SUM (Rows)
                                FROM sys.partitions
                                WHERE index_id IN (0, 1)
                                And OBJECT_ID = OBJECT_ID('{schema_name}.{tbl_name}');"""
        elif self.db_type == "postgresql":
            row_query = f"""SELECT reltuples AS row_count
                                FROM pg_class
                                WHERE relkind = 'r' AND relname = '{tbl_name.lower()}';"""

        cursor = self.connection.cursor()
        cursor.execute(row_query)
        row_count = cursor.fetchone()[0]
        return row_count

    def transform_hypopg_index_usage(self, non_clustered_index_usage, bandit_arm_list_copy):
        non_clustered_index_usage_new = non_clustered_index_usage.copy()
        for i in range(len(non_clustered_index_usage)):
            usage_tuple = non_clustered_index_usage_new[i]
            usage_list = list(usage_tuple)
            hypo_idx_name = usage_tuple[0]
            new_idx_name = transform_hypopg_index_name(hypo_idx_name, bandit_arm_list_copy)
            usage_list[0] = new_idx_name
            non_clustered_index_usage_new[i] = tuple(usage_list)
        return non_clustered_index_usage_new

    def create_query_drop_v3(self, schema_name, bandit_arm_list: List[BanditArm],
                             arm_list_to_add, arm_list_to_delete, queries: List[Query]):
        """
        This method aggregate few functions of the sql helper class.
            1. This method create the indexes related to the given bandit arms;
            2. Execute all the queries in the given list;
            3. Clean (drop) the created indexes;
            4. Finally returns the time taken to run all the queries.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: arms considered in this round
        :param arm_list_to_add: arms that need to be added in this round
        :param arm_list_to_delete: arms that need to be removed in this round
        :param queries: queries that should be executed
        :return:
        """
        self.bulk_drop_index(schema_name, arm_list_to_delete)
        cur_round_idx_creation_cost_dict = self.bulk_create_indexes(schema_name, arm_list_to_add)

        w_execute_cost = 0
        # dict indexed by index name and map to a tuple indicating benefit and creation cost of the index
        arm_reward_dict: Dict[str, Tuple[float, float]] = {}
        for query in queries:
            q_cost, non_clustered_index_usage0, clustered_index_usage, table_scans = self.execute_query_v1(query.query_string)
            # query_text = format_query_string(query.query_string)
            logging.info(f"Query {query.id} cost: {q_cost}")
            w_execute_cost += q_cost

            if self.hypo_idx:
                # update non_clustered_index_usage for adjusting index name for hypo_idx_name from hypopg
                bandit_arm_list_copy = bandit_arm_list
                bandit_arm_list_copy.update(arm_list_to_add)
                non_clustered_index_usage = self.transform_hypopg_index_usage(non_clustered_index_usage0, bandit_arm_list_copy)
            else:
                non_clustered_index_usage = non_clustered_index_usage0.copy()

            non_clustered_index_usage = merge_index_use(non_clustered_index_usage)
            clustered_index_usage = merge_index_use(clustered_index_usage)

            for tscan in table_scans:
                tbl_name = tscan[0].upper()
                cost = tscan[constants.COST_TYPE_CURRENT_EXECUTION]
                self.conn_table_scan_time_dict[tbl_name].append(cost)
            # update table_scan_times dict for the query instance and db connection instance from index usages
            for index_scan_info in clustered_index_usage:  # fill current_clustered_index_scan_costs
                idx_name = index_scan_info[0]
                if '_pkey' in idx_name:
                    table_name = idx_name.split('_pkey')[0].upper()
                else:
                    table_name = idx_name.split('_')[1].upper()
                cidx_scan_cost = index_scan_info[constants.COST_TYPE_CURRENT_EXECUTION]
                self.conn_cluster_idx_scan_time_dict[table_name].append(cidx_scan_cost)

            nc_idx_acccess_table_counts = {}
            for index_use in non_clustered_index_usage:  # for each non_clustered_index usage
                index_name = index_use[0]
                table_name = bandit_arm_list[index_name].table_name.upper()

                # idx_creation_cost = idx_creation_cost_dict[index_name] if index_name in idx_creation_cost_dict else 0.0
                idx_scan_cost = index_use[constants.COST_TYPE_CURRENT_EXECUTION]
                self.conn_noncluster_idx_scan_time_dict[table_name].append(idx_scan_cost)

                if table_name in nc_idx_acccess_table_counts:
                    nc_idx_acccess_table_counts[table_name] += 1
                else:
                    nc_idx_acccess_table_counts[table_name] = 1

                # compute reward
                if len(self.conn_table_scan_time_dict[table_name]) > 0:
                    # assert len(self.conn_table_scan_time_dict[table_name]) == 1
                    tbl_scan_cost = max(self.conn_table_scan_time_dict[table_name])
                else:
                    tbl_scan_cost = max(self.conn_noncluster_idx_scan_time_dict[table_name])
                gain_q_i = tbl_scan_cost - idx_scan_cost
                # gain_q_i = gain_q_i/nc_idx_acccess_table_counts[table_name]
                # if table_name in self.conn_cluster_idx_scan_time_dict:
                #     gain_q_i -= tbl_scan_cost /nc_idx_acccess_table_counts[table_name]
                # rew = gain_q_i - idx_creation_cost_dict[index_name]
                if index_name not in arm_reward_dict:  # the index is used but not created from the query
                    arm_reward_dict[index_name] = [gain_q_i, 0]
                else:  # the index is used and created from this query
                    arm_reward_dict[index_name][0] += gain_q_i
                    # arm_reward_dict[index_name][1] -= idx_creation_cost
        for key in cur_round_idx_creation_cost_dict:
            if key in arm_reward_dict:
                arm_reward_dict[key][1] += -1*cur_round_idx_creation_cost_dict[key]
            else:
                arm_reward_dict[key] = [0, -1*cur_round_idx_creation_cost_dict[key]]
        logging.info(f"Index creation cost: {sum(cur_round_idx_creation_cost_dict.values())}")
        logging.info(f"Time taken to run the queries: {w_execute_cost} after the configuration change")

        return w_execute_cost, cur_round_idx_creation_cost_dict, arm_reward_dict

    def hyp_create_index_v1(self, schema_name, tbl_name, col_names,
                            idx_name, include_cols=()):
        """
        Create an hypothetical index on the given table.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param tbl_name: name of the database table
        :param col_names: string list of column names
        :param idx_name: name of the index
        :param include_cols: columns that needed to be added as includes
        """
        if self.db_type == "MSSQL":
            if include_cols:
                query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)}) " \
                        f"INCLUDE ({', '.join(include_cols)}) WITH STATISTICS_ONLY = -1"
            else:
                query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)}) " \
                        f"WITH STATISTICS_ONLY = -1"
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            logging.debug(query)
            logging.info(f"Added HYP: {idx_name}")
            return 0
        elif self.db_type == "postgresql":
            if include_cols:
                query = f"CREATE INDEX {idx_name} ON {tbl_name} ({', '.join(col_names)})" \
                        f" INCLUDE ({', '.join(include_cols)})"
            else:
                query = f"CREATE INDEX {idx_name} ON {tbl_name} ({', '.join(col_names)})"
            query = f"SELECT * FROM hypopg_create_index('{query}');"

            cursor = self.connection.cursor()
            cursor.execute(query)
            oid = cursor.fetchone()[0]
            self.connection.commit()
            logging.info(f"Added HYP: {idx_name}")
            # Return the current reward
            # query_plan = QueryPlanPG(stat_xml)
            return oid

    def hyp_bulk_create_indexes(self, schema_name, bandit_arm_list):
        """
        This uses hyp_create_index method to create multiple indexes at once.
        This is used when a super arm is pulled.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: list of BanditArm objects
        :return: index name list
        """
        # logging.info(f"The length of `bandit_arm_list` is {len(bandit_arm_list)}.")

        cost = {}
        for index_name, bandit_arm in bandit_arm_list.items():
            oid = self.hyp_create_index_v1(schema_name, bandit_arm.table_name, bandit_arm.index_cols,
                                           bandit_arm.index_name, bandit_arm.include_cols)
            bandit_arm.oid = oid
            cost[index_name] = 0
        return cost

    def hyp_enable_index(self):
        """
        This enables the hypothetical indexes for the given connection.
        This will be enabled for a given connection and all hypothetical queries
        must be executed via the same connection.

        :param connection: connection for which hypothetical indexes will be enabled
        """
        query = """SELECT dbid = Db_id(),
                        objectid = object_id,
                        indid = index_id
                    FROM   sys.indexes
                    WHERE  is_hypothetical = 1;"""
        cursor = self.connection.cursor()
        cursor.execute(query)
        result_rows = cursor.fetchall()
        for result_row in result_rows:
            query_2 = f"DBCC AUTOPILOT(0, {result_row[0]}, {result_row[1]}, {result_row[2]})"
            cursor.execute(query_2)

    def hyp_execute_query(self, query, db_type="postgresql"):
        """
        This hypothetically executes the given query and return the estimated sub tree cost.
        If required we can add the operation cost as well.
        However, most of the cases operation cost at the top level is 0.

        :param connection: sql_connection
        :param query: query that need to be executed
        :return: estimated sub tree cost
        """
        if db_type == "MSSQL":
            self.hyp_enable_index()
            cursor = self.connection.cursor()
            cursor.execute("SET AUTOPILOT ON")
            cursor.execute(query)
            stat_xml = cursor.fetchone()[0]
            cursor.execute("SET AUTOPILOT OFF")
            query_plan = QueryPlan_MSSQL(stat_xml)
            return float(
                query_plan.est_statement_cost), query_plan.non_clustered_index_usage, query_plan.clustered_index_usage
        elif db_type == "postgresql":
            cursor = self.connection.cursor()
            cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
            stat_xml = cursor.fetchone()[0][0]["Plan"]
            return stat_xml["Total Cost"]

    def hyp_create_query_drop_v1(self, schema_name, bandit_arm_list, arm_list_to_add, arm_list_to_delete, queries):
        """
        This method aggregate few functions of the sql helper class.
            1. This method create the hypothetical indexes related to the given bandit arms;
            2. Execute all the queries in the given list;
            3. Clean (drop) the created hypothetical indexes;
            4. Finally returns the sum of estimated sub tree cost for all queries.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: contains the information related to indexes that is considered in this round
        :param arm_list_to_add: arms that need to be added in this round
        :param arm_list_to_delete: arms that need to be removed in this round
        :param queries: queries obj list that should be executed
        :return: sum of estimated sub tree cost for all queries
        """
        self.bulk_drop_index(schema_name, arm_list_to_delete)
        creation_cost = self.hyp_bulk_create_indexes(schema_name, arm_list_to_add)
        estimated_sub_tree_cost = 0
        arm_rewards = {}
        for query in queries:
            cost, index_seeks, clustered_index_scans = self.hyp_execute_query(query.query_string)
            estimated_sub_tree_cost += float(cost)
            if clustered_index_scans:
                for index_scan in clustered_index_scans:
                    if len(query.table_scan_times_hyp[index_scan[0]]) < constants.TABLE_SCAN_TIME_LENGTH:
                        query.table_scan_times_hyp[index_scan[0]].append(index_scan[3])

            if index_seeks:
                for index_seek in index_seeks:
                    table_scan_time_hyp = query.table_scan_times_hyp[bandit_arm_list[index_seek[0]].table_name]
                    arm_rewards[index_seek[0]] = max(table_scan_time_hyp) - index_seek[3]

        for key in creation_cost:
            creation_cost[key] = max(query.table_scan_times_hyp[bandit_arm_list[key].table_name])
            if key in arm_rewards:
                arm_rewards[key] += -1 * creation_cost[key]
            else:
                arm_rewards[key] = -1 * creation_cost[key]
        logging.info(f"Time taken to run the queries: {estimated_sub_tree_cost}")
        return estimated_sub_tree_cost, creation_cost, arm_rewards

    def hyp_create_query_drop_v2(self, schema_name, bandit_arm_list, arm_list_to_add, arm_list_to_delete, queries):
        """
        This method aggregate few functions of the sql helper class.
            1. This method create the indexes related to the given bandit arms;
            2. Execute all the queries in the given list;
            3. Clean (drop) the created indexes;
            4. Finally returns the time taken to run all the queries.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: arms considered in this round
        :param arm_list_to_add: arms that need to be added in this round
        :param arm_list_to_delete: arms that need to be removed in this round
        :param queries: queries that should be executed
        :return:
        """
        self.bulk_drop_index(schema_name, arm_list_to_delete)
        creation_cost = self.hyp_bulk_create_indexes(schema_name, arm_list_to_add)
        execute_cost = 0
        arm_rewards = {}
        for query in queries:
            time, non_clustered_index_usage, clustered_index_usage = self.hyp_execute_query(query.query_string)
            execute_cost += time
            if clustered_index_usage:
                for index_scan in clustered_index_usage:
                    # (0814): newly added.
                    table_name = index_scan[0].upper()
                    if len(query.table_scan_times_hyp[table_name]) < constants.TABLE_SCAN_TIME_LENGTH:
                        query.table_scan_times_hyp[table_name].append(index_scan[constants.COST_TYPE_SUB_TREE_COST])
                        query.table_scan_times_hyp[table_name].append(index_scan[constants.COST_TYPE_SUB_TREE_COST])
            if non_clustered_index_usage:
                for index_use in non_clustered_index_usage:
                    re.findall(r"btree_(.*)", "<666>btree_supplier_suppkey")
                    index_name = index_use[0]
                    # (0814): newly added.
                    table_name = bandit_arm_list[index_name].table_name.upper()
                    if len(query.table_scan_times_hyp[table_name]) < constants.TABLE_SCAN_TIME_LENGTH:
                        query.index_scan_times_hyp[table_name].append(index_use[constants.COST_TYPE_SUB_TREE_COST])
                    table_scan_time = query.table_scan_times_hyp[table_name]
                    if len(table_scan_time) > 0:
                        temp_reward = max(table_scan_time) - index_use[constants.COST_TYPE_SUB_TREE_COST]
                    elif len(query.table_scan_times_hyp[table_name]) > 0:
                        temp_reward = max(query.table_scan_times_hyp[table_name]) - index_use[constants.COST_TYPE_SUB_TREE_COST]
                    else:
                        logging.error(f"Queries without index scan information {query.id}")
                        raise Exception
                    if index_name not in arm_rewards:
                        arm_rewards[index_name] = [temp_reward, 0]
                    else:
                        arm_rewards[index_name][0] += temp_reward

        for key in creation_cost:
            if key in arm_rewards:
                arm_rewards[key][1] += -1 * creation_cost[key]
            else:
                arm_rewards[key] = [0, -1 * creation_cost[key]]
        logging.info(f"Time taken to run the queries: {execute_cost}")
        return execute_cost, creation_cost, arm_rewards

    def hyp_create_query_drop_new(self, schema_name, bandit_arm_list,
                                  arm_list_to_add, arm_list_to_delete, queries, execute_cost_no_index):
        """
        This method aggregate few functions of the sql helper class.
            1. This method create the indexes related to the given bandit arms;
            2. Execute all the queries in the given list;
            3. Clean (drop) the created indexes;
            4. Finally returns the time taken to run all the queries.

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: arms considered in this round
        :param arm_list_to_add: arms that need to be added in this round
        :param arm_list_to_delete: arms that need to be removed in this round
        :param queries: queries that should be executed
        :return:
        """
        # (0814): newly added.
        # execute_cost_no_index = 0
        # for query in queries:
        #     time = hyp_execute_query(connection, query.query_string)
        #     execute_cost_no_index += time

        self.bulk_drop_index(self.connection, schema_name, arm_list_to_delete)
        creation_cost = self.hyp_bulk_create_indexes(schema_name, arm_list_to_add)
        # logging.info(f"L632, The size of delete ({len(arm_list_to_delete)}) and add ({len(arm_list_to_add)}).")

        execute_cost = 0
        arm_rewards = dict()

        # indexes = get_current_index(connection)
        # logging.info(f"L639, The list of the current indexes ({len(indexes)}) is: {indexes}.")

        time_split = list()
        for query in queries:
            # (1016): newly modified. `* query.freq`
            time = self.hyp_execute_query(query.query_string) * query.freq
            time_split.append(time)
            execute_cost += time

        for arm in bandit_arm_list.keys():
            # arm_rewards[arm] = [execute_cost_no_index - execute_cost, 0]
            arm_rewards[arm] = [1 - execute_cost / execute_cost_no_index, 0]

        for key in creation_cost:
            if key in arm_rewards:
                # (0815): newly added.
                creation = 0
                arm_rewards[key][1] += creation
            else:
                arm_rewards[key] = [0, -creation]

        logging.info(f"Time taken to run the queries: {execute_cost}")
        return time_split, execute_cost, creation_cost, arm_rewards

    def get_all_columns(self):
        """
        Get all column in the database of the given connection.
        Note that the connection here is directly pointing to a specific database of interest.

        :param connection: Sql connection
        :param db_type:
        :return: dictionary of lists - columns, number of columns
        """
        if self.db_type == "MSSQL":
            query = """SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS;"""
        elif self.db_type == "postgresql":
            query = """SELECT TABLE_NAME, COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_schema = 'public' AND TABLE_NAME != 'hypopg_list_indexes';"""

        columns = defaultdict(list)
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        for result in results:
            columns[result[0]].append(result[1])

        return columns, len(results)

    def get_all_columns_v2(self):
        """
        Return all columns in table above 100 rows.

        :param connection: Sql connection
        :return: dictionary of lists - columns, number of columns
        """
        query = """SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS;"""
        columns = defaultdict(list)
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        count = 0
        for result in results:
            row_count = self.get_table_row_count(constants.SCHEMA_NAME, result[0])
            if row_count >= constants.SMALL_TABLE_IGNORE:
                columns[result[0]].append(result[1])
                count += 1

        return columns, count

    def get_current_pds_size(self, hypo=True):
        """
        Get the current size of all the physical design structures.

        :param connection: SQL Connection
        :param db_type:
        :return: size of all the physical design structures in MB
        """
        if self.db_type == "MSSQL":
            query = """SELECT (SUM(s.[used_page_count]) * 8) / 1024.0 AS size_mb
                    FROM sys.dm_db_partition_stats AS s;"""

            cursor = self.connection.cursor()
            cursor.execute(query)
            return cursor.fetchone()[0]

        elif self.db_type == "postgresql":
            # query = "SELECT COALESCE(SUM(pg_relation_size(indexrelid)/1024/1024), 0) AS total_size FROM pg_index JOIN pg_class ON pg_index.indexrelid = pg_class.oid WHERE pg_class.relkind = 'i' AND relname LIKE '%_pkey';"
            if not hypo:
                query = "select COALESCE(sum(pg_relation_size(indexrelid))/1024/1024, 0) AS total_index_size FROM pg_index JOIN pg_class ON pg_class.oid = pg_index.indexrelid JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE pg_namespace.nspname = 'public';"
                cursor = self.connection.cursor()
                try:
                    cursor.execute(query)
                except psycopg2.OperationalError:
                    print(f"error executing query {query}")
                return cursor.fetchone()[0]
            else:
                query = f"select COALESCE(sum(hypopg_relation_size(indexrelid))/1024/1024, 0) from {self.hypopg_list_indexes_fn};"
                cursor = self.connection.cursor()
                cursor.execute(query)
                res = cursor.fetchall()
                idx_size_list = [i[0] for i in res]
                size = sum(idx_size_list)
                # print('debug line')
                return size

    def get_primary_key(self, schema_name, table_name):
        """
        Get Primary key of a given table. Note tis might not be in order (not sure).

        :param connection: SQL Connection
        :param schema_name: schema name of table
        :param table_name: table name which we want to find the PK
        :return: array of columns
        """
        if table_name in self.pk_columns_dict:
            pk_columns = self.pk_columns_dict[table_name]
        else:
            pk_columns = list()

            if self.db_type == "MSSQL":
                query = f"""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + "." + QUOTENAME(CONSTRAINT_NAME)), "IsPrimaryKey") = 1 AND TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'"""

            elif self.db_type == "postgresql":
                query = f"""SELECT
                            a.attname AS column_name
                            FROM
                            pg_index AS i
                            JOIN
                            pg_attribute AS a ON a.attnum = ANY(i.indkey) AND a.attrelid = i.indrelid
                            WHERE
                            i.indisprimary
                            AND i.indrelid = '{table_name}'::regclass;"""

            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            for result in results:
                pk_columns.append(result[0])
            self.pk_columns_dict[table_name] = pk_columns
        return pk_columns

    def get_column_data_length_v2(self, table_name, col_names):
        """
        get the data length of given set of columns.

        :param connection: SQL Connection
        :param table_name: Name of the SQL table
        :param col_names: array of columns
        :return:
        """
        tables = self.tables_global
        varchar_count = 0
        column_data_length = 0

        for column_name in col_names:
            # (0801): newly added.
            column = tables[table_name.lower()].columns[column_name.lower()]
            if column.column_type == "varchar":
                varchar_count += 1
            column_data_length += column.column_size if column.column_size else 0

        if varchar_count > 0:
            variable_key_overhead = 2 + varchar_count * 2
            return column_data_length + variable_key_overhead
        else:
            return column_data_length

    def get_table_columns(self, table_name):
        """
        Get all the columns in the given table.

        :param connection: sql connection
        :param table_name: table name
        :return: dictionary of columns column name as the key
        """
        columns = {}
        cursor = self.connection.cursor()

        if self.db_type == "MSSQL":
            data_type_query = f"""SELECT COLUMN_NAME, DATA_TYPE, COL_LENGTH( '{table_name}' , COLUMN_NAME)
                                FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE
                                    TABLE_NAME = '{table_name}'"""
        elif self.db_type == "postgresql":
            data_type_query = f"""SELECT
                                    column_name,
                                    data_type,
                                    LENGTH(column_name)
                                FROM
                                    information_schema.columns
                                WHERE
                                    table_name = '{table_name}';"""

        cursor.execute(data_type_query)
        results = cursor.fetchall()
        variable_len_query = "SELECT "
        variable_len_select_segments = []
        variable_len_inner_segments = []
        varchar_ids = []
        for result in results:
            col_name = result[0]
            column = Column(table_name, col_name, result[1])
            column.set_max_column_size(int(result[2]))
            if result[1] != "varchar":
                column.set_column_size(int(result[2]))
            else:
                varchar_ids.append(col_name)
                variable_len_select_segments.append(f"""AVG(DL_{col_name})""")
                variable_len_inner_segments.append(f"""DATALENGTH({col_name}) DL_{col_name}""")
            columns[col_name] = column

        if len(varchar_ids) > 0:
            variable_len_query = variable_len_query + ', '.join(
                variable_len_select_segments) + " FROM (SELECT TOP (1000) " + ', '.join(
                variable_len_inner_segments) + f" FROM {table_name}) T"
            cursor.execute(variable_len_query)
            result_row = cursor.fetchone()
            for i in range(0, len(result_row)):
                columns[varchar_ids[i]].set_column_size(result_row[i])

        return columns

    def get_tables(self):
        """
        Get all tables as Table objects.

        :param connection: SQL Connection
        :return: Table dictionary with table name as the key
        """
        tables: Dict[str, Table] = {}
        if self.db_type == 'MSSQL':
            get_tables_query = """SELECT TABLE_NAME
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_TYPE = 'BASE TABLE'"""
        elif self.db_type == 'postgresql':
            get_tables_query = """SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE' AND table_schema = 'public'"""
        else:
            raise NotImplementedError
        cursor = self.connection.cursor()
        cursor.execute(get_tables_query)
        results = cursor.fetchall()
        for result in results:
            table_name = result[0].upper()
            row_count = self.get_table_row_count(constants.SCHEMA_NAME, table_name)
            pk_columns = self.get_primary_key(constants.SCHEMA_NAME, table_name)

            tbl = Table(table_name, row_count, pk_columns)
            tables[table_name] = tbl
            tbl.size = self.get_table_size(table_name)
            tbl.set_columns(self.get_table_columns(table_name))
        return tables

    def get_estimated_size_of_index_v1(self, schema_name, tbl_name, col_names):
        """
        This helper method can be used to get a estimate size for a index.
        This simply multiply the column sizes with a estimated row count (need to improve further).

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param tbl_name: name of the database table
        :param col_names: string list of column names
        :return: estimated size in MB
        """
        if self.db_type == "MSSQL":
            # (0801): newly added.
            table = self.tables_global[tbl_name.lower()]
            header_size = 6
            nullable_buffer = 2
            primary_key = self.get_primary_key(schema_name, tbl_name)
            primary_key_size = self.get_column_data_length_v2(tbl_name, primary_key)
            col_not_pk = tuple(set(col_names) - set(primary_key))
            key_columns_length = self.get_column_data_length_v2(tbl_name, col_not_pk)
            index_row_length = header_size + primary_key_size + key_columns_length + nullable_buffer
            row_count = table.table_row_count
            estimated_size = row_count * index_row_length
            estimated_size = estimated_size / float(1024 * 1024)
            max_column_length = self.get_max_column_data_length_v2(tbl_name, col_names)
            if max_column_length > 1700:
                print(f"Index going past 1700: {col_names}")
                estimated_size = 99999999
            logging.debug(f"{col_names} : {estimated_size}")
        elif self.db_type == "postgresql":
            cursor = self.connection.cursor()

            query = f"CREATE INDEX ON {tbl_name} ({', '.join(col_names)})"
            query = f"SELECT * FROM hypopg_create_index('{query}');"

            cursor.execute(query)
            oid = cursor.fetchone()[0]

            query = f"""SELECT * FROM hypopg_relation_size({oid});"""

            cursor.execute(query)
            estimated_size = cursor.fetchone()[0] / float(1024 * 1024)

            query = f"SELECT * FROM hypopg_drop_index({oid});"
            cursor.execute(query)
        else:
            raise NotImplementedError(f"get_estimated_size_of_index_v1() for {self.db_type} not implemented yet.")

        return estimated_size

    def get_max_column_data_length_v2(self, table_name, col_names):
        tables = self.tables_global
        column_data_length = 0
        for column_name in col_names:
            # (0801): newly added.
            column = tables[table_name.lower()].columns[column_name.lower()]
            column_data_length += column.max_column_size if column.max_column_size else 0
        return column_data_length

    def get_query_plan(self, query):
        """
        This returns the XML query plan of  the given query.

        :param connection: sql_connection
        :param query: sql query for which we need the query plan
        :param db_type:
        :return: XML query plan as a String
        """
        if self.db_type == "MSSQL":
            cursor = self.connection.cursor()
            cursor.execute("SET SHOWPLAN_XML ON;")
            cursor.execute(query)
            query_plan = cursor.fetchone()[0]
            cursor.execute("SET SHOWPLAN_XML OFF;")
        elif self.db_type == "postgresql":
            cursor = self.connection.cursor()
            query2 = self.fix_tsql_to_psql(query)
            if self.hypo_idx:
                cursor.execute(f"EXPLAIN (FORMAT JSON) {query2}")
            else:
                cursor.execute(f"EXPLAIN (FORMAT JSON, ANALYZE) {query2}")
            query_plan = cursor.fetchone()[0]
        else:
            raise NotImplementedError

        return query_plan

    def get_selectivity_v3(self, query_str: str, predicates):
        """
        Return the selectivity of the given query.

        :param connection: sql connection
        :param query: sql query for which predicates will be identified
        :param predicates: predicates of that query
        :param db_type:
        :return: Predicates list
        """
        query_plan_string = self.get_query_plan(query_str)
        read_rows = {}

        db_type = self.db_type
        # plan_load = "/data/wz/index/data_resource/query_plan.xml"
        # with open(plan_load, "r") as rf:
        #     query_plan_string = rf.readlines()
        # query_plan_string = "".join(query_plan_string)

        if query_plan_string != "":
            if db_type == "MSSQL":
                query_plan = QueryPlan_MSSQL(query_plan_string)
            elif db_type == "postgresql":
                if len(query_plan_string) == 1:
                    query_plan_string = query_plan_string[0]
                else:
                    raise
                query_plan = QueryPlanPG(query_plan_string, self)
            else:
                raise NotImplementedError(f"get_selectivity_v3() for {self.db_type} not implemented yet")

            tables = predicates.keys()
            # for table in tables:
            #     read_rows[table] = 1000000000
            #
            # for index_scan in query_plan.clustered_index_usage:
            #     if index_scan[0] not in read_rows:
            #         read_rows[index_scan[0]] = 1000000000
            #     read_rows[index_scan[0]] = min(float(index_scan[5]), read_rows[index_scan[0]])

            # e.g., {'LINEITEM': {'L_SHIPDATE': 'r'}}

            query_table_selectivity = {t.upper(): 0.0 for t in tables}
            if len(query_plan.clustered_index_usage) == 0\
                and len(query_plan.non_clustered_index_usage) == 0:
                return query_table_selectivity
            else:
                c_sel_list_dict = self.get_sel_list_dict_from_index_usage(query_plan.clustered_index_usage)
                nc_sel_list_dict = self.get_sel_list_dict_from_index_usage(query_plan.non_clustered_index_usage)
                ts_sel_list_dict = self.get_sel_list_dict_from_table_scan(query_plan.table_scans)
                sel_list_dict = {}
                for t in set(c_sel_list_dict.keys()) | set(nc_sel_list_dict) | set(ts_sel_list_dict):
                    sel_list_dict[t]=[]
                    if t in c_sel_list_dict:
                        sel_list_dict[t].extend(c_sel_list_dict[t])
                    if t in nc_sel_list_dict:
                        sel_list_dict[t].extend(nc_sel_list_dict[t])
                    if t in ts_sel_list_dict:
                        sel_list_dict[t].extend(ts_sel_list_dict[t])

                query_table_selectivity = {k: 1.0*sum(v)/len(v) for k, v in sel_list_dict.items()}
                return query_table_selectivity
        else:
            return 1

    def get_sel_list_dict_from_index_usage(self, index_usage_list):
        sel_list_dict: Dict[str, List[float]] = {}
        if len(index_usage_list) > 0:
            for usage in index_usage_list:
                idx_name = usage[0]
                if '_pkey' in idx_name:
                    tbl_name = idx_name.split('_pkey')[0].upper()
                else:
                    tbl_name = idx_name.split('_')[1].upper()
                    if tbl_name == 'WEB':
                        if 'sales' in idx_name:
                            tbl_name = 'WEB_SALES'
                        elif 'returns' in  idx_name:
                            tbl_name = 'WEB_RETURNS'
                    elif tbl_name == 'STORE':
                        if 'sales' in idx_name:
                            tbl_name = 'STORE_SALES'

                    # tbl_name = idx_name.split('_bree')
                rows_out = usage[-1]
                rows_in = usage[-2]
                sel = 1.0 * rows_out / rows_in
                if tbl_name in sel_list_dict:
                    sel_list_dict[tbl_name].append(sel)
                else:
                    sel_list_dict[tbl_name] = [sel]
        return sel_list_dict

    def get_sel_list_dict_from_table_scan(self, table_scans: list):
        sel_list_dict: Dict[str, List[float]] = {}
        for table_scan in table_scans:
            tbl_name = table_scan[0].upper()
            row_out = table_scan[-1]
            row_in = table_scan[-2]
            sel = 1.0*row_out/row_in
            if tbl_name in sel_list_dict:
                sel_list_dict[tbl_name].append(sel)
            else:
                sel_list_dict[tbl_name] = [sel]
        return sel_list_dict

    def remove_all_non_clustered(self, schema_name):
        """
        Removes all non-clustered indexes from the database.

        :param connection: SQL Connection
        :param schema_name: schema name related to the index
        """
        query = """select i.name as index_name, t.name as table_name
                    from sys.indexes i, sys.tables t
                    where i.object_id = t.object_id and i.type_desc = 'NONCLUSTERED'"""
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        for result in results:
            self.drop_index(schema_name, result[1], result[0])

    def get_table_scan_times(self, query_string):
        query_table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[self.database])
        time, index_seeks, clustered_index_scans = self.execute_query_v1(query_string)
        if clustered_index_scans:
            for index_scan in clustered_index_scans:
                table_name = index_scan[0]
                if len(query_table_scan_times[table_name]) < constants.TABLE_SCAN_TIME_LENGTH:
                    query_table_scan_times[table_name].append(index_scan[constants.COST_TYPE_CURRENT_EXECUTION])
        return query_table_scan_times

    def get_table_scan_times_structure(self) -> Dict[str, list]:
        # query_table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES["TPCH"])
        # (0814): newly added.
        query_table_scan_times = dict()
        for table in self.tables_global:
            query_table_scan_times[table.upper()] = list()
        return query_table_scan_times

    def drop_all_dta_statistics(self):
        query_get_stat_names = """SELECT OBJECT_NAME(s.[object_id]) AS TableName, s.[name] AS StatName
                                    FROM sys.stats s
                                    WHERE OBJECTPROPERTY(s.OBJECT_ID,'IsUserTable') = 1 AND s.name LIKE '_dta_stat%';"""
        cursor = self.connection.cursor()
        cursor.execute(query_get_stat_names)
        results = cursor.fetchall()
        for result in results:
            self.drop_statistic(result[0], result[1])
        logging.info("Dropped all dta statistics")

    def drop_statistic(self, table_name, stat_name):
        query = f"DROP STATISTICS {table_name}.{stat_name}"
        cursor = self.connection.cursor()
        cursor.execute(query)
        cursor.commit()

    def set_arm_size(self, bandit_arm):
        if self.db_type == "MSSQL":
            query = f"""SELECT (SUM(s.[used_page_count]) * 8)/1024 AS IndexSizeMB
                        FROM sys.dm_db_partition_stats AS s
                        INNER JOIN sys.indexes AS i ON s.[object_id] = i.[object_id]
                            AND s.[index_id] = i.[index_id]
                        WHERE i.[name] = '{bandit_arm.index_name}'
                        GROUP BY i.[name]
                        ORDER BY i.[name]
                    """
        elif self.db_type == "postgresql":
            if self.hypo_idx:
                query = f"""SELECT * FROM hypopg_relation_size({bandit_arm.oid})"""
            else:
                query = f"select pg_relation_size(indexrelid) from pg_stat_all_indexes where indexrelname=('{bandit_arm.index_name}');"
        else:
            raise NotImplementedError(f"set_arm_size() for {self.db_type} is not implemented.")

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        bandit_arm.memory = result[0]

        return bandit_arm

    def get_table_size(self, tbl_name):
        """return the table size in kilo byte"""
        cursor = self.connection.cursor()
        if self.db_type == 'postgresql':
            get_tbl_size_query = f"select 1.0*pg_relation_size('{tbl_name}');"
            cursor.execute(get_tbl_size_query)
            res = cursor.fetchone()
            return float(res[0])
        elif self.db_type == 'MSSQL':
            get_tbl_size_query = f"EXEC sp_spaceused '{tbl_name}';"
            cursor.execute(get_tbl_size_query)
            res = cursor.fetchone()
            # the results is a 5-element tuple (index_name, rows, reserved, data, unused)
            return float(res[3])
        else:
            raise NotImplementedError(f"get_table_size() for {self.db_type} not implemented yet")

    def get_database_size(self):
        if self.db_type == "MSSQL":
            database_size = 10240
            try:
                query = "exec sp_spaceused @oneresultset = 1;"
                cursor = self.connection.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                database_size = float(result[4].split(" ")[0]) / 1024
            except Exception as e:
                logging.error("Exception when get_database_size: " + str(e))
        elif self.db_type == "postgresql":
            query = "SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;"
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()[0][:-2]
            database_size = int(result)

        return database_size

    @staticmethod
    def get_sql_connection(db_conf_dict: dict):
        """
        This method simply returns the sql connection based on the DB type
        and the connection settings defined in the `db.conf`.
        :return: connection
        """
        db_type = db_conf_dict["db_type"]
        database = db_conf_dict["database"]
        if db_type.lower() == 'mssql':
            server = os.uname()[1]
            # driver = db_config[db_type]['driver']
            # driver = 'ODBC Driver 18 for SQL Server'
            # TODO: make the user and PWD also configurable in the conf file
            dsn = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID=sa;PWD=Sql123456;'
            dsn += 'TrustServerCertificate=Yes;'
            return pyodbc.connect(dsn)
        elif db_type.lower() == 'postgresql':
            conn = psycopg2.connect(host='/tmp', port=51204, database=database.lower())
            return conn
        else:
            raise "unkown db type"

    def restart_db(self):
        if self.db_type == 'MSSQL':
            command1 = "net stop mssqlserver"
            command2 = "net start mssqlserver"
            with open(os.devnull, "w") as devnull:
                subprocess.run(command1, shell=True, stdout=devnull)
                time.sleep(60)
                subprocess.run(command2, shell=True, stdout=devnull)
            logging.info("Server Restarted")
            return
        elif self.db_type == 'postgresql':
            pass

    @staticmethod
    def fix_tsql_to_psql(query):
        query2 = sqlglot.transpile(query, read='tsql', write='postgres')[0]
        if 'YEAR(' in query2:
            query2 = re.sub(r'YEAR\((.*?)\)', r'EXTRACT(YEAR FROM \1)', query2)
        return query2

    @staticmethod
    def get_connection_type(connection) -> str:
        if 'pyodbc' in str(connection):
            db_type = "MSSQL"
        elif 'psycopg2' in str(connection.info):
            db_type = "postgresql"
        else:
            raise ValueError(f"unknown connection type {str(connection)}")
        return db_type

    @staticmethod
    def get_selectivity_list(query_obj_list):
        selectivity_list = []
        for query_obj in query_obj_list:
            selectivity_list.append(query_obj.selectivity)
        return selectivity_list


def transform_hypopg_index_name(hypo_idx_name, arms):
    for armname in arms:
        if arms[armname].hypopg_idx_name == hypo_idx_name:
            return armname
    # if '<' in hypo_idx_name:
    #     _split_list = hypo_idx_name.split('>')[1].split('_')[1:]  # e.g., ['lineitem', 'l', 'orderkey', 'l', 'shipmode', 'l', 'receiptdate', 'l', 'shipdate']
    #     tbl_name = _split_list[0].upper()
    #     # tabled_idx_name = tbl_name + '_' + '_'.join(_split_list[1:])
    #     cols_str = '_'.join(_split_list[1:])
    # for arm_name in arms:
    #     arm = arms[arm_name]
    #     arm_cols_str = ('_'.join(arm.index_cols)).lower()
    #     if arm.include_cols:
    #         arm_cols_str += '_' + ('_'.join(arm.include_cols)).lower()
    #     if arm_cols_str == cols_str:
    #         return arm_name
    """btree_lineitem_l_shipmode_l_partkey_l_quantity_l_shipinstruct_l_disco"""
    raise ValueError(f"no bandits with name similar to {hypo_idx_name}")


def merge_index_use(index_uses):
    d = defaultdict(list)
    for index_use in index_uses:
        if index_use[0] not in d:
            d[index_use[0]] = [0] * (len(index_use) - 1)
        d[index_use[0]] = [sum(x) for x in zip(d[index_use[0]], index_use[1:])]
    return [tuple([x] + y) for x, y in d.items()]


def format_query_string(query_string):
    query_text = query_string.replace('\r\n\t', ' ').replace('\r\n', ' ').replace('\t', ' ')
    return query_text
