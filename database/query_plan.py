import xml.etree.ElementTree as ET
# from typing import Dict

# from bandits.bandit_arm import BanditArm
import constants
# from database.table import Table


class QueryPlan_MSSQL:
    def __init__(self, xml_string):
        """
        parse the plan xml to get the estimated costs, and update index usage dicts.
        subtree_cost info can be directly obtained from the node stats;
        the cpu_time info is computed in porpotion with the subtree_cost ratio
        """
        ns = {'sp': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
        physical_operations = {'Index Seek', 'Index Scan', "Clustered Index Scan", "Clustered Index Seek"}
        # global stats for the whole plan
        self.estimated_rows = 0
        self.est_statement_cost = 0
        self.stmt_elapsed_time = 0
        self.cpu_time = 0  # use the fraction relationship to compute the cpu time of each node
        self.non_clustered_index_usage = []
        self.clustered_index_usage = []

        root = ET.fromstring(xml_string)
        stmt_simple = root.find('.//sp:StmtSimple', ns)
        self.estimated_rows = stmt_simple.attrib.get('StatementEstRows')
        self.est_statement_cost = stmt_simple.attrib.get('StatementSubTreeCost')

        query_stats = root.find('.//sp:QueryTimeStats', ns)
        if query_stats is not None:
            self.cpu_time = query_stats.attrib.get('CpuTime')
            self.stmt_elapsed_time = float(query_stats.attrib.get('ElapsedTime')) / 1000

        rel_ops = root.findall('.//sp:RelOp', ns)
        total_po_sub_tree_cost = 0
        total_po_actual = 0
        # Get the sum of sub tree cost for physical operations (assumption: sub tree cost is dominated by the physical
        # operations)
        for rel_op in rel_ops:
            temp_act_elapsed_time = 0
            if rel_op.attrib.get('PhysicalOp') in physical_operations:
                total_po_sub_tree_cost += float(rel_op.attrib.get('EstimatedTotalSubtreeCost'))
                runtime_thread_information = rel_op.findall('.//sp:RunTimeCountersPerThread', ns)
                for thread_info in runtime_thread_information:
                    temp_act_elapsed_time = max(
                        int(thread_info.attrib.get('ActualElapsedms')) if thread_info.attrib.get(
                            'ActualRowsRead') is not None else 0, temp_act_elapsed_time)
                total_po_actual += temp_act_elapsed_time / 1000

        # Now for each rel operator we estimate the elapsed time using the sub tree costs
        for rel_op in rel_ops:
            rows_read = 0
            act_rel_op_elapsed_time = 0
            if rel_op.attrib.get('PhysicalOp') in physical_operations:
                runtime_thread_information = rel_op.findall('.//sp:RunTimeCountersPerThread', ns)
                for thread_info in runtime_thread_information:
                    rows_read += int(thread_info.attrib.get('ActualRowsRead')) if thread_info.attrib.get(
                        'ActualRowsRead') is not None else 0
                    act_rel_op_elapsed_time = max(int(thread_info.attrib.get('ActualElapsedms')) if thread_info.attrib.get(
                        'ActualElapsedms') is not None else 0, act_rel_op_elapsed_time)
            act_rel_op_elapsed_time = act_rel_op_elapsed_time / 1000
            # act_rel_op_elapsed_time = float(self.elapsed_time) * (act_rel_op_elapsed_time/total_po_actual) if total_po_actual > 0 else 0
            # We can either use act_rel_op_elapsed_time or po_elapsed_time for the elapsed time
            if rows_read == 0:
                rows_read = float(rel_op.attrib.get('EstimatedRowsRead')) if rel_op.attrib.get('EstimatedRowsRead') else 0
            rows_output = float(rel_op.attrib.get('EstimateRows'))
            if rel_op.attrib.get('PhysicalOp') in physical_operations:
                po_subtree_cost = float(rel_op.attrib.get('EstimatedTotalSubtreeCost'))
                po_elapsed_time = float(self.stmt_elapsed_time) * (po_subtree_cost / total_po_sub_tree_cost)
                po_cpu_time = float(self.cpu_time) * (
                    po_subtree_cost / float(self.est_statement_cost))
                po_index_scan = rel_op.find('.//sp:IndexScan', ns)
                if rel_op.attrib.get('PhysicalOp') in {'Index Seek', 'Index Scan'}:
                    po_index = po_index_scan.find('.//sp:Object', ns).attrib.get('Index').strip("[]")
                    self.non_clustered_index_usage.append(
                        (po_index, act_rel_op_elapsed_time, po_cpu_time, po_subtree_cost, rows_read, rows_output))
                elif rel_op.attrib.get("PhysicalOp") in {"Clustered Index Scan", "Clustered Index Seek"}:
                    table = po_index_scan.find(".//sp:Object", ns).attrib.get("Table").strip("[]")
                    self.clustered_index_usage.append(
                        (table, act_rel_op_elapsed_time, po_cpu_time, po_subtree_cost, rows_read, rows_output))


class QueryPlanPG:
    def __init__(self, plan_json, dbconn):
        # self.estimated_rows = 0
        # self.est_statement_sub_tree_cost = 0
        # each item is a tuple(index_name, elapsed_time, cpu_time, subtree_cost, rows_read, rows_output)
        self.non_clustered_index_usage, self.clustered_index_usage = [], []
        self.table_scans = []

        self.est_num_res_rows = plan_json["Plan"]["Plan Rows"]
        self.est_stmt_cost = plan_json["Plan"]["Total Cost"]

        if "Execution time" in plan_json.keys():  # explain analyze
            self.stmt_elapsed_time = float(plan_json["Execution time"]) / 1000
        else:
            self.stmt_elapsed_time = 0.0

        self.rel_ops = [plan_json["Plan"]]
        self.ops_stack = [plan_json["Plan"]]
        while len(self.ops_stack) != 0:  # in top-down manner
            ops = self.ops_stack.pop(-1)
            if "Plans" in ops.keys():
                for op in ops["Plans"]:
                    self.rel_ops.append(op)
                    self.ops_stack.append(op)

        reversed_rel_ops = self.rel_ops.copy()
        reversed_rel_ops.reverse()
        # subtree_cost = 0
        tables_global = dbconn.tables_global
        num_row_prev = {}
        for i in range(len(reversed_rel_ops)):
            rel_op = reversed_rel_ops[i]
            if rel_op["Node Type"] == 'Seq Scan':
                tbl_name = rel_op['Relation Name'].upper()

                if tbl_name in num_row_prev:
                    est_op_rows_read = num_row_prev[tbl_name]
                else:
                    est_op_rows_read = int(tables_global[tbl_name].table_row_count)
                est_op_rows_output = rel_op['Plan Rows']
                num_row_prev[tbl_name] = est_op_rows_output

                est_op_subtree_cost = rel_op['Total Cost']

                op_elapsed_time = -1.0
                op_cpu_time = -1.0

                self.table_scans.append((tbl_name, op_elapsed_time, op_cpu_time, est_op_subtree_cost, est_op_rows_read, est_op_rows_output))
            elif 'Index' in rel_op["Node Type"]:
                idx_name = rel_op['Index Name']
                # print(idx_name)
                if rel_op['Node Type'] in ['Index Scan', 'Index Only Scan']:
                    tbl_name = rel_op['Relation Name'].upper()
                elif rel_op['Node Type'] == 'Bitmap Index Scan':
                    tbl_name = idx_name.split('_')[1].upper()
                else:
                    raise NotImplementedError(f"node type {rel_op['Node Type']} not handled yet")
                # col_names = '_'.join(idx_name.split('_')[2:])

                if tbl_name in num_row_prev:
                    est_op_rows_read = num_row_prev[tbl_name]
                else:
                    est_op_rows_read = int(tables_global[tbl_name].table_row_count)
                est_op_rows_output = rel_op['Plan Rows']
                num_row_prev[tbl_name] = est_op_rows_output

                # sel = 1.0 * est_op_rows_output / est_op_rows_read
                # num_index_rows = est_op_rows_read * sel
                # op_cpu_time = constants.PG_RANDOM_PAGE_COST + num_index_rows * constants.PG_CPU_INDEX_TUPLE_COST\
                #     + num_index_rows * constants.PG_CPU_OPERATOR_COST\
                #     + est_op_rows_read * constants.PG_CPU_TUPLE_COST
                est_op_subtree_cost = rel_op['Total Cost']

                op_elapsed_time = -1.0
                op_cpu_time = -1.0
                if 'pkey' not in idx_name:
                    self.non_clustered_index_usage.append((idx_name, op_elapsed_time, op_cpu_time, est_op_subtree_cost, est_op_rows_read, est_op_rows_output))
                else:  # primary key index scan
                    self.clustered_index_usage.append((idx_name, op_elapsed_time, op_cpu_time, est_op_subtree_cost, est_op_rows_read, est_op_rows_output))
            else:
                # e.g., hash, hash join, aggregate, gather, gather merge, sort, nested loop, bitmap heap scan, materialize, merge join
                # print(f"node type is {rel_op['Node Type']}")
                # print('debug line')
                pass
