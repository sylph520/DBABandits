import argparse
from pglast import ast
from pglast import parse_sql
import psycopg2
from typing import List, Set
import json


class COLUMN:
    def __init__(self, colname: str, tblname: str='') -> None:
        self.column_name = colname
        self.table_name = tblname

    def __repr__(self) -> str:
        return  self.table_name + '.' + self.column_name 


class DB_SCHEMA:
    def __init__(self) -> None:
        self.tables = [] 
        self.table_columns = {}

    def setup_db_schema_from_pgconn(self, pg_conn: psycopg2.extensions.connection):
        with pg_conn.cursor() as cur:
            get_tbl_stmt = "select table_name from information_schema.tables where table_schema='public' order by table_name;"
            cur.execute(get_tbl_stmt)
            tbls_res = cur.fetchall()
            tbl_names = [t[0] for t in tbls_res]
            self.table_name = tbl_names

            for tbl_name in tbl_names:
                get_col_stmt = f"select column_name from information_schema.columns where table_name = '{tbl_name}' order by order_idx"
                cur.execute(get_col_stmt)
                cols_res = cur.fetchall()
                col_names = (c[0] for c in cols_res)
                self.table_columns[tbl_name] = col_names


def main(sql_path: str):
    db_schema = DB_SCHEMA
    with open(sql_path, 'r') as f:
        content = f.readlines()

    json_items = []
    id = 1
    bsize = 16
    for sql_str in content:
        json_item = {}
        q_tbl_aliases = {}
        root = parse_sql(sql_str)[0]
        stmt = root.stmt

        fromClause = stmt.fromClause 
        for rangeElem in fromClause:
            if isinstance(rangeElem, ast.RangeVar):
                relname  =  rangeElem.relname
                aliased_tname = rangeElem.alias.aliasname
                q_tbl_aliases[aliased_tname] = relname
            else:
                raise

        whereClause = stmt.whereClause
        where_cols: List[COLUMN] = []
        # __import__('ipdb').set_trace()
        if isinstance(whereClause, ast.BoolExpr):
            boolexpr_args = whereClause.args
            where_cols = parse_boolexpr(boolexpr_args, q_tbl_aliases)
        else:
            raise

        sortClause = stmt.sortClause
        sort_cols: List[COLUMN] = []
        if sortClause:
            pass

        groupClause = stmt.groupClause
        group_cols: List[COLUMN]  = []
        if groupClause:
            pass

        targetList: tuple = stmt.targetList
        payload_cols: List[COLUMN]  = []
        for t in targetList:
            if isinstance(t, ast.ResTarget):
                tval = t.val
                t_cols = parse_tgt_arg(tval, q_tbl_aliases)
                payload_cols.extend(t_cols)
            else:
                raise ValueError(t)


        q_cols_set = set(where_cols) | set(sort_cols) | set(group_cols) | set(payload_cols)
        # __import__('ipdb').set_trace()
        # print(q_cols_set)

        json_item['id'] = id
        if id == 16:
            id = 1
        else:
            id += 1
        json_item['query_string'] = sql_str

        pred_dict = {}
        fill_dict(pred_dict, set(where_cols))
        json_item['predicates'] = pred_dict

        payload_dict = {}
        fill_dict(payload_dict, set(payload_cols))
        json_item['payload'] = payload_dict

        sort_dict = {}
        fill_dict(sort_dict, set(sort_cols))
        json_item['sort_by'] = sort_dict

        group_dict = {}
        fill_dict(group_dict, set(group_cols))
        json_item['group_by'] = group_dict

        json_items.append(json_item)
    output_json_path = '.'.join(input_sql_path.split('.')[0:-1]) + '.json'
    # print(json.dumps(json_items, indent=4))
    with open(output_json_path, 'w') as f:
        for i in json_items:
            f.write(json.dumps(i))
            f.write('\n')

def fill_dict(d: dict, cols: Set[COLUMN]):
    for c in cols:
        tname = c.table_name
        if tname not in d:
            d[tname] = [c.column_name]
        else:
            d[tname].append(c.column_name)
 

def parse_boolexpr(boolexpr_args, q_tbl_aliases):
    where_cols: List[COLUMN] = []
    for arg in boolexpr_args:
        wcs = parse_where_arg(arg, q_tbl_aliases)
        where_cols.extend(wcs)
    return where_cols


def parse_funcall(tval: ast.FuncCall, q_tbl_aliases):
    cols = []
    if len(tval.funcname) == 1:
        funcname = tval.funcname[0].sval
    else:
        raise ValueError(tval.funcname)

    if funcname == 'count':
        if tval.agg_star:
            return cols
        else:
            raise
    elif funcname in ['min', 'max']:
        args = tval.args
        for arg in args:
            fc_cols = parse_tgt_arg(arg, q_tbl_aliases)
            cols.extend(fc_cols)
        return cols
    else:
        raise ValueError(funcname)


def parse_tgt_arg(tval, q_tbl_aliases):
    tgt_cols = []
    if isinstance(tval, ast.FuncCall):
        fc_cols = parse_funcall(tval, q_tbl_aliases)
        tgt_cols.extend(fc_cols)
    elif isinstance(tval, ast.ColumnRef):
        t_col = parse_columnref(tval, q_tbl_aliases)
        tgt_cols.append(t_col)
    else:
        raise ValueError(tval)
    return tgt_cols


def parse_where_arg(arg, q_tbl_aliases: dict):
    cols = []
    if isinstance(arg, ast.A_Expr):
        expr_name = arg.name
        if len(expr_name) == 1:
            if isinstance(expr_name[0], ast.String):
                op = expr_name[0].sval
            else:
                raise
            if op in ['=', '<', '>', '<=', '>=', '~', '~~*']:  # sargable
                lexpr, rexpr = arg.lexpr, arg.rexpr
                for opr in [lexpr, rexpr]:
                    if isinstance(opr, ast.ColumnRef):
                        col = parse_columnref(opr, q_tbl_aliases)
                        cols.append(col)
                    else:
                        pass
                return cols
            else:
                raise ValueError(op)
        else:
            raise
    elif isinstance(arg, ast.BoolExpr):
        boolexpr_cols = parse_boolexpr(arg.args, q_tbl_aliases)
        return boolexpr_cols
    elif isinstance(arg, ast.NullTest):
        if isinstance(arg.arg, ast.ColumnRef):
            col = parse_columnref(arg.arg, q_tbl_aliases)
        else:
            raise ValueError(arg)
        cols.append(col)
        return cols
    else:
        raise ValueError(arg)


def parse_columnref(opr, q_tbl_aliases: dict) -> COLUMN:
    assert isinstance(opr, ast.ColumnRef)
    col_fields = opr.fields
    if len(col_fields) == 2:
        tblname, colname = (i.sval for i in col_fields)
        if tblname in q_tbl_aliases:
            tblname = q_tbl_aliases[tblname]
        col = COLUMN(colname, tblname)
        return col
    elif len(col_fields) == 1:
        raise
    else:
        raise ValueError(col_fields)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_sql', type=str, default='./imdb-sqls_25/concat_type1.sql')
    args = parser.parse_args()
    input_sql_path = args.input_sql

    main(input_sql_path)
