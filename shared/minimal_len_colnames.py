import psycopg2
import os
import pickle
from typing import Dict


def connect_to_db(dbname, port=51204, host='/tmp'):
    conn = psycopg2.connect(database=dbname, port=port, host=host)
    return conn


def get_db_table_names(conn) -> list:
    # query = "select relname from pg_class where relkind='r' and relnamespace=2200;"
    query = "select tablename from pg_tables where schemaname='public' order by tablename;"
    cur = conn.cursor()
    cur.execute(query)
    res = cur.fetchall()
    table_names = [i[0] for i in res]
    return table_names


def get_table_column_names(t, conn=None, cur=None):
    if not cur:
        assert conn
        cur = conn.cursor()
    query = f"select column_name from information_schema.columns where table_name= '{t}' order by ordinal_position "
    cur.execute(query)
    res = cur.fetchall()
    tbl_colnames = [i[0] for i in res]
    return tbl_colnames


def get_db_column_names(conn, dbname):
    table_names = get_db_table_names(conn)
    cur = conn.cursor()
    db_colnames = []
    for t in table_names:
        tbl_colnames = get_table_column_names(t, cur=cur)
        db_colnames.extend(tbl_colnames)
    return db_colnames


def truncate_colname(name, i):
    if len(name) > i:
        return name[:i]
    else:
        return name


def find_min_uniprefix_len(colnames):
    i = 1
    while True:
        col_prefixs = [truncate_colname(c, i) for c in colnames]
        if len(set(col_prefixs)) == len(col_prefixs):
            return i
        else:
            i += 1


def is_prefix_unique(prefix, other_colnames: list):
    unique = True
    for n in other_colnames:
        if n.startswith(prefix):
            unique = False
            break
    return unique


def find_min_unique_nameprefix(colnames):
    colname_map: Dict[str, str] = {}
    for i in range(len(colnames)):  # for each column
        cur_name = colnames[i]
        colnames_copy = colnames.copy()
        colnames_copy.pop(i)

        max_len = len(cur_name)
        for end_pos in range(max_len + 1):
            name_prefix = cur_name[:end_pos]
            if is_prefix_unique(name_prefix, colnames_copy):
                colname_map[cur_name] = name_prefix
                break
            elif end_pos == max_len:  # sometimes the whole names is contained by other names
                # assert is_prefix_unique(name_prefix, colnames_copy), f"colname {cur_name}, name prefix {name_prefix}"
                colname_map[cur_name] = cur_name
            else:  # check the next value of end_pos
                pass
    assert len(colname_map) == len(colnames), f"{len(colname_map)} vs. {len(colnames)}"
    return colname_map


def get_db_col_min_len(dbname, save=False):
    # get all column names
    conn = connect_to_db(dbname)

    # db_colnames = get_db_column_names(conn, dbname)
    # # print(db_colnames)
    # min_len = find_min_uniprefix_len(db_colnames)
    # col_prefixs = [truncate_colname(c, min_len) for c in db_colnames]
    # print(col_prefixs)

    dbtbl_min_len_dict: Dict[str, int] = {}
    tbl_truncated_colname_dict: Dict[str, str] = {}
    tbls = get_db_table_names(conn)
    cur = conn.cursor()
    for t in tbls:
        tbl_colnames = get_table_column_names(t, cur=cur)
        tbl_min_len = find_min_uniprefix_len(tbl_colnames)
        dbtbl_min_len_dict[t] = tbl_min_len
        tbl_truncated_colname_dict[t] = [truncate_colname(i, tbl_min_len) for i in tbl_colnames]
    print(tbl_truncated_colname_dict)

    fn = f'{dbname}_min_len.pkl'
    if save and not os.path.exists(fn):
        with open(fn, 'w') as f:
            pickle.dump(dbtbl_min_len_dict, f)
    return dbtbl_min_len_dict


def get_db_colname_prefixs(dbname, save=True):
    if 'tpcds' in dbname:
        fn = "tpcds_colname_minprefix.pkl"
    elif 'tpch' in dbname:
        fn = "tpch_colname_minprefix.pkl"
    else:
        raise f"saving file name for database {dbname} not determined"

    if os.path.exists(fn):
        with open(fn, 'rb') as f:
            col_prefix_map = pickle.load(f)
        print(f"{fn} already exists, loaded it")
        return col_prefix_map
    else:
        # dbname = 'indexselection_tpcds___10'
        conn = connect_to_db(dbname)

        # db_col_prefix_dict: Dict[str, str] = {}
        db_cols = get_db_column_names(conn, dbname)
        col_prefix_map = find_min_unique_nameprefix(db_cols)
        # print(col_prefix_map)

        # db_names = ['tpch_010', 'indexselection_tpcds___10']
        # bm_tbl_min_len_dict: Dict[str, int] = {}
        # for dbname in db_names:
        #     dbtbl_min_len_dict = get_db_col_min_len(dbname)
        #     bm_tbl_min_len_dict[dbname] = dbtbl_min_len_dict
        #
        # fn = f"{'-'.join(db_names)}_min_len.pkl"
        # __import__('pdb').set_trace()

        if save:
            with open(fn, 'wb') as f:
                pickle.dump(col_prefix_map, f)
                print(f"saved to {fn}")
        return col_prefix_map


if __name__ == "__main__":
    dbname = 'tpch_010'
    print(get_db_colname_prefixs(dbname, False))
