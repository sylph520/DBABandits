from __future__ import annotations
from typing import Dict, TYPE_CHECKING
if TYPE_CHECKING:
    from database.dbconn import DBConnection


class Query:
    def __init__(self, connection: DBConnection, query_id: int, query_string: str,
                 predicates: Dict[str, dict], payloads: Dict[str, dict], time_stamp=0,
                 sel_store=None):
        """
        initialize a query instance with its selectivity computed
        """
        self.id = query_id
        self.predicates = predicates
        self.payload = payloads
        self.group_by = {}
        self.order_by = {}
        # selectivity_list: query-wise
        # selectivity: table-wise, {table: float_val}
        if sel_store is not None and query_string in sel_store:
            self.selectivity = sel_store[query_string]
        else:
            self.selectivity = connection.get_selectivity_v3(query_string, self.predicates)
        self.query_string = query_string
        self.frequency = 1
        self.last_seen_round = time_stamp
        self.first_seen_round = time_stamp
        self.table_scan_time_dict = connection.get_table_scan_times_structure()
        self.clustered_index_scan_time_dict = connection.get_table_scan_times_structure()
        self.nonclustered_index_scan_time_dict = connection.get_table_scan_times_structure()
        self.table_scan_times_hyp = connection.get_table_scan_times_structure()
        self.index_scan_times_hyp = connection.get_table_scan_times_structure()
        self.context = None

    def __hash__(self):
        return self.id

    def get_id(self):
        return self.id


def get_tblname_from_hypopg_idxname(idx_name: str, tbl_name: str):
    if tbl_name == 'WEB':
        if 'sales' in idx_name:
            tbl_name = 'WEB_SALES'
        elif 'returns' in idx_name:
            tbl_name = 'WEB_RETURNS'
        elif 'site' in  idx_name:
            tbl_name = 'WEB_SITE'
        else:
            raise
    elif tbl_name == 'STORE':
        if 'sales' in idx_name:
            tbl_name = 'STORE_SALES'
        elif 'returns' in idx_name:
            tbl_name = 'STORE_RETURNS'
    elif tbl_name == 'CATALOG':
        if 'page' in idx_name:
            tbl_name = 'CATALOG_PAGE'
        elif 'returns' in idx_name:
            tbl_name = 'CATALOG_RETURNS'
        elif 'sales' in idx_name:
            tbl_name = 'CATALOG_SALES'
        else:
            raise
    elif tbl_name == 'DATE':
        tbl_name = 'DATE_DIM'
    elif tbl_name == 'CUSTOMER':
        if 'address' in idx_name:
            tbl_name = 'CUSTOMER_ADDRESS'
        elif 'demographics' in idx_name:
            tbl_name = 'CUSTOMER_DEMOGRAPHICS'
    elif tbl_name == 'TIME':
        tbl_name = 'TIME_DIM'
    elif tbl_name == 'MOVIE':
        idx_cands = ['movie_info_idx', 'movie_info', 'movie_link', 'movie_keyword', 'movie_companies']
        for i in idx_cands:
            if i in idx_name:
                tbl_name = i.upper()
        assert '_' in tbl_name, f"{tbl_name}, index name is {idx_name}"
    elif tbl_name == 'CAST':
        tbl_name = 'CAST_INFO'
    elif tbl_name == 'PERSON':
        tbl_name = 'PERSON_INFO'
    elif tbl_name == 'COMPANY':
        if 'name' in idx_name:
            tbl_name = 'COMPANY_NAME'
        else:
            tbl_name = 'COMPANY_TYPE'
    return tbl_name