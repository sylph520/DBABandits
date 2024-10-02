from typing import Dict


class Query:
    def __init__(self, connection, query_id: int, query_string: str,
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
