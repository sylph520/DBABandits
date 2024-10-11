from typing import List
import constants

class BanditArm:
    def __init__(self, index_cols, table_name, memory, table_row_count, include_cols=(), db_type='MSSQL'):
        self.db_type = db_type
        self.table_name = table_name
        self.table_row_count = table_row_count
        self.index_cols = index_cols
        self.include_cols = include_cols
        self.memory = memory
        self.schema_name = 'dbo'
        self.hypopg_index_name = ''

        self.query_id = None
        self.query_ids = set()
        self.query_ids_backup = set()
        self.is_include = 0
        self.index_usage_last_batch = 0
        self.cluster = None
        self.clustered_index_time = 0

        self.name_encoded_context = []
        self.arm_value = {}  # {query_id: value}

    def __eq__(self, other):
        return self.index_name == other.index_name

    def __hash__(self):
        return hash(self.index_name)

    def __le__(self, other):
        if len(self.index_cols) > len(other.index_cols):
            return False
        else:
            for i in range(len(self.index_cols)):
                if self.index_cols[i] != other.index_cols[i]:
                    return False
            return True

    def __str__(self):
        return self.index_name

    @staticmethod
    def get_arm_str_id(index_cols: List[str], table_name: str, include_cols=(), db_type='MSSQL') -> str:
        db_colname_prefixs = constants.db_colname_prefixs
        c_prefixs = truncate_colnames(index_cols, db_colname_prefixs)
        indexing_col_names = '_'.join(c_prefixs)
        if include_cols:
            ic_prefixs = truncate_colnames(include_cols, db_colname_prefixs)
            include_col_names = '_'.join(ic_prefixs)

            arm_id = 'IXN_' + table_name + '_' + indexing_col_names + '_' + include_col_names
        else:
            arm_id = 'IX_' + table_name + '_' + indexing_col_names
        return arm_id

    @property
    def index_name(self):
        return self.get_arm_str_id(self.index_cols, self.table_name, self.include_cols, self.db_type)


def truncate_colnames(colnames, db_colname_prefixs):
    tc_cns = []
    for c in colnames:
        c = c.lower()
        if c in db_colname_prefixs:
            tc = db_colname_prefixs[c].lower()
            tc_cns.append(tc)
        else:
            raise ValueError(f"column {c} not found in db_colname_prefixs")
    return tc_cns
