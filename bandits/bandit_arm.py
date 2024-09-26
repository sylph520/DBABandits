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
    def get_arm_str_id(index_cols, table_name, include_cols=(), db_type="MSSQL") -> str:
        if include_cols:
            if db_type == 'MSSQL':
                include_col_names = '_'.join(tuple(map(lambda x: x[0:4], include_cols))).lower()
            elif db_type == 'postgresql':
                include_col_names = '_'.join(tuple(map(lambda x: x, include_cols))).lower()
            arm_id = 'IXN_' + table_name + '_' + '_'.join(index_cols).lower() + '_' + include_col_names
        else:
            arm_id = 'IX_' + table_name + '_' + '_'.join(index_cols).lower()
        # arm_id = arm_id[:127]
        return arm_id

    @property
    def index_name(self):
        if self.include_cols:
            # include_col_hash = hashlib.sha1('_'.join(include_cols).lower().encode()).hexdigest()
            if self.db_type == 'MSSQL':
                include_col_names = '_'.join(tuple(map(lambda x: x[0:4], self.include_cols))).lower()
            elif self.db_type == 'postgresql':
                include_col_names = '_'.join(tuple(map(lambda x: x, self.include_cols))).lower()
            idx_name = 'IXN_' + self.table_name + '_' + '_'.join(self.index_cols).lower() + '_' + include_col_names
        else:
            idx_name = 'IX_' + self.table_name + '_' + '_'.join(self.index_cols).lower()
        return idx_name
