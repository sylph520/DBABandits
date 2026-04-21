class BanditArm:
    def __init__(self, index_cols, table_name, memory, table_row_count, include_cols=()):
        self.schema_name = 'dbo'
        self.table_name = table_name
        self.index_cols = index_cols
        self.include_cols = include_cols
        if self.include_cols:
            # include_col_hash = hashlib.sha1('_'.join(include_cols).lower().encode()).hexdigest()
            include_col_names = '_'.join(tuple(map(lambda x: x[0:4], include_cols))).lower()
            self.index_name = 'IXN_' + table_name + '_' + '_'.join(index_cols).lower() + '_' + include_col_names
        else:
            self.index_name = 'IX_' + table_name + '_' + '_'.join(index_cols).lower()
        self.index_name = self.index_name[:127]
        self.memory = memory
        self.table_row_count = table_row_count
        self.name_encoded_context = []
        self.index_usage_last_batch = 0

        # cluster: str or None
        # Group identifier for full-coverage indexes (covers all query predicates).
        # Set when len(col_permutation) == len(table_predicates), e.g., "customer_5_all".
        # Used to mark arms as mutually exclusive - picking one removes others in same cluster.
        self.cluster = None

        # query_id: int or None (legacy - use query_ids instead)
        # The primary query ID associated with this arm (older pattern).
        self.query_id = None

        # query_ids: Set[int]
        # Set of query IDs that this index arm can benefit.
        # An index on (a,b) benefits queries that use a or b in their predicates.
        self.query_ids = set()

        # query_ids_backup: Set[int]
        # Backup of query_ids for restoration or rollback purposes.
        self.query_ids_backup = set()

        # is_include: 0 or 1
        # Whether this is a "pure" covering index (full key coverage, no INCLUDE columns).
        # 1 = full key coverage AND no INCLUDE columns (pure covering index).
        # 0 = partial coverage OR has INCLUDE columns.
        # Used in oracle to decide when to remove redundant partial indexes.
        self.is_include = 0
        # arm_value: Dict[query_id, float]
        # Estimated reward/benefit of this index arm per query.
        # Calculated during arm generation as: (1 - selectivity) * coverage_ratio * table_row_count
        # Higher value = more valuable index for that query.
        self.arm_value = {}
        self.clustered_index_time = 0

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
    def get_arm_id(index_cols, table_name, include_cols=()) -> str:
        if include_cols:
            include_col_names = '_'.join(tuple(map(lambda x: x[0:4], include_cols))).lower()
            arm_id = 'IXN_' + table_name + '_' + '_'.join(index_cols).lower() + '_' + include_col_names
        else:
            arm_id = 'IX_' + table_name + '_' + '_'.join(index_cols).lower()
        arm_id = arm_id[:127]
        return arm_id
