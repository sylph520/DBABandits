import os
import copy
import random
import pickle
from typing import List
from database.query_v5 import Query


def templated_workload_gen(self, templated_workload, sel_matrix, exec_flag=False,
                             repeat=1, save_file='varied_wks.pkl', new_seed=0.17,
                             opv=0):
     """
     generate workload with the same template but with literals
     with different selectivity (keep the same join)
     """
     # old_sel_matrix = sel_matrix.copy()
     # print(old_sel_matrix[0]) #debug
     templated_workload_buf :list = []
     templated_workload_buf.append(templated_workload)
     old_seed = 0.17
     new_seed = new_seed # debug
     # save_file = save_file.split('.')[0] + f'_seed{new_seed}.' + save_file.split('.')[1]
     save_file_basefn = save_file.split('.')[0]
     if new_seed != 0.17:
         self.set_random_seed(new_seed)
     for j in range(repeat-1):
         template :Workload = copy.deepcopy(templated_workload)
         for i in range(len(template.queries)):
             q_inst = template.queries[i]
             for pred in q_inst.qry_predicates_list:
                 # print(pred.predicate_str) # debug
                 # print(pred.left_op)
                 if pred.type == 0: # lt+rval
                     lop :Column = pred.left_op
                     rop :Val= pred.right_op
                     old_rval = rop.value
                     col_name = lop.name
                     tbl_name = lop.table.name
                     rval_new = self.col_val_sampling(tbl_name, col_name)
                     val_change = True if rval_new != rop.value else False
                     rop.value = rval_new
                     rop.value_str = rop.value.__str__()
                     pred.right_op_str = rop.value_str
                     if opv in [1,2,3]:
                         if pred.operator_str == '<':
                            pred.operator_str = '>'
                         elif pred.operator_str == '>':
                            pred.operator_str = '<'
                         elif pred.operator_str == '=':
                             if opv == 2: # 2: change = to < or >
                                 pred.operator_str = random.choice(['<', '>'])
                             elif opv == 3: # 2: change = to < or = or >
                                 pred.operator_str = random.choice(['<', '>', '='])
                     pred.update_predstr()
                 elif pred.type == 1: # rt+lval
                     rop :Column = pred.left_op
                     lop :Val= pred.right_op
                     col_name = rop.name
                     tbl_name = rop.table.name
                     lval_new = self.col_val_sampling(tbl_name, col_name)
                     val_change = True if lval_new != lop.value else False
                     lop.value = lval_new
                 elif pred.type == 2: # join
                     continue
                 else:
                     raise
                 if val_change:
                     sel_new = self.compute_sel_from_pred(pred, exec_flag = exec_flag)
                     self.update_sel_from_pred(sel_matrix, pred, sel_new, i)
             # finished varying preds for a query inst
             q_inst.update_query_text(self.columns) # update the queries
         template.query_str_list = template.get_query_str_list()
         w_sql_file = save_file_basefn + f"_{j+1}.sql"
         if not os.path.exists(w_sql_file):
             with open(w_sql_file, 'w') as f:
                 f.write('\n'.join(template.query_str_list))
         # print(f"sel equal? {(sel_matrix==old_sel_matrix).all()}") #debug
         templated_workload_buf.append(template)
     assert not os.path.exists(save_file)
     with open(save_file, 'wb+') as wf:
         pickle.dump(templated_workload_buf, wf)
     new_save_loc = '/'.join(save_file.split('/')[0:-2]) + '/' + save_file.split('/')[-1].split('.')[0] + '.txt'
     with open(new_save_loc, 'wb+') as wf:
         pickle.dump(templated_workload_buf, wf)
     if new_seed!=0.17: # return to 0.17
         self.set_random_seed(old_seed)
     return templated_workload_buf

def tpl_qgen(tpls: List[Query])-> List[Query]:
    wk = []
    tpls_copy = copy.deepcopy(tpls)
    for tpl in tpls_copy:
        for pred in 
        pass
    return wk

def dynamic_shifting_gen(wk: List[Query], g_size, rep_size, save_fn, overwrite = False):
    if not os.path.exists(save_fn) or overwrite:
        wks = []

        shuffled_queries = wk[:]
        random.seed(123)
        random.shuffle(shuffled_queries)
        tpls_groups = [shuffled_queries[i*g_size, (i+1)*g_size] for i in range(g_size)]

        # generate rep_size * g_size
        for tpls in tpls_groups:  # for each template group 
            for _ in range(rep_size):  # repeat generating within the same template group
                wk_len = len(wks)
                new_wk = tpl_qgen(tpls)

                with open(save_fn + f'_{wk_len + 1}', 'wb') as wf:
                    pickle.dump(new_wk, wf)

                wks.append(new_wk)
        with open(save_fn, 'wb') as wf:
            pickle.dump(wks, wf)
    else:
        with open(save_fn, 'rb') as rf:
            wks = pickle.load(rf)

    return wks

def dynamic_adhoc_gen(wk: List[Query]):
    wks = []
    return wks



if __name__ == "__main__":
    wk = [
        
    ]