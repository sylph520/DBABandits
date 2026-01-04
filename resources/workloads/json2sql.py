import json
import pickle
import argparse

import re
import sqlglot


def transform_tsql_to_psql(query):
    query2 = sqlglot.transpile(query, read='tsql', write='postgres')[0]
    if 'YEAR(' in query2:
        query2 = re.sub(r'YEAR\((.*?)\)', r'EXTRACT(YEAR FROM \1)', query2)
    return query2


def main(args):
    json_fn = args.json_fn
    q_texts = []
    q_per_tpl = {}
    with open(json_fn, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line_dict = json.loads(line)
            tpl_id = line_dict['id']
            q_text = line_dict['query_string']
            q_text = q_text.replace('\r\n', ' ')
            q_text = q_text.replace('\t', ' ')
            q_text = transform_tsql_to_psql(q_text)
            q_text += '\n'
            q_texts.append(q_text)
            if tpl_id in q_per_tpl:
                q_per_tpl[tpl_id].append(q_text)
            else:
                q_per_tpl[tpl_id] = [q_text]

    # __import__('ipdb').set_trace()
    sql_fn = json_fn.replace('json', 'sql')
    pkl_fn = json_fn.replace('json', 'pkl')
    with open(sql_fn, 'w') as f:
        f.writelines(q_texts)
    with open(pkl_fn, 'wb') as f:
        pickle.dump(q_per_tpl, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--json_fn', type=str, default='tpc_h_static_100.json')
    args = parser.parse_args()
    main(args)
