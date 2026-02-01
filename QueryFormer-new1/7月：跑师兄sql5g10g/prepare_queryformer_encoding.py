import os
import json
import torch
import pandas as pd

CSV_FILE = "data/plan_size5_queryformer.csv"

def collect_node_types_and_joins(plan_dir, csv_file=None):
    node_types = set()
    joins = set()
    def extract_join(node):
        join = None
        if 'Hash Cond' in node:
            join = node['Hash Cond']
        elif 'Join Filter' in node:
            join = node['Join Filter']
        elif 'Index Cond' in node and not node['Index Cond'][-2].isnumeric():
            join = node['Index Cond']
        if join is not None:
            joins.add(str(join))
    if csv_file:
        df = pd.read_csv(csv_file)
        for i, row in df.iterrows():
            plan = json.loads(row['json'])['Plan']
            def traverse(node):
                node_types.add(node['Node Type'])
                extract_join(node)
                if 'Plans' in node:
                    for sub in node['Plans']:
                        traverse(sub)
            traverse(plan)
    else:
        for fname in os.listdir(plan_dir):
            if not (fname.endswith('.txt') or fname.endswith('.json')):
                continue
            with open(os.path.join(plan_dir, fname), 'r', encoding='utf-8') as f:
                lines = f.readlines()
            for line in lines:
                if line.strip().startswith('{'):
                    try:
                        plan = json.loads(line.strip())[0]['Plan']
                        def traverse(node):
                            node_types.add(node['Node Type'])
                            extract_join(node)
                            if 'Plans' in node:
                                for sub in node['Plans']:
                                    traverse(sub)
                        traverse(plan)
                    except Exception as e:
                        continue
    return sorted(list(node_types)), sorted(list(joins))

def build_encoding(node_types, joins, col_names=None, op_list=None):
    type2idx = {t: i for i, t in enumerate(node_types)}
    idx2type = {i: t for t, i in type2idx.items()}
    if col_names is None:
        col2idx = {'NA': 0}
    else:
        col2idx = {'NA': 0}
        for i, c in enumerate(col_names, 1):
            col2idx[c] = i
    if op_list is None:
        op2idx = {'>': 0, '=': 1, '<': 2, 'NA': 3}
    else:
        op2idx = {op: i for i, op in enumerate(op_list)}
    join2idx = {j: i for i, j in enumerate(joins)}
    idx2join = {i: j for j, i in join2idx.items()}
    column_min_max_vals = {c: (0.0, 1.0) for c in col2idx}
    column_min_max_vals['NA'] = (0.0, 1.0)
    encoding = {
        'type2idx': type2idx,
        'idx2type': idx2type,
        'col2idx': col2idx,
        'op2idx': op2idx,
        'join2idx': join2idx,
        'idx2join': idx2join,
        'column_min_max_vals': column_min_max_vals
    }
    return encoding

if __name__ == '__main__':
    csv_file = CSV_FILE
    plan_dir = 'plan/sql_plan on size5'
    out_file = 'checkpoints/encoding.pt'

    node_types, joins = collect_node_types_and_joins(plan_dir=None, csv_file=csv_file)
    print('共发现 Node Type:', node_types)
    print('共发现 Join:', joins)
    encoding = build_encoding(node_types, joins)
    os.makedirs('checkpoints', exist_ok=True)
    torch.save({'encoding': encoding}, out_file)
    print(f'已保存新的 encoding 到 {out_file}')