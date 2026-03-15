import numpy as np # type: ignore
import psycopg2 ## type: ignore
import os
import datetime
import sys
import time
from models import GAT
import torch
import random
sys.path.append(r"../")
from utils.htap_query import HTAPController
from utils import load_plans, cost_split
from multiprocessing import Pool
from utils.restore import restore
from train import run_train_upd, run_test_upd

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--no-cuda', action='store_true', default=False, help='Disables CUDA training.')
parser.add_argument('--fastmode', action='store_true', default=False, help='Validate during training pass.')
parser.add_argument('--sparse', action='store_true', default=False, help='GAT with sparse version or not.')
parser.add_argument('--seed', type=int, default=72, help='Random seed.')
parser.add_argument('--epochs', type=int, default=10, help='Number of epochs to train.')
parser.add_argument('--lr', type=float, default=0.005, help='Initial learning rate.')
parser.add_argument('--weight_decay', type=float, default=5e-4, help='Weight decay (L2 loss on parameters).')
parser.add_argument('--hidden', type=int, default=32, help='Number of hidden units.')
parser.add_argument('--nb_heads', type=int, default=8, help='Number of head attentions.')
parser.add_argument('--dropout', type=float, default=0, help='Dropout rate (1 - keep probability).')
parser.add_argument('--alpha', type=float, default=0.2, help='Alpha for the leaky_relu.')
parser.add_argument('--patience', type=int, default=100, help='Patience')

args = parser.parse_args()
args.cuda = not args.no_cuda and torch.cuda.is_available()

random.seed(args.seed)
np.random.seed(args.seed)
torch.manual_seed(args.seed)
if args.cuda:
    torch.cuda.manual_seed(args.seed)

params = []
for i in range(1, 23):
    path = "../../data/params/" + str(i) + ".txt"
    param = []
    if os.path.exists(path):
        with open(path, "r") as file:
            lines = file.readlines()
            for tup in lines:
                tup = eval(tup)
                param.append(tup[0])
    params.append(param)


if __name__ == "__main__":

    restore()
    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")

    feature_num = 3 + 13
    scan_model = GAT(nfeat=feature_num, 
                nhid=args.hidden, 
                nclass=35, 
                dropout=args.dropout, 
                nheads=args.nb_heads, 
                alpha=args.alpha)
    scan_plan_path, join_plan_path = "../../data/cost_col_hint_plan.txt", "../../data/cost_join_type_hint_plan.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)
    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 80)
    scan_model = run_train_upd(X_train, y_train, scan_model)
    # scan_model = run_train_upd("../../data/X_latency_train_scan_htap", 22 * 20, scan_model)


    table = ["order_line", "stock", "customer", "orders", "item"]
    scan_hint_set = []
    for i in range(0, 32):
        hint = ""

        for j in range(0, 5):
            k = i >> j & 1
            if k == 1:
                hint += "ColumnarScan(" + table[j] +")\n"
            else:
                hint += "NoColumnarScan(" + table[j] +")\n"
        
        scan_hint_set.append(hint)

    scan_hint_set.append("")
    scan_hint_set.append('''
                        Set(google_columnar_engine.enable_columnar_scan on)\n
                        Set(google_columnar_engine.enable_vectorized_join on)\n
                        Set(enable_indexscan off)\n
                        Set(enable_seqscan off)\n
                        Set(enable_indexonlyscan off)\n
                    ''')
    scan_hint_set.append('''
                        Set(google_columnar_engine.enable_columnar_scan off)\n
                        Set(google_columnar_engine.enable_vectorized_join off)\n
                        Set(enable_indexscan on)\n
                        Set(enable_seqscan on)\n
                        Set(enable_indexonlyscan on)\n
                    ''')

    scan_latency = []
    scan_sql_latency = np.zeros(22)
    scan_failed = []
    k = 10
    
    explain_time = 0
    inference_time = 0
    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                param = params[id - 1]

                ## inference starts here, io included
                explain_sql = "EXPLAIN (FORMAT JSON)\n" + sql
                start = time.time()
                if len(param) == 0:
                    cur.execute(explain_sql)
                else:
                    cur.execute(explain_sql, param[i * 5])
                plan = cur.fetchone()[0][0]
                end = time.time()
                explain_time += end - start
                with open("plan.txt", "w") as file:
                    file.write(str(plan) + '\n')
                start = time.time()
                scan_hint_id = int(run_test_upd(scan_model))
                end = time.time()
                inference_time += end - start
                ## inference ends here. Maybe we can increase it by extending to the code below if necessary

                param = None if len(param) == 0 else param[i * 5]
                print("execute stream, sql_id:", i, id)

                scan_hint = "/*+\n" + scan_hint_set[scan_hint_id] + "*/\n"
                
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                scan_hint_sql = scan_hint + exp_sql

                p = Pool(k + 1)
                htapcontroller = HTAPController()
                tp = p.map_async(htapcontroller.oltp_worker, range(k))
                ap = p.starmap_async(htapcontroller.olap_worker, [(scan_hint_sql, param, 20000)])
                p.close()
                p.join()
                plan = ap.get()[0]
                if plan is None:
                    scan_failed.append(id)
                    continue
                scan_latency.append(plan['Execution Time'])
                scan_sql_latency[j] += plan['Execution Time']


        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    print(scan_failed)
    print(scan_sql_latency)
    print(explain_time, inference_time)
    scan_latency = np.sort(np.array(scan_latency))
    print("sum latency:", np.sum(scan_latency))
    print("median latency:", np.median(scan_latency))
    idx75, idx95 = int(max(0, len(scan_latency) * 0.75 - 1)), int(max(0, len(scan_latency) * 0.95 - 1))
    print("75 latency:", scan_latency[idx75])
    print("95 latency:", scan_latency[idx95])

cur.close()
conn.close()
