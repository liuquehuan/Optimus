import numpy as np # type: ignore
import psycopg2 ## type: ignore
import os
import datetime
import sys
import copy
import time
sys.path.append(r"..")
from htap_query import HTAPController
from multiprocessing import Pool
from restore import restore
from train import run_train_upd, run_test_upd

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
    model, cost_label_count = run_train_upd("../../data/exp_data/best_5_exp_plans", 60 * 22 * 5)
    cost_label_count = copy.deepcopy(cost_label_count)
    model, late_label_count = run_train_upd("../../data/gnn_data/best_10_plans", 20 * 22 * 10, model)

    col_latency = []
    col_sql_latency = np.zeros(22)
    col_failed = []
    k = 10
    pred_count = np.zeros(4)
    
    explain_time = 0
    inference_time = 0
    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]

                ## inference starts here, io included
                p = Pool(k + 1)
                htapcontroller = HTAPController()
                start = time.time()
                tp = p.starmap_async(htapcontroller.oltp_worker, [(i, True, False) for i in range(k)])
                ap = p.starmap_async(htapcontroller.olap_worker, [("EXPLAIN (FORMAT JSON)\n" + sql, param, 20000)])
                p.close()
                p.join()
                end = time.time()
                explain_time += end - start
                plan = tp.get()
                plan.append(ap.get()[0])
                with open("plan.txt", "w") as file:
                    for p in plan:
                        file.write(str(p) + '\n')
                start = time.time()
                res = run_test_upd(model)
                end = time.time()
                inference_time += end - start
                ## inference ends here. Maybe we can increase it by extending to the code below if necessary

                print("execute stream, sql_id:", i, id)

                LEAF_TYPES = ["SeqScan", "IndexScan", "ColumnarScan", "IndexOnlyScan"]
                hint = "/*+\n"
                for rel, node in res.items():
                    if node[-2] < 4:
                        hint += LEAF_TYPES[node[-2]] + "(" + rel + ")\n"
                    pred_count[node[-2]] += 1
                hint += "*/\n"
                
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + exp_sql

                p = Pool(k + 1)
                tp = p.map_async(htapcontroller.oltp_worker, range(k))
                ap = p.starmap_async(htapcontroller.olap_worker, [(hint_sql, param, 20000)])
                p.close()
                p.join()
                plan = ap.get()[0]
                if plan is None:
                    col_failed.append(id)
                    continue
                col_latency.append(plan['Execution Time'])
                col_sql_latency[j] += plan['Execution Time']


        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    print(cost_label_count)
    print(late_label_count)
    print(col_failed)
    print(col_sql_latency)
    print(pred_count)
    print(pred_count / np.sum(pred_count))
    print(explain_time, inference_time)
    col_latency = np.sort(np.array(col_latency))
    print("sum latency:", np.sum(col_latency))
    print("median latency:", np.median(col_latency))
    idx75, idx95 = int(max(0, len(col_latency) * 0.75 - 1)), int(max(0, len(col_latency) * 0.95 - 1))
    print("75 latency:", col_latency[idx75])
    print("95 latency:", col_latency[idx95])

cur.close()
conn.close()
