import numpy as np
import psycopg2 ## type: ignore
import os
import datetime
import sys
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

    # restore()
    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")
    model = run_train_upd()

    col_latency = []
    col_sql_latency = np.zeros(22)
    col_failed = []
    k = 10
    pred_count = np.zeros(5)
    
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
                tp = p.starmap_async(htapcontroller.oltp_worker, [(i, True, False) for i in range(k)])
                ap = p.starmap_async(htapcontroller.olap_worker, [("EXPLAIN (FORMAT JSON)\n" + sql, param, 20000)])
                p.close()
                p.join()
                plan = tp.get()
                plan.append(ap.get()[0])
                with open("plan.txt", "w") as file:
                    for p in plan:
                        file.write(str(p) + '\n')
                res = run_test_upd(model)
                ## inference ends here. Maybe we can increase it by extending to the code below if necessary

                LEAF_TYPES = ["SeqScan", "IndexScan", "ColumnarScan", "IndexOnlyScan"]
                hint = "/*+\n"
                for rel, node in res.items():
                    if node[3] < 4:
                        hint += LEAF_TYPES[node[3]] + "(" + rel + ")\n"
                    pred_count[node[3]] += 1
                hint += "*/\n"
                
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + exp_sql

                print("execute stream, sql_id:", i, id)
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

    print(col_failed)
    print(col_sql_latency)
    print(pred_count)
    col_latency = np.sort(np.array(col_latency))
    print("sum latency:", np.sum(col_latency))
    print("median latency:", np.median(col_latency))
    idx75, idx95 = int(max(0, len(col_latency) * 0.75 - 1)), int(max(0, len(col_latency) * 0.95 - 1))
    print("75 latency:", col_latency[idx75])
    print("95 latency:", col_latency[idx95])

cur.close()
conn.close()
