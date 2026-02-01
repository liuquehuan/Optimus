import psycopg2 ## type: ignore
import os
import torch ## type: ignore
import numpy as np
import time
import datetime
import sys
sys.path.append(r"../../TCNN")
sys.path.append("../")
from utils.htap_query import HTAPController
from utils.restore import restore_10x
os.environ['PGOPTIONS'] = '-c statement_timeout=100000'
from multiprocessing import Pool
import model

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


def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list


def split(plans):
    X_train, y_train = [], []

    for plan in plans:
        if plan['Execution Time'] is not None:
            X_train.append(plan)
            y_train.append(plan['Execution Time'])

    return  X_train, y_train


def train_and_save_model(fn, X, y, verbose=True):

    reg = model.BaoRegression(verbose=verbose)
    start = time.time()
    reg.fit(X, y)
    end = time.time()
    print("training:", end - start)
    reg.save(fn)
    return reg


if __name__ == "__main__":

    import sys
    if len(sys.argv) != 2:
        print("Usage: train.py MODEL_FILE")
        exit(-1)

    restore_10x()
    choose_latency = []
    sql_latency = np.zeros(22)
    failed = []
    k1, k2 = 500000, 0
    htapcontroller = HTAPController()
    with Pool(100) as p1:
        for i in range(k1 // 100):
            print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    file_path = "../../data/global_hint_plan.txt"
    plans = load_plans(file_path)
    X_train, y_train = split(plans)
    train_and_save_model(sys.argv[1], X_train, y_train)

    print("Model saved, attempting load...")
    reg = model.BaoRegression()
    reg.load(sys.argv[1])

    operator_type = ["enable_indexscan", "enable_seqscan", "google_columnar_engine.enable_columnar_scan", "enable_hashjoin", "enable_mergejoin", "enable_nestloop"]
    row_hint_set = []
    for i in range(0, 64):
        hint = "/*+\n"
        if i >> 3 == 0 or i & 7 == 0:
            continue

        for j in range(0, 6):
            k = i >> j & 1
            if k == 1:
                hint += "Set(" + operator_type[j] +" on)\n"
            else:
                hint += "Set(" + operator_type[j] +" off)\n"
        hint += "*/\n"
        row_hint_set.append(hint)
    
    start_time = time.time()
    explain_time, inference_time = 0, 0
    with Pool(k2 + 1) as p2:
        for i in range(20):
            for j in range(22):
                id = j + 1
                path = "../../data/queries_for_plan/" + str(id) + ".sql.template"
                param = params[id - 1]

                conn = psycopg2.connect(
                    host="localhost",
                    database="htap_sf10",
                    user="postgres",
                    port=5555,
                    password="postgres"
                )
                cur = conn.cursor()
                cur.execute("load \'pg_hint_plan\'")

                with open(path, "r") as file:
                    sql = file.read()
                    X_test = []
                    for hint_id in range(49):
                        explain_sql = row_hint_set[hint_id] + "EXPLAIN (FORMAT JSON)\n" + sql
                        start = time.time()
                        if len(param) == 0:
                            cur.execute(explain_sql)
                        else:
                            cur.execute(explain_sql, param[i * 5])
                        plan = cur.fetchone()[0][0]
                        end = time.time()
                        explain_time += end - start
                        X_test.append(plan)
                    
                    start = time.time()
                    pred = reg.predict(X_test)
                    end = time.time()
                    inference_time += end - start
                    opt_hint_id = np.argmin(pred)
                            
                    hint = row_hint_set[opt_hint_id]
                    sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                    hint_sql = hint + sql
                    param = None if len(param) == 0 else param[i * 5]

                    print("execute stream, sql_id:", i, id)
                    tp2 = p2.map_async(htapcontroller.oltp_worker, range(k2))
                    ap2 = p2.starmap_async(htapcontroller.olap_worker, [(hint_sql, param, 100000)])
                    plan = ap2.get()[0]
                    if plan is None:
                        failed.append(id)
                        continue
                    choose_latency.append(plan['Execution Time'])
                    sql_latency[j] += plan['Execution Time']
                
                cur.execute("show google_columnar_engine.refresh_threshold_percentage")
                print(cur.fetchone())
                cur.execute("show google_columnar_engine.enable_vectorized_join")
                print(cur.fetchone())
                cur.close()
                conn.close()

    print("k1:", k1, "k2:", k2)
    print(time.time() - start_time)

    print(failed)
    print(sql_latency)
    print(explain_time, inference_time)

    latency = np.sort(np.array(choose_latency))
    print("sum latency:", np.sum(latency))
    print("median latency:", np.median(latency))
    idx95, idx99, idx995 = int(max(0, len(latency) * 0.95 - 1)), int(max(0, len(latency) * 0.99 - 1)), int(max(0, len(latency) * 0.995 - 1))
    print("95 latency:", latency[idx95])
    print("99 latency:", latency[idx99])
    print("99.5 latency:", latency[idx995])
