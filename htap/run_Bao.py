import psycopg2 ## type: ignore
import os
import torch ## type: ignore
import numpy as np
import time
import datetime
from htap_query import HTAPController
from restore import restore
# os.environ['PGOPTIONS'] = '-c statement_timeout=50000'
from multiprocessing import Pool
import sys
sys.path.append(r"../TCNN")
import model

params = []
for i in range(1, 23):
    path = "../data/params/" + str(i) + ".txt"
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

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    file_path = "../data/global_hint_plan.txt"
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


    choose_latency = []
    sql_latency = np.zeros(22)
    failed = []
    k = 10

    explain_time, inference_time = 0, 0
    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../data/queries_for_plan/" + str(id) + ".sql.template"
            param = params[id - 1]

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
                htapcontroller = HTAPController()

                print("execute stream, sql_id:", i, id)
                print(hint)

                p = Pool(k + 1)
                tp = p.map_async(htapcontroller.oltp_worker, range(k))
                ap = p.starmap_async(htapcontroller.olap_worker, [(hint_sql, param, 20000)])
                p.close()
                p.join()
                plan = ap.get()[0]
                if plan is None:
                    failed.append(id)
                    continue

                choose_latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']

        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    print(failed)
    print(sql_latency)
    print(explain_time, inference_time)

    choose_latency = np.sort(np.array(choose_latency))
    print("sum latency:", np.sum(choose_latency))
    print("median latency:", np.median(choose_latency))
    idx75, idx95 = int(max(0, len(choose_latency) * 0.75 - 1)), int(max(0, len(choose_latency) * 0.95 - 1))
    print("75 latency:", choose_latency[idx75])
    print("95 latency:", choose_latency[idx95])


cur.close()
conn.close()
