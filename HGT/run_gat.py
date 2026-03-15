import numpy as np # type: ignore
import psycopg2 ## type: ignore
import os
import datetime
import sys
import time
sys.path.append(r"../")
from utils.htap_query import HTAPController
from utils.utils import load_plans, cost_split
from multiprocessing import Pool
from utils.restore import restore
from train import Optimus

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


def train_and_save_model(fn, X, y, verbose=True, node_level=False, reg=None):
    if reg is None:
        reg = Optimus(verbose=verbose, node_level=node_level)

    start = time.time()
    reg.run_train_upd(X, y)
    end = time.time()
    print("training:", end - start)
    reg.save(fn)
    return reg


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: run_gat.py SCAN_MODEL_FILE JOIN_MODEL_FILE")
        exit(-1)

    feature_num = 3 + 13
    scan_plan_path, join_plan_path = "../../data/cost_col_hint_plan.txt", "../../data/cost_join_type_hint_plan.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 80)
    scan_model = train_and_save_model(sys.argv[1], X_cost, y_cost)
    scan_model = train_and_save_model(sys.argv[1], X_latency, y_latency, reg=scan_model)

    X_cost, y_cost, X_latency, y_latency = cost_split(join_plan, 7, -1, 80)
    join_model = train_and_save_model(sys.argv[2], X_cost, y_cost, node_level=True)
    join_model = train_and_save_model(sys.argv[2], X_latency, y_latency, reg=join_model, node_level=True)

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

    latency, scan_latency, join_latency = [], [], []
    sql_latency = np.zeros(22)
    failed, scan_failed, join_failed = [], [], []
    explain_time, inference_time = 0, 0

    k1, k2 = 50000, 0
    restore()
    htapcontroller = HTAPController()
    print("start tp")
    for _ in range(k1 // 100):
        print(_)
        p1 = Pool(100)
        tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))
        p1.close()
        p1.join()

    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")

    start_time = time.time()
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

                start = time.time()
                scan_hint_id = int(scan_model.run_test_upd(plan))
                end = time.time()
                inference_time += end - start
                ## inference ends here. Maybe we can increase it by extending to the code below if necessary

                scan_hint = "/*+\n" + scan_hint_set[scan_hint_id] + "*/\n"
                explain_sql = scan_hint + "EXPLAIN (FORMAT JSON)\n" + sql

                start = time.time()
                if len(param) == 0:
                    cur.execute(explain_sql)
                else:
                    cur.execute(explain_sql, param[i * 5])
                plan = cur.fetchone()[0][0]
                end = time.time()
                explain_time += end - start

                start = time.time()
                join_hint = join_model.run_test_upd(plan)
                end = time.time()
                inference_time += end - start

                param = None if len(param) == 0 else param[i * 5]

                hint = "/*+\n" + scan_hint_set[scan_hint_id] + join_hint + "*/\n"
                sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + sql

                print("execute stream, sql_id:", i, id)

                p = Pool(k2 + 1)
                tp = p.map_async(htapcontroller.oltp_worker, range(k2))
                ap = p.starmap_async(htapcontroller.olap_worker, [(hint_sql, param, 20000)])
                p.close()
                p.join()
                plan = ap.get()[0]
                if plan is None:
                    failed.append(id)
                    continue
                latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']

    print(time.time() - start_time)
    cur.execute("show google_columnar_engine.refresh_threshold_percentage")
    print(cur.fetchone())
    cur.execute("show google_columnar_engine.enable_columnar_scan")
    print(cur.fetchone())
    cur.execute("show google_columnar_engine.enable_vectorized_join")
    print(cur.fetchone())
    conn.close()
    cur.close()
    print("k1:", k1, "k2:", k2)

    print(sql_latency)
    print(explain_time, inference_time)
    
    print(failed)
    print(scan_failed)
    print(join_failed)
    latency = np.sort(np.array(latency))
    scan_latency = np.sort(np.array(scan_latency))
    join_latency = np.sort(np.array(join_latency))
    print("sum latency:", np.sum(latency), np.sum(scan_latency), np.sum(join_latency))
    print("median latency:", np.median(latency), np.median(scan_latency) if len(scan_latency) > 0 else 0, np.median(join_latency) if len(join_latency) > 0 else 0)
    idx95, idx99, idx995 = int(max(0, len(latency) * 0.95 - 1)), int(max(0, len(latency) * 0.99 - 1)), int(max(0, len(latency) * 0.995 - 1))
    print("95 latency:", latency[idx95], scan_latency[idx95] if len(scan_latency) > idx95 else 0, join_latency[idx95] if len(join_latency) > idx95 else 0)
    print("99 latency:", latency[idx99], scan_latency[idx99] if len(scan_latency) > idx99 else 0, join_latency[idx99] if len(join_latency) > idx99 else 0)
    print("99.5 latency:", latency[idx995], scan_latency[idx995] if len(scan_latency) > idx995 else 0, join_latency[idx995] if len(join_latency) > idx995 else 0)
