import numpy as np
import psycopg2
import os
import sys
import datetime
import time
from multiprocessing import Pool

CUR_DIR = os.path.dirname(__file__)
PROJ_ROOT = os.path.abspath(os.path.join(CUR_DIR, "..", ".."))
if PROJ_ROOT not in sys.path:
    sys.path.append(PROJ_ROOT)
CODE_DIR = os.path.join(PROJ_ROOT, "code")
if CODE_DIR not in sys.path:
    sys.path.append(CODE_DIR)

from utils.htap_query import HTAPController
from utils.restore import restore
from utils.utils import set_refresh_threshold, run_ap_with_tp

params = []
PARAM_ROOT = os.path.join(CODE_DIR, "params")
for i in range(1, 23):
    path = os.path.join(PARAM_ROOT, f"{i}.txt")
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
    # set_refresh_threshold(100)

    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")

    pg_latency = []
    sql_latency = np.zeros(22)
    k1, k2 = 50000, 20

    htapcontroller = HTAPController()

    with Pool(100) as p1:
        for i in range(k1 // 100):
            print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))
    

    start_time = time.time()
    for i in range(20):
        for j in range(22):
            id = j + 1
            # 实际目录是 code/queries_for_plan，因此用 CODE_DIR 拼绝对路径
            query_tpl_path = os.path.join(CODE_DIR, "queries_for_plan", f"{id}.sql.template")

            with open(query_tpl_path, "r") as file:
                sql = file.read()
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]
                print("execute stream, sql_id:", i, id)

                plan, tp_results = run_ap_with_tp(exp_sql, param, k2, htapcontroller)
                # 与 run_gpredictor.py 一致的健壮性处理，防止 None/缺字段导致实验中断
                if plan is None:
                    print(f"[warn] OLAP plan is None for stream {i}, qid {id}, skip this run")
                    continue
                if 'Execution Time' not in plan:
                    print(f"[warn] plan has no 'Execution Time' field for stream {i}, qid {id}, skip this run")
                    continue

                pg_latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']

    print(time.time() - start_time)
    cur.execute("show google_columnar_engine.enable_vectorized_join")
    print(cur.fetchone())
    cur.execute("show google_columnar_engine.refresh_threshold_percentage")
    print(cur.fetchone())
    print("k1:", k1, "k2:", k2)
    
    print(sql_latency)
    pg_latency = np.sort(np.array(pg_latency))
    print("sum latency:", np.sum(pg_latency))
    print("median latency:", np.median(pg_latency))
    idx95, idx99, idx995 = int(max(0, len(pg_latency) * 0.95 - 1)), int(max(0, len(pg_latency) * 0.99 - 1)), int(max(0, len(pg_latency) * 0.995 - 1))
    print("95 latency:", pg_latency[idx95])
    print("99 latency:", pg_latency[idx99])
    print("995 latency:", pg_latency[idx995])

    # set_refresh_threshold(50)
    cur.close()
    conn.close()
