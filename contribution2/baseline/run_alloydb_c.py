import numpy as np
import psycopg2
import os
import sys
sys.path.append("../")
from utils.htap_query import HTAPController
from multiprocessing import Pool
from utils.restore import restore
import datetime
import time
from utils.utils import set_refresh_threshold

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
    cur.execute("set enable_seqscan = off")
    cur.execute("set enable_indexscan = off")
    cur.execute("set enable_indexonlyscan = off")
    cur.execute("show google_columnar_engine.enable_vectorized_join")
    print(cur.fetchone())
    cur.execute("show google_columnar_engine.refresh_threshold_percentage")
    print(cur.fetchone())

    pg_latency = []
    sql_latency = np.zeros(22)
    k1, k2 = 50000, 0

    htapcontroller = HTAPController()
    with Pool(100) as p1:
        for i in range(k1 // 100):
            print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))

    
    start_time = time.time()
    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]
                print("execute stream, sql_id:", i, id)
                p2 = Pool(k2 + 1)
                tp2 = p2.map_async(htapcontroller.oltp_worker, range(k2))
                ap2 = p2.starmap_async(htapcontroller.olap_worker, [(exp_sql, param)])
                plan = ap2.get()[0]
                p2.close()
                p2.join()
                if plan is not None:
                    pg_latency.append(plan['Execution Time'])
                    sql_latency[j] += plan['Execution Time']
                else:
                    print(f"警告: SQL {id} 在第 {i} 次执行时返回了 None")

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
