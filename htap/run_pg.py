import numpy as np
import psycopg2
import os
from htap_query import HTAPController
from multiprocessing import Pool
from restore import restore
import datetime

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

    pg_latency = []
    k = 10

    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]
                htapcontroller = HTAPController()

                print("execute stream, sql_id:", i, id)

                # pg
                p = Pool(k + 1)
                tp = p.map_async(htapcontroller.oltp_worker, range(k))
                ap = p.starmap_async(htapcontroller.olap_worker, [(exp_sql, param)])
                p.close()
                p.join()
                plan = ap.get()[0]
                pg_latency.append(plan['Execution Time'])

        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    pg_latency = np.sort(np.array(pg_latency))
    print("sum latency:", np.sum(pg_latency))
    print("median latency:", np.median(pg_latency))
    idx75, idx95 = int(max(0, len(pg_latency) * 0.75 - 1)), int(max(0, len(pg_latency) * 0.95 - 1))
    print("75 latency:", pg_latency[idx75])
    print("95 latency:", pg_latency[idx95])

cur.close()
conn.close()
