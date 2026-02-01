import psycopg2 # type: ignore
import datetime
import os
import sys
sys.path.append("..")
from restore import restore
from htap_query import HTAPController
from multiprocessing import Pool

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


table = ["order_line", "stock", "customer", "orders", "item"]
hint_set = []
for i in range(0, 32):
    hint = "/*+\n"

    for j in range(0, 5):
        k = i >> j & 1
        if k == 1:
            hint += "ColumnarScan(" + table[j] +")\n"
        else:
            hint += "NoColumnarScan(" + table[j] +")\n"
    
    hint += "*/\n"
    hint_set.append(hint)
hint_set.append("/*+\n*/")
hint_set.append('''
                    /*+\n
                    Set(google_columnar_engine.enable_columnar_scan on)\n
                    Set(google_columnar_engine.enable_vectorized_join on)\n
                    Set(enable_indexscan off)\n
                    Set(enable_seqscan off)\n
                    Set(enable_indexonlyscan off)\n
                    */\n
                ''')
hint_set.append('''
                    /*+\n
                    Set(google_columnar_engine.enable_columnar_scan off)\n
                    Set(google_columnar_engine.enable_vectorized_join off)\n
                    Set(enable_indexscan on)\n
                    Set(enable_seqscan on)\n
                    Set(enable_indexonlyscan on)\n
                    */\n
                ''')

k = 10
restore()
for hint_id in range(0, len(hint_set)):
    hint_plan = []
    hint = hint_set[hint_id]

    for i in range(100):
        if i % 5 == 1 or i % 5 == 0:
            continue
                
        for id in range(1, 23):
            conn = psycopg2.connect(
                host="localhost",
                database="htap_sf1",
                user="postgres",
                port=5555,
                password="postgres"
            )
            cur = conn.cursor()
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                hint_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i]

                print("execute stream, hint_id, sql_id:", i, hint_id, id)

                p = Pool(k + 1)
                htapcontroller = HTAPController()
                tp = p.starmap_async(htapcontroller.oltp_worker, [(i, True, False) for i in range(k)])
                ap = p.starmap_async(htapcontroller.olap_worker, [(hint_sql, param, 6000)])
                p.close()
                p.join()
                plan = tp.get()
                plan.append(ap.get()[0])

                hint_plan.append(plan)

            cur.execute("show google_columnar_engine.enable_vectorized_join")
            print(cur.fetchone())
    
    os.makedirs("../../data/exp_data/" + str(hint_id), exist_ok=True)
    id = 0
    for plans in hint_plan:
        with open("../../data/exp_data/" + str(hint_id) + "/" + str(id) + ".txt", "a+") as file:
            for plan in plans:
                file.write(str(plan) + '\n')
            id += 1

cur.close()
conn.close()
