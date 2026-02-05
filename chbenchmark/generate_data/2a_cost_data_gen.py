import psycopg2 # type: ignore
import datetime
import os
import sys
sys.path.append("..")
from utils.restore import restore
from utils.htap_query import HTAPController
from multiprocessing import Pool
import time

start = time.time()
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

k1 = 100
restore()
for i in range(100):
    if i % 5 == 0:
        continue
    timeout = [6000] * 22
    stream = [[] for _ in range(35)]
    
    for id in range(1, 23):
        hint_id = 0
        htapcontroller = HTAPController()
        for _ in range(k1 // 100):
            p1 = Pool(100)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))
            p1.close()
            p1.join()

        for hint in hint_set:
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"

            os.environ['PGOPTIONS'] = '-c statement_timeout=' + str(timeout[id - 1])
            conn = psycopg2.connect(
                host="localhost",
                database="htap_sf1",
                user="postgres",
                port=5555,
                password="postgres"
            )
            cur = conn.cursor()
            cur.execute("load \'pg_hint_plan\'")
            cur.execute("set google_columnar_engine.enable_vectorized_join=on")
            cur.execute("set google_columnar_engine.enable_columnar_scan=on")

            with open(path, "r") as file:
                sql = file.read()
                if i % 10 == 1:
                    hint_sql = hint + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                else:
                    hint_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql
                param = params[id - 1]

                print("execute stream, hint_id, sql_id:", i, hint_id, id)
                plan = None
                prev = hint_id

                if hint_id < 32:
                    for table_name in table:
                        # print(table_name, sql)
                        if table_name not in sql:
                            idx = table.index(table_name)
                            prev = prev & (~(1 << idx))
                if prev == hint_id:
                    try:
                        if len(param) == 0:
                            cur.execute(hint_sql)
                        else:
                            cur.execute(hint_sql, param[i])
                        print(prev, "done")
                        plan = cur.fetchone()[0][0]
                    except psycopg2.extensions.QueryCanceledError as e:
                        print(prev, "timeout")
                        conn.rollback()
                        print("done rollback")
                        print(timeout[id - 1])
                        hint_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql
                        if len(param) == 0:
                            cur.execute(hint_sql)
                        else:
                            cur.execute(hint_sql, param[i])
                        plan = cur.fetchone()[0][0]
                        plan['Execution Time'] = None

                        
                    if i % 10 == 1 and plan['Execution Time'] is not None:
                        timeout[id - 1] = min(timeout[id - 1], plan['Execution Time'])
                        timeout[id - 1] = max(timeout[id - 1], 300)
                else:
                    print(prev, "skip")
                    plan = None

                stream[hint_id].append(plan)
            cur.close()
            conn.close()
            hint_id += 1
        

    with open("../../data/2a_cost_col_hint_plan.txt", "a+") as file:
        for hint in stream:
            for plan in hint:
                file.write(str(plan) + '\n')


end = time.time()
print("labelling time:", end - start)
