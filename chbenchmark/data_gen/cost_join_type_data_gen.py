import psycopg2
import datetime
import os
import time
import sys
sys.path.append("../")
from utils.restore import restore
from utils.htap_query import HTAPController
from multiprocessing import Pool
start = time.time()


def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list
    

def split(plans, n, train_pos, num_stream):
    X_train, y_train = [], []

    for i in range(num_stream):
        start = i * 22 * n
        for id in range(22):
            cur_id = start + id
            cost = []
            
            for _ in range(n):
                y = None
                if plans[cur_id] is not None:
                    if 'Execution Time' in plans[cur_id]:
                        y = plans[cur_id]['Execution Time']
                    else:
                        y = plans[cur_id]['Plan']['Total Cost']

                if y is not None:
                    cost.append(y)
                else:
                    cost.append(6000)
                cur_id += 22
            y_train.append(cost.index(min(cost)))

            if train_pos == -1:
                X_train.append(plans[start + id + 22 * cost.index(min(cost))])
            else:
                X_train.append(plans[start + id + 22 * train_pos])

    # if (train_pos == -1):
    #     print(X_train[15], X_train[22 + 15], X_train[22 * 2 + 15])
    return  X_train, y_train


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

scan_plan_path = "../../data/2a_cost_col_hint_plan.txt"
scan_plan = load_plans(scan_plan_path)
X_train, y_train = split(scan_plan, 35, 32, 80)

operator_type = ["enable_hashjoin", "enable_mergejoin", "enable_nestloop"]
hint_set = []
for i in range(1, 8):
    # hint = "/*+\n"
    hint = ""

    for j in range(0, 3):
        k = i >> j & 1
        if k == 1:
            hint += "Set(" + operator_type[j] +" on)\n"
        else:
            hint += "Set(" + operator_type[j] +" off)\n"
    
    # hint += "*/\n"
    hint_set.append(hint)
print(len(hint_set)) ## 7

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

pos = -1
restore()
k1 = 100
for i in range(0, 100):
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
                    hint_sql = "/*+\n" + scan_hint_set[y_train[pos + id]] + hint + "*/\n" + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                else:
                    hint_sql = "/*+\n" + scan_hint_set[y_train[pos + id]] + hint + "*/\n" + "EXPLAIN (FORMAT JSON)\n" + sql
                param = params[id - 1]

                print("execute stream, hint_id, sql_id:", i, hint_id, id)
                plan = None

                try:
                    if len(param) == 0:
                        cur.execute(hint_sql)
                    else:
                        cur.execute(hint_sql, param[i])
                    plan = cur.fetchone()[0][0]
                except psycopg2.extensions.QueryCanceledError as e:
                    print("timeout")
                    conn.rollback()
                    print("done rollback")
                    print(timeout[id - 1])
                    hint_sql = "/*+\n" + scan_hint_set[y_train[pos + id]] + hint + "*/\n" + "EXPLAIN (FORMAT JSON)\n" + sql
                    if len(param) == 0:
                        cur.execute(hint_sql)
                    else:
                        cur.execute(hint_sql, param[i])
                    plan = cur.fetchone()[0][0]
                    plan['Execution Time'] = None

                if i % 10 == 1 and plan['Execution Time'] is not None:
                    timeout[id - 1] = min(timeout[id - 1], plan['Execution Time'])
                    timeout[id - 1] = max(timeout[id - 1], 100)

                stream[hint_id].append(plan)
            
            cur.close()
            conn.close()
            hint_id += 1
    
    with open("../../data/stage2_scan_aware_cost_join_type_hint_plan.txt", "a+") as file:
        for hint in stream:
            for plan in hint:
                file.write(str(plan) + '\n')
    pos += 22

end = time.time()
print("labelling time:", end - start)
