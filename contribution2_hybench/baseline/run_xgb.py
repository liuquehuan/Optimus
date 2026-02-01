import xgboost as xgb
import numpy as np
import os
import psycopg2
import time
import datetime
from restore import restore
from htap_query import HTAPController
from multiprocessing import Pool

os.environ['PGOPTIONS'] = '-c statement_timeout=20000'

conn = psycopg2.connect(
    host="localhost",
    database="htap_sf1",
    user="postgres",
    port=5555,
    password="postgres"
)
cur = conn.cursor()
cur.execute("load \'pg_hint_plan\'")

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


def convert(plan):
    plan = plan['Plan']
    return np.array([plan['Total Cost'], plan['Startup Cost'], plan['Plan Rows'], plan['Plan Width']])


def op_and_row_count(plan):
    res = np.array([1, plan['Plan Rows'], 0, 0])
    if (plan['Node Type'] == 'Custom Scan'):
        res[2], res[3] = 1, plan['Plan Rows']

    children = plan["Plans"] if "Plans" in plan else []
    for child in children:
        res += op_and_row_count(child)
    return res


def convert2(plan):
    feature1 = convert(plan)
    plan = plan['Plan']
    return np.concatenate((feature1, op_and_row_count(plan)))
    

def train(X, y, num_class):

    dtrain = xgb.DMatrix(X, y)
    param = {
        'objective': 'multi:softmax',   # 多分类任务，输出最终分类编号
        'num_class': num_class,                # 你有 35 个不同的类别
        'max_depth': 5,                 # 树的最大深度（可调）
        'eta': 0.1,                     # 学习率（可调）
        'eval_metric': 'merror',       # 多分类错误率
        'seed': 42                      # 随机种子（可选）
    }
    model = xgb.train(param, dtrain, num_boost_round=20)
    return model


def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list
    

def split(plans, n, train_pos):
    X_train, y_train = [], []

    for i in range(80):
        start = i * 22 * n
        for id in range(22):
            cur_id = start + id
            X_train.append(convert2(plans[cur_id + 22 * train_pos]))
            cost = []
            
            for _ in range(n):
                y = plans[cur_id]['Execution Time']
                if y is not None:
                    cost.append(y)
                else:
                    cost.append(6000)
                cur_id += 22
            y_train.append(cost.index(min(cost)))

    return  X_train, y_train
    

if __name__ == "__main__":

    restore()
    scan_plan_path, join_plan_path = "../data/col_hint_plan.txt", "../data/join_type_hint_plan.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    X_train, y_train = split(scan_plan, 35, 32)
    scan_model = train(X_train, y_train, 35)

    X_train, y_train = split(join_plan, 7, 6)
    join_model = train(X_train, y_train, 7)

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

    join_type = ["enable_hashjoin", "enable_mergejoin", "enable_nestloop"]
    join_hint_set = []
    for i in range(1, 8):
        hint = ""
        # if i >> 3 == 0 or i & 7 == 0:
        #     continue

        for j in range(0, 3):
            k = i >> j & 1
            if k == 1:
                hint += "Set(" + join_type[j] +" on)\n"
            else:
                hint += "Set(" + join_type[j] +" off)\n"
        
        join_hint_set.append(hint)


    choose_latency = []
    sql_latency = np.zeros(22)
    failed = []
    explain_time, inference_time = 0, 0
    k = 40
    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../data/queries_for_plan/" + str(id) + ".sql.template"
            param = params[id - 1]

            with open(path, "r") as file:
                sql = file.read()
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
                X_test = [convert2(plan)]
                dtest = xgb.DMatrix(X_test)
                scan_hint_id = int(scan_model.predict(dtest)[0])
                end = time.time()
                inference_time += end - start
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
                X_test = [convert2(plan)]
                dtest = xgb.DMatrix(X_test)
                join_hint_id = int(join_model.predict(dtest)[0])
                end = time.time()
                inference_time += end - start
                join_hint = "/*+\n" + join_hint_set[join_hint_id] + "*/\n"

                hint = "/*+\n" + scan_hint_set[scan_hint_id] + join_hint_set[join_hint_id] + "*/\n"

                sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]
                htapcontroller = HTAPController()

                print("execute stream, sql_id:", i, id)
                # if len(param) == 0:
                #     print(hint_sql)
                # else:
                #     print(hint_sql % param[i * 5])

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

    print(sql_latency)
    print(explain_time, inference_time)
    
    print(failed)
    choose_latency = np.sort(np.array(choose_latency))
    print("sum latency:", np.sum(choose_latency))
    print("median latency:", np.median(choose_latency))
    idx75, idx95 = int(max(0, len(choose_latency) * 0.75 - 1)), int(max(0, len(choose_latency) * 0.95 - 1))
    print("75 latency:", choose_latency[idx75])
    print("95 latency:", choose_latency[idx95])


cur.close()
conn.close()
