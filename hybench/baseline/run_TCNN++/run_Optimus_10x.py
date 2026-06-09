import psycopg2
import os
import model
import torch
import numpy as np
import time
import datetime
os.environ['PGOPTIONS'] = '-c statement_timeout=200000'

conn = psycopg2.connect(
    host="localhost",
    database="htap_sf10",
    user="postgres",
    port=5555,
    password="postgres"
)
cur = conn.cursor()
cur.execute("load \'pg_hint_plan\'")

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


def split(plans, n, train_pos, num_stream):
    X_train, y_train = [], []

    for i in range(num_stream):
        start = i * 22 * n
        for id in range(22):
            cur_id = start + id
            cost = []
            
            for _ in range(n):
                y = plans[cur_id]['Execution Time']
                if y is not None:
                    cost.append(y)
                else:
                    cost.append(30000)
                cur_id += 22
            y_train.append(cost.index(min(cost)))

            if train_pos == -1:
                X_train.append(plans[start + id + 22 * cost.index(min(cost))])
            else:
                X_train.append(plans[start + id + 22 * train_pos])

    # if (train_pos == -1):
    #     print(X_train[15], X_train[22 + 15], X_train[22 * 2 + 15])
    return  X_train, y_train


def cost_split(plans, n, train_pos, num_stream):
    X_cost, y_cost, X_latency, y_latency = [], [], [], []

    for i in range(num_stream):
        start = i * 22 * n
        for id in range(22):
            cur_id = start + id
            cost = []
            
            for _ in range(n):
                plan_cost = plans[cur_id]['Execution Time'] if 'Execution Time' in plans[cur_id] else plans[cur_id]['Plan']['Total Cost']

                if plan_cost is not None:
                    cost.append(plan_cost)
                else:
                    cost.append(30000)
                cur_id += 22
            if 'Execution Time' in plans[start]:
                y_latency.append(cost.index(min(cost)))
                if train_pos == -1:
                    X_latency.append(plans[start + id + 22 * cost.index(min(cost))])
                else:
                    X_latency.append(plans[start + id + 22 * train_pos])
            else:
                y_cost.append(cost.index(min(cost)))
                if train_pos == -1:
                    X_cost.append(plans[start + id + 22 * cost.index(min(cost))])
                else:
                    X_cost.append(plans[start + id + 22 * train_pos])

    if (train_pos == -1):
        print(X_latency[15], X_latency[22 + 15], X_latency[22 * 2 + 15])
    return  X_cost, y_cost, X_latency, y_latency


def train_and_save_model(fn, X, y, verbose=True, node_level=False, reg=None):
    if reg is None:
        reg = model.BaoRegression(verbose=verbose, node_level=node_level)

    start = time.time()
    reg.fit(X, y)
    end = time.time()
    print("training:", end - start)
    reg.save(fn)
    return reg


if __name__ == "__main__":

    import sys
    if len(sys.argv) != 3:
        print("Usage: train.py SCAN_MODEL_FILE JOIN_MODEL_FILE")
        exit(-1)

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    # scan_plan_path, join_plan_path = "../../data/col_hint_plan.txt", "../../data/join_type_hint_plan.txt"
    scan_plan_path, join_plan_path = "../../data/cost_col_hint_plan_10x.txt", "../../data/scan_aware_cost_join_type_hint_plan_10x.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    # X_train, y_train = split(scan_plan, 35, 32, 802
    # train_and_save_model(sys.argv[1], X_train, y_train)
    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 80)
    scan_reg = train_and_save_model(sys.argv[1], X_cost, y_cost)
    train_and_save_model(sys.argv[1], X_latency, y_latency, reg=scan_reg)

    # X_train, y_train = split(join_plan, 7, -1, 80)
    # train_and_save_model(sys.argv[2], X_train, y_train, node_level=True)
    X_cost, y_cost, X_latency, y_latency = cost_split(join_plan, 7, -1, 80)
    join_reg = train_and_save_model(sys.argv[2], X_cost, y_cost, node_level=True)
    train_and_save_model(sys.argv[2], X_latency, y_latency, reg=join_reg, node_level=True)


    print("Model saved, attempting load...")
    scan_reg = model.BaoRegression()
    scan_reg.load(sys.argv[1])

    join_reg = model.BaoRegression()
    join_reg.load(sys.argv[2])

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
    for i in range(20):
        for j in range(22):
        # for j in range(17, 18):
            id = j + 1
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"
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
                scan_hint_id = int(scan_reg.predict(plan)[0])
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
                join_hint = join_reg.predict(plan)
                end = time.time()
                inference_time += end - start

                hint = "/*+\n" + scan_hint_set[scan_hint_id] + join_hint + "*/\n"
                sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + sql

                print("execute stream, sql_id:", i, id)
                print(hint)

                cur.execute("set google_columnar_engine.enable_columnar_scan=on")
                cur.execute("set google_columnar_engine.enable_vectorized_join=on")
                try:
                    if len(param) == 0:
                        cur.execute(hint_sql)
                    else:
                        cur.execute(hint_sql, param[i * 5])
                        # print(hint_sql % param[i * 5])
                    plan = cur.fetchone()[0][0]
                    # print(plan)
                    latency.append(plan['Execution Time'])
                    sql_latency[j] += plan['Execution Time']
                except psycopg2.extensions.QueryCanceledError as e:
                    conn.rollback()
                    failed.append((i, id))

                # cur.execute("set google_columnar_engine.enable_columnar_scan=on")
                # cur.execute("set google_columnar_engine.enable_vectorized_join=on")
                # try:
                #     if len(param) == 0:
                #         cur.execute(scan_hint + sql)
                #     else:
                #         cur.execute(scan_hint + sql, param[i * 5])
                #     plan = cur.fetchone()[0][0]
                #     # print(plan)
                #     scan_latency.append(plan['Execution Time'])
                # except psycopg2.extensions.QueryCanceledError as e:
                #     conn.rollback()
                #     scan_failed.append((i, id))
                
                # cur.execute("set google_columnar_engine.enable_columnar_scan=on")
                # cur.execute("set google_columnar_engine.enable_vectorized_join=on")
                # try:
                #     if len(param) == 0:
                #         cur.execute("/*+\n" + join_hint + "*/" + sql)
                #     else:
                #         cur.execute("/*+\n" + join_hint + "*/" + sql, param[i * 5])
                #     plan = cur.fetchone()[0][0]
                #     print(plan)
                #     join_latency.append(plan['Execution Time'])
                # except psycopg2.extensions.QueryCanceledError as e:
                #     conn.rollback()
                #     join_failed.append((i, id))

        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    scan_sql_latency = np.zeros(22)
    scan_explain_time, scan_inference_time = 0, 0
    for i in range(20):
        for j in range(22):
        # for j in range(17, 18):
            id = j + 1
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"
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
                scan_explain_time += end - start
                start = time.time()
                scan_hint_id = int(scan_reg.predict(plan)[0])
                end = time.time()
                scan_inference_time += end - start
                scan_hint = "/*+\n" + scan_hint_set[scan_hint_id] + "*/\n"

                sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = scan_hint + sql

                print("execute stream, sql_id:", i, id)
                print(scan_hint)

                cur.execute("set google_columnar_engine.enable_columnar_scan=on")
                cur.execute("set google_columnar_engine.enable_vectorized_join=on")
                try:
                    if len(param) == 0:
                        cur.execute(scan_hint + sql)
                    else:
                        cur.execute(scan_hint + sql, param[i * 5])
                    plan = cur.fetchone()[0][0]
                    # print(plan)
                    scan_latency.append(plan['Execution Time'])
                    scan_sql_latency[j] += plan['Execution Time']
                except psycopg2.extensions.QueryCanceledError as e:
                    conn.rollback()
                    scan_failed.append((i, id))

        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    print(sql_latency)
    print(scan_sql_latency)
    print(explain_time, inference_time)
    print(scan_explain_time, scan_inference_time)
    
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


cur.close()
conn.close()
