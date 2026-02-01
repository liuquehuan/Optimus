import psycopg2 # type: ignore
import os
# import model
import torch # type: ignore
import numpy as np # type: ignore
import time
import datetime
from multiprocessing import Pool
import sys
import argparse
sys.path.append("../../../")
from model import train_and_save_model
from utils.htap_query import HTAPController
from utils.restore import restore, restore_10x
from utils.utils import set_refresh_threshold, run_ap_with_tp

params = []
for i in range(1, 23):
    path = "../../../data/params/" + str(i) + ".txt"
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


def cost_split(plans, n, train_pos, num_stream):
    X_cost, y_cost, X_latency, y_latency = [], [], [], []
    failed_count = 0

    for i in range(num_stream):
        start = i * 22 * n
        for id in range(22):
            cur_id = start + id
            cost = []
            
            for _ in range(n):
                plan_cost = None
                if plans[cur_id] is not None:
                    plan_cost = plans[cur_id]['Execution Time'] if 'Execution Time' in plans[cur_id] else plans[cur_id]['Plan']['Total Cost']

                if plan_cost is not None:
                    cost.append(plan_cost)
                else:
                    cost.append(6000000000000)
                cur_id += 22
            
            if min(cost) == 6000000000000:
                failed_count += 1
            else:
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

    print("failed_count:", failed_count)
    # assert failed_count == 0
    if (train_pos == -1):
        print(X_latency[15], X_latency[22 + 15], X_latency[22 * 2 + 15])
    return  X_cost, y_cost, X_latency, y_latency


if __name__ == "__main__":
    import sys
    parser = argparse.ArgumentParser(description='运行 TCNN-- 基准测试')
    parser.add_argument('scan_model', type=str, help='扫描模型文件路径')
    parser.add_argument('join_model', type=str, help='连接模型文件路径')
    parser.add_argument('--db', type=str, default='10x', choices=['1x', '10x'],
                        help='选择数据库规模: 1x 或 10x (默认)')
    args = parser.parse_args()

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    # 根据数据库规模选择不同的计划文件
    if args.db == '10x':
        scan_plan_path, join_plan_path = "../../../data/cost_col_hint_plan_10x.txt", "../../../data/scan_aware_cost_join_type_hint_plan_10x.txt"
    else:
        scan_plan_path, join_plan_path = "../../../data/cost_col_hint_plan.txt", "../../../data/cost_join_type_hint_plan.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 80)
    scan_reg = train_and_save_model(args.scan_model, X_cost, y_cost)
    scan_reg = train_and_save_model(args.scan_model, X_latency, y_latency, reg=scan_reg)
    del scan_plan

    X_cost, y_cost, X_latency, y_latency = cost_split(join_plan, 7, -1, 80)
    join_reg = train_and_save_model(args.join_model, X_cost, y_cost, node_level=True)
    join_reg = train_and_save_model(args.join_model, X_latency, y_latency, reg=join_reg, node_level=True)
    del join_plan

    # print("Model saved, attempting load...")
    # scan_reg = model.BaoRegression()
    # scan_reg.load(sys.argv[1])

    # join_reg = model.BaoRegression()
    # join_reg.load(sys.argv[2])

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
    sql_latency, scan_sql_latency, join_sql_latency = np.zeros(22), np.zeros(22), np.zeros(22)
    failed, scan_failed, join_failed = [], [], []
    explain_time, inference_time = 0, 0
    tp_count = 0
    # 根据数据库规模设置参数
    if args.db == '10x':
        k1, k2 = 500000, 30
        pool_size = 100
        restore_10x()
        database_name = "htap_sf10"
        timeout = 100000
    else:
        k1, k2 = 50000, 40
        pool_size = 50
        restore()
        database_name = "htap_sf1"
        timeout = 20000
    # set_refresh_threshold(50)
    htapcontroller = HTAPController()
    print("start tp")
    with Pool(pool_size) as p1:
        for _ in range(k1 // pool_size):
            print(_)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(pool_size))

    os.environ['PGOPTIONS'] = f'-c statement_timeout={timeout}'
    conn = psycopg2.connect(
        host="localhost",
        database=database_name,
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")
    scan_hint_id_list = []
    start_time = time.time()
    for i in range(20):
        for j in range(22):
        # for j in range(6, 7):
            id = j + 1
            path = "../../../data/queries_for_plan/" + str(id) + ".sql.template"
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
                logits = scan_reg.predict(plan)[0]  # shape: (35,)
                # print("shape(logits):", logits.shape)
                
                # todo: scan_rows
                table_stats = {}
                for t in table:
                    table_stats[t] = {}
                    cur.execute(f"/*+ ColumnarScan({t}) */ EXPLAIN (FORMAT JSON) SELECT * FROM {t}")
                    plan = cur.fetchone()[0][0]

                    def extract_main_rows(node, table_stats):
                        table_stats[t]['main_rows'] = node['Plan Rows']
                        if node['Node Type'] == 'Append':
                            assert node['Plans'][1]['Node Type'] == 'Seq Scan'
                            table_stats[t]['delta_rows'] = node['Plans'][1]['Plan Rows']
                        else:                            
                            table_stats[t]['delta_rows'] = 0

                    extract_main_rows(plan['Plan'], table_stats)
                
                theta = {t: 0 for t in table}
                adjusted_logits = logits.copy()
            
                for hint_id in range(32):
                    total_delta_cost = 0.0
                    
                    for table_name in table:
                        
                        ## todo: w_T
                        # w_T = scan_rows.get(table_name, 0) / total_scan_rows
                        w_T = 1.0
                        
                        delta_rows = table_stats[table_name]['delta_rows']
                        main_rows = table_stats[table_name]['main_rows']
                        if delta_rows + main_rows > 0:
                            delta_ratio = delta_rows / (delta_rows + main_rows)
                        else:
                            delta_ratio = 0.0
                        m_T = np.clip(delta_ratio, 0, 0.9)
                        
                        table_idx = table.index(table_name)
                        use_columnar = (hint_id >> table_idx) & 1
                        
                        C_delta_T = theta[table_name] * m_T * use_columnar
                        total_delta_cost += w_T * C_delta_T
                    
                    adjusted_logits[hint_id] = logits[hint_id] - total_delta_cost
                
                scan_hint_id = int(np.argmax(adjusted_logits))
                
                end = time.time()
                # inference_time += end - start

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
                # join_hint = ""
                # scan_hint = ""
                end = time.time()
                inference_time += end - start

                hint = "/*+\n" + scan_hint_set[scan_hint_id] + join_hint + "*/\n"
                sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + sql

                print("execute stream, sql_id:", i, id)
                # print(hint)
                # cur.execute("set google_columnar_engine.enable_columnar_scan=on")
                # cur.execute("set google_columnar_engine.enable_vectorized_join=on")

                param = None if len(param) == 0 else param[i * 5]
                plan, tp_results = run_ap_with_tp(hint_sql, param, k2, htapcontroller, timeout=100000)
                if plan is None:
                    failed.append(id)
                    continue
                latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']
                tp_count += len(tp_results)

    print("tp_count:", tp_count)
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

    set_refresh_threshold(50)

    print(sql_latency)
    print(scan_sql_latency)
    print(join_sql_latency)
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
    # print(scan_hint_id_list)
    