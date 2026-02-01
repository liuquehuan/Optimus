import psycopg2 # type: ignore
import os
import time
import datetime
from multiprocessing import Pool, Manager
import sys
sys.path.append("../")
from utils.htap_query import HTAPController
from utils.restore import restore
from utils.utils import run_ap_with_tp

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

k1, k2 = 50000, 20
start_time = time.time()
timeout = [10000] * 220

output_base_dir = os.path.expanduser("~/Optimus/data/graph_data")
output_base_dir_exp = os.path.expanduser("~/Optimus/data/graph_data_exp")
os.makedirs(output_base_dir, exist_ok=True)
os.makedirs(output_base_dir_exp, exist_ok=True)

# for hint_id in range(0, len(scan_hint_set)):
for hint_id in range(32, 33):
    restore()
    hint = scan_hint_set[hint_id]
    # 为每个 hint_id 在两个目录下创建单独的文件夹
    hint_dir = os.path.join(output_base_dir, str(hint_id))
    hint_dir_exp = os.path.join(output_base_dir_exp, str(hint_id))
    os.makedirs(hint_dir, exist_ok=True)
    os.makedirs(hint_dir_exp, exist_ok=True)
    data_idx = 0  # graph_data 数据文件计数器
    data_idx_exp = 0  # graph_data_exp 数据文件计数器
    htapcontroller = HTAPController()
    with Pool(100) as p1:
        for _ in range(k1 // 100):
            print(_)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))

    with Pool(k2 + 1) as p2:  # 复用这个 Pool，k2+1 因为 run_ap_with_tp 需要 TP_WORKERS+1 个进程
        manager = Manager()
        for i in range(100):
            if i % 5 == 0:
                continue
            
            for id in range(1, 23):
                path = "../../data/queries_for_plan/" + str(id) + ".sql.template"
                sql = None
                hint_sql = None
                with open(path, "r") as file:
                    sql = file.read()
                    if i % 10 == 1:
                        hint_sql = "/*+\n" + hint + "*/\n" + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                    else:
                        hint_sql = "/*+\n" + hint + "*/\n" + "EXPLAIN (FORMAT JSON)\n" + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i]
                print("execute stream, hint_id, sql_id:", i, hint_id, id)

                prev = hint_id
                plan = None
                if hint_id < 32:
                    for table_name in table:
                        if table_name not in hint_sql:
                            idx = table.index(table_name)
                            prev = prev & (~(1 << idx))
                if prev == hint_id:
                    plan, tp_results = run_ap_with_tp(hint_sql, param, k2, htapcontroller, timeout=timeout[i // 10 * 22 + id - 1], tp_explain=True, tp_analyze=(i % 10 == 1), pool=p2, manager=manager)
                    # 根据 i % 10 == 1 选择存放目录
                    if i % 10 == 1:
                        data_file = os.path.join(hint_dir, f"{data_idx}.txt")
                        data_idx += 1
                    else:
                        data_file = os.path.join(hint_dir_exp, f"{data_idx_exp}.txt")
                        data_idx_exp += 1
                    with open(data_file, "w") as file:
                        file.write(str(plan) + '\n')
                        # 写入前 20 个结果（如果不足 20 个则写入全部）
                        for _ in range(min(20, len(tp_results))):
                            file.write(str(tp_results[_]) + '\n')
                    if i % 10 == 1 and plan is not None:
                        timeout[i // 10 * 22 + id - 1] = min(timeout[i // 10 * 22 + id - 1], plan['Execution Time'])
                        timeout[i // 10 * 22 + id - 1] = max(timeout[i // 10 * 22 + id - 1], 300)
                else:
                    # tp2 = p2.map_async(htapcontroller.oltp_worker, range(k2))
                    # tp2.wait()  # 等待这批任务完成

                    plan = None
                    # 存储空 plan 数据，根据 i % 10 == 1 选择存放目录
                    if i % 10 == 1:
                        data_file = os.path.join(hint_dir, f"{data_idx}.txt")
                        data_idx += 1
                    else:
                        data_file = os.path.join(hint_dir_exp, f"{data_idx_exp}.txt")
                        data_idx_exp += 1
                    with open(data_file, "w") as file:
                        file.write(str(plan) + '\n')

print(time.time() - start_time)