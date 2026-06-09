import psycopg2 # type: ignore
import os
import time
import datetime
from multiprocessing import Pool, Manager
import sys
sys.path.append("../")
from utils.htap_query import HTAPController
from utils.restore import restore_hybench
from utils.utils import run_ap_with_tp

sql_queries = []
sql_dir = "../sql"
for ap_id in range(1, 14):
    ap_queries = []
    ap_prefix = f"{ap_id:02d}"
    sql_files = []
    for filename in sorted(os.listdir(sql_dir)):
        if filename.startswith(ap_prefix) and filename.endswith(".sql"):
            sql_files.append(filename)
    
    for filename in sorted(sql_files):
        sql_path = os.path.join(sql_dir, filename)
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_content = f.read().strip()
            ap_queries.append(sql_content)
    
    sql_queries.append(ap_queries)

table = ["transfer", "loantrans", "customer", "checking", "loanapps"]
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
timeout = [5000] * 260

output_base_dir = os.path.expanduser("~/Optimus/data/graph_data_hybench")
output_base_dir_exp = os.path.expanduser("~/Optimus/data/graph_data_hybench_exp")
os.makedirs(output_base_dir, exist_ok=True)
os.makedirs(output_base_dir_exp, exist_ok=True)

for hint_id in range(0, len(scan_hint_set)):
# for hint_id in range(32, 33):
    restore_hybench()
    hint = scan_hint_set[hint_id]
    hint_dir = os.path.join(output_base_dir, str(hint_id))
    hint_dir_exp = os.path.join(output_base_dir_exp, str(hint_id))
    os.makedirs(hint_dir, exist_ok=True)
    os.makedirs(hint_dir_exp, exist_ok=True)
    data_idx = 0
    data_idx_exp = 0
    htapcontroller = HTAPController("1x")
    with Pool(100) as p1:
        for _ in range(k1 // 100):
            print(_)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))

    with Pool(k2 + 1) as p2:
        manager = Manager()
        for i in range(200):
            if i % 5 == 0:
                continue
            
            for id in range(1, 14):
                ap_queries = sql_queries[id - 1]
                sql = ap_queries[i]
                
                if i % 10 == 1:
                    hint_sql = "/*+\n" + hint + "*/\n" + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                else:
                    hint_sql = "/*+\n" + hint + "*/\n" + "EXPLAIN (FORMAT JSON)\n" + sql
                
                print("execute stream, hint_id, sql_id:", i, hint_id, id)

                prev = hint_id
                plan = None
                if hint_id < 32:
                    for table_name in table:
                        if table_name not in hint_sql:
                            idx = table.index(table_name)
                            prev = prev & (~(1 << idx))
                if prev == hint_id:
                    plan, tp_results = run_ap_with_tp(hint_sql, None, k2, htapcontroller, timeout=timeout[i // 10 * 13 + id - 1], tp_explain=True, tp_analyze=(i % 10 == 1), pool=p2, manager=manager)
                    if i % 10 == 1:
                        data_file = os.path.join(hint_dir, f"{data_idx}.txt")
                        data_idx += 1
                    else:
                        data_file = os.path.join(hint_dir_exp, f"{data_idx_exp}.txt")
                        data_idx_exp += 1
                    with open(data_file, "w") as file:
                        file.write(str(plan) + '\n')
                        for _ in range(min(20, len(tp_results))):
                            file.write(str(tp_results[_]) + '\n')
                    if i % 10 == 1 and plan is not None:
                        timeout[i // 10 * 13 + id - 1] = min(timeout[i // 10 * 13 + id - 1], plan['Execution Time'])
                        timeout[i // 10 * 13 + id - 1] = max(timeout[i // 10 * 13 + id - 1], 300)
                else:
                    # tp2 = p2.map_async(htapcontroller.oltp_worker, range(k2))

                    plan = None
                    if i % 10 == 1:
                        data_file = os.path.join(hint_dir, f"{data_idx}.txt")
                        data_idx += 1
                    else:
                        data_file = os.path.join(hint_dir_exp, f"{data_idx_exp}.txt")
                        data_idx_exp += 1
                    with open(data_file, "w") as file:
                        file.write(str(plan) + '\n')

print(time.time() - start_time)