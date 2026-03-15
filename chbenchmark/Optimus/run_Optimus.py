import psycopg2 # type: ignore
import os
import torch # type: ignore
import numpy as np # type: ignore
import time
import datetime
from multiprocessing import Pool
import sys
import argparse
import psutil # type: ignore
sys.path.append("../../")
from HGT.train import Optimus
from utils.htap_query import HTAPController
from utils.restore import restore, restore_10x
from utils.utils import run_ap_with_tp

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


def extract_scan_rows_from_plan(plan_node, table_names):
    scan_rows = {t: 0 for t in table_names}
    
    def traverse(node):
        node_type = node.get('Node Type', '')
        
        if node_type in ['Seq Scan', 'Index Scan', 'Index Only Scan', 'Bitmap Heap Scan', 
                          'Custom Scan']:
            relation_name = node.get('Relation Name', '')
            if relation_name in scan_rows:
                scan_rows[relation_name] += node.get('Plan Rows', 0)

        if 'Plans' in node:
            for child in node['Plans']:
                traverse(child)
    
    traverse(plan_node)
    return scan_rows


def get_contention_vector():
    r_cpu = psutil.cpu_percent(interval=0.1) / 100.0
    
    r_mem = psutil.virtual_memory().percent / 100.0
    
    disk_io = psutil.disk_io_counters()
    if hasattr(get_contention_vector, 'prev_io'):
        io_diff = (disk_io.read_bytes + disk_io.write_bytes - 
                   get_contention_vector.prev_io)
        r_io = min(io_diff / (100 * 1024 * 1024 * 0.1), 1.0)
    else:
        r_io = 0.0
    get_contention_vector.prev_io = disk_io.read_bytes + disk_io.write_bytes
    
    return np.array([r_cpu, r_mem, r_io])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('scan_model', type=str)
    parser.add_argument('join_model', type=str)
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x'])
    args = parser.parse_args()

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)

    scan_reg = Optimus()
    scan_reg.load(sys.argv[1])

    join_reg = Optimus()
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
    sql_latency, scan_sql_latency, join_sql_latency = np.zeros(22), np.zeros(22), np.zeros(22)
    failed, scan_failed, join_failed = [], [], []
    explain_time, inference_time = 0, 0
    k2 = 20
    if args.db == '10x':
        k1 = 500000
        restore_10x()
        database_name = "htap_sf10"
        timeout = 100000
    else:
        k1 = 50000
        restore()
        database_name = "htap_sf1"
        timeout = 20000
    htapcontroller = HTAPController()
    print("start tp")
    with Pool(50) as p1:
        for _ in range(k1 // 50):
            print(_)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(50))
    time.sleep(10)

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
                logits = scan_reg.predict(plan)  # shape: (35,)
                
                query_scan_rows = extract_scan_rows_from_plan(plan['Plan'], table)
                total_scan_rows = sum(query_scan_rows.values())
                
                if total_scan_rows == 0:
                    total_scan_rows = 1.0
                
                table_stats = {}
                for t in table:
                    table_stats[t] = {}
                    cur.execute(f"/*+ ColumnarScan({t}) */ EXPLAIN (FORMAT JSON) SELECT * FROM {t}")
                    plan_t = cur.fetchone()[0][0]

                    def extract_main_rows(node, table_stats):
                        table_stats[t]['main_rows'] = node['Plan Rows']
                        if node['Node Type'] == 'Append':
                            assert node['Plans'][1]['Node Type'] == 'Seq Scan'
                            table_stats[t]['delta_rows'] = node['Plans'][1]['Plan Rows']
                        else:                            
                            table_stats[t]['delta_rows'] = 0

                    extract_main_rows(plan_t['Plan'], table_stats)
                
                R_t = get_contention_vector()  # [r_cpu, r_mem, r_io]
                
                s_row = np.array([0.6, 1.0, 0.5])
                s_col = np.array([0.4, 0.6, 0.2])
                
                eta = np.array([0, 0, 0])
                
                theta = {t: 0 for t in table}
                adjusted_logits = logits.copy()
            
                for hint_id in range(32):
                    total_delta_cost = 0.0
                    
                    S_qh = np.zeros(3)
                    
                    for table_name in table:
                        
                        w_T = query_scan_rows.get(table_name, 0) / total_scan_rows
                        
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
                        
                        if use_columnar:
                            S_qh += w_T * s_col
                        else:
                            S_qh += w_T * s_row
                    
                    contention_cost = np.dot(eta, R_t * S_qh)
                    
                    adjusted_logits[hint_id] = logits[hint_id] - total_delta_cost - contention_cost
                
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
                join_hint = join_reg.run_test_upd(plan)
                end = time.time()
                inference_time += end - start

                hint = "/*+\n" + scan_hint_set[scan_hint_id] + join_hint + "*/\n"
                sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + sql

                print("execute stream, sql_id:", i, id)
                param = None if len(param) == 0 else param[i * 5]
                plan, _ = run_ap_with_tp(hint_sql, param, k2, htapcontroller, timeout=timeout)
                if plan is None:
                    failed.append(id)
                    continue
                latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']


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
    