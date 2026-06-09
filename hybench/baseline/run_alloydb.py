import numpy as np
import psycopg2
import os
import sys
import argparse
sys.path.append("../..")
from utils.htap_query_hybench import HTAPController
from multiprocessing import Pool
import datetime
import time
from utils.utils import run_ap_with_tp
from utils.restore import restore_hybench, restore_hybench_10x, restore_hybench_100x

def get_scan_hint(scan_mode):
    if scan_mode == 'row':
        hint = """/*+
Set(google_columnar_engine.enable_columnar_scan off)
Set(google_columnar_engine.enable_vectorized_join off)
Set(enable_seqscan on)
Set(enable_indexscan on)
Set(enable_indexonlyscan on)
*/
"""
    elif scan_mode == 'column':
        hint = """/*+
Set(google_columnar_engine.enable_columnar_scan on)
Set(google_columnar_engine.enable_vectorized_join on)
Set(enable_seqscan off)
Set(enable_indexscan off)
Set(enable_indexonlyscan off)
*/
"""
    else:  # mixed
        hint = ""
    return hint


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the AlloyDB benchmark')
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x', '100x'],
                        help='Database scale: 1x (default), 10x, or 100x')
    parser.add_argument('--scan-mode', type=str, default='mixed', 
                        choices=['row', 'column', 'mixed'],
                        help='Scan mode: row, column, or mixed (default)')
    args = parser.parse_args()
    
    if args.db == '100x':
        restore_hybench_100x()
        database_name = "hybench_sf100"
        sql_dir = "../sql_100x"
        k1 = 5000000
        htapcontroller_param = "100x"
    elif args.db == '10x':
        restore_hybench_10x()
        database_name = "hybench_sf10"
        sql_dir = "../sql_10x"
        k1 = 500000
        htapcontroller_param = "10x"
    else:
        restore_hybench()
        database_name = "hybench_sf1"
        sql_dir = "../sql"
        k1 = 50000
        htapcontroller_param = "1x"
    
    sql_queries = []
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
    
    # set_refresh_threshold(100)

    conn = psycopg2.connect(
        host="localhost",
        database=database_name,
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")

    pg_latency = []
    sql_latency = np.zeros(13)
    query_count_by_type = np.zeros(13)
    k2 = 30

    htapcontroller = HTAPController(htapcontroller_param, dbname=database_name)

    with Pool(100) as p1:
        for i in range(k1 // 100):
            print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))
    
    # if args.db == '10x':
    time.sleep(5)
    
    failed = []
    start_time = time.time()
    tp_count = 0
    last_report_time = time.time()
    minute_tp_count = 0
    minute_query_count = 0
    minute_statistics = []
    
    with Pool(k2 + 1) as p2:
        for i in range(40):
            for j in range(13):
                id = j + 1
                ap_queries = sql_queries[id - 1]
                query_idx = i * 5

                sql = ap_queries[query_idx]
                scan_hint = get_scan_hint(args.scan_mode)
                exp_sql = scan_hint + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                print("execute stream, sql_id:", i, id)

                plan, tp_results = run_ap_with_tp(exp_sql, None, k2, htapcontroller, pool=p2)
                if plan is None:
                    failed.append(id)
                    continue
                pg_latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']
                query_count_by_type[j] += 1
                tp_count += len(tp_results)
                
                minute_tp_count += len(tp_results)
                minute_query_count += 1
                
                current_time = time.time()
                if current_time - last_report_time >= 60:
                    elapsed_minutes = (current_time - last_report_time) / 60
                    minute_statistics.append({
                        'time': current_time,
                        'elapsed_minutes': elapsed_minutes,
                        'tp_count': minute_tp_count,
                        'query_count': minute_query_count
                    })
                    minute_tp_count = 0
                    minute_query_count = 0
                    last_report_time = current_time
    
    end_time = time.time()
    if minute_tp_count > 0 or minute_query_count > 0:
        elapsed_minutes = (end_time - last_report_time) / 60
        minute_statistics.append({
            'time': end_time,
            'elapsed_minutes': elapsed_minutes,
            'tp_count': minute_tp_count,
            'query_count': minute_query_count
        })
    
    # if len(minute_statistics) > 0:
    #     tp_list = [s['tp_count'] for s in minute_statistics]
    #     query_list = [s['query_count'] for s in minute_statistics]
        
    #     output_filename = f"minute_statistics_{args.db}_{args.scan_mode}_{time.strftime('%Y%m%d_%H%M%S', time.localtime(start_time))}.txt"
    #     try:
    #         with open(output_filename, 'w', encoding='utf-8') as f:
    #     except Exception as e:
    
    total_time = time.time() - start_time
    cur.execute("show google_columnar_engine.enable_vectorized_join")
    vectorized_join = cur.fetchone()
    cur.execute("show google_columnar_engine.refresh_threshold_percentage")
    refresh_threshold = cur.fetchone()
    
    pg_latency = np.sort(np.array(pg_latency))
    sum_latency = np.sum(pg_latency)
    median_latency = np.median(pg_latency)
    idx95, idx99, idx995 = int(max(0, len(pg_latency) * 0.95 - 1)), int(max(0, len(pg_latency) * 0.99 - 1)), int(max(0, len(pg_latency) * 0.995 - 1))
    
    print("failed:", failed)
    print(total_time)
    print(vectorized_join)
    print(refresh_threshold)
    print("k1:", k1, "k2:", k2)
    print("tp count:", tp_count)
    print(sql_latency)
    print("sum latency:", sum_latency)
    print("median latency:", median_latency)
    print("95 latency:", pg_latency[idx95])
    print("99 latency:", pg_latency[idx99])
    print("995 latency:", pg_latency[idx995])
    
    output_filename = f"results_{args.db}_{args.scan_mode}_{time.strftime('%Y%m%d_%H%M%S', time.localtime(start_time))}.txt"
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(f"failed: {failed}\n")
            f.write(f"total_time: {total_time}\n")
            f.write(f"enable_vectorized_join: {vectorized_join}\n")
            f.write(f"refresh_threshold_percentage: {refresh_threshold}\n")
            f.write(f"k1: {k1}, k2: {k2}\n")
            f.write(f"tp count: {tp_count}\n")
            f.write(f"sql_latency: {sql_latency}\n")
            f.write(f"sum latency: {sum_latency}\n")
            f.write(f"median latency: {median_latency}\n")
            f.write(f"95 latency: {pg_latency[idx95]}\n")
            f.write(f"99 latency: {pg_latency[idx99]}\n")
            f.write(f"995 latency: {pg_latency[idx995]}\n")
        print(f"Results saved to file: {output_filename}")
    except Exception as e:
        print(f"Warning: failed to save results to file: {e}")

    # set_refresh_threshold(50)
    cur.close()
    conn.close()
