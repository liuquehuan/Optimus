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
import psutil # type: ignore
sys.path.append("../../..")
from model import train_and_save_model
from utils.htap_query_hybench import HTAPController
from utils.restore import restore_hybench, restore_hybench_10x
from utils.utils import run_ap_with_tp


def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list


def extract_scan_rows_from_plan(plan_node, table_names):
    """
    从执行计划中提取每个表的扫描行数
    返回: dict {table_name: scan_rows}
    """
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
    """
    获取当前系统的资源竞争信息向量 R(t) = [r_cpu, r_mem, r_io]
    返回值范围为 [0, 1]，表示资源利用率
    """
    # CPU利用率 (0-1)
    r_cpu = psutil.cpu_percent(interval=0.1) / 100.0
    
    # 内存利用率 (0-1)
    r_mem = psutil.virtual_memory().percent / 100.0
    
    # I/O利用率 (0-1)
    # 使用磁盘I/O计数来估算I/O压力
    disk_io = psutil.disk_io_counters()
    if hasattr(get_contention_vector, 'prev_io'):
        io_diff = (disk_io.read_bytes + disk_io.write_bytes - 
                   get_contention_vector.prev_io)
        # 归一化: 假设100MB/s为满负载
        r_io = min(io_diff / (100 * 1024 * 1024 * 0.1), 1.0)
    else:
        r_io = 0.0
    get_contention_vector.prev_io = disk_io.read_bytes + disk_io.write_bytes
    
    return np.array([r_cpu, r_mem, r_io])


def cost_split(plans, n, train_pos, num_stream):
    X_cost, y_cost, X_latency, y_latency = [], [], [], []
    failed_count = 0

    for i in range(num_stream):
        start = i * 13 * n
        for id in range(13):
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
                cur_id += 13
            
            if min(cost) == 6000000000000:
                failed_count += 1
            else:
                if 'Execution Time' in plans[start]:
                    y_latency.append(cost.index(min(cost)))
                    if train_pos == -1:
                        X_latency.append(plans[start + id + 13 * cost.index(min(cost))])
                    else:
                        X_latency.append(plans[start + id + 13 * train_pos])
                else:
                    y_cost.append(cost.index(min(cost)))
                    if train_pos == -1:
                        X_cost.append(plans[start + id + 13 * cost.index(min(cost))])
                    else:
                        X_cost.append(plans[start + id + 13 * train_pos])

    print("failed_count:", failed_count)
    # assert failed_count == 0
    if (train_pos == -1):
        print(X_latency[10], X_latency[13 + 10], X_latency[13 * 2 + 10])
    return  X_cost, y_cost, X_latency, y_latency


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='运行 TCNN-- 基准测试')
    parser.add_argument('scan_model', type=str, help='扫描模型文件路径')
    parser.add_argument('join_model', type=str, help='连接模型文件路径')
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x'],
                        help='选择数据库规模: 1x (默认) 或 10x')
    args = parser.parse_args()

    # 根据参数选择数据库和恢复函数
    if args.db == '10x':
        restore_hybench_10x()
        database_name = "hybench_sf10"
        sql_dir = "../../sql_10x"
        k1 = 500000
        htapcontroller_param = "10x"
        timeout = 100000
    else:
        restore_hybench()
        database_name = "hybench_sf1"
        sql_dir = "../../sql"
        k1 = 50000
        htapcontroller_param = "1x"
        timeout = 20000
    
    # 从sql目录读取SQL查询文件
    sql_queries = []
    for ap_id in range(1, 14):  # AP-1 到 AP-13
        ap_queries = []
        # 获取该AP的所有SQL文件（按字母后缀排序）
        ap_prefix = f"{ap_id:02d}"
        sql_files = []
        for filename in sorted(os.listdir(sql_dir)):
            if filename.startswith(ap_prefix) and filename.endswith(".sql"):
                sql_files.append(filename)
        
        # 读取每个SQL文件的内容
        for filename in sorted(sql_files):
            sql_path = os.path.join(sql_dir, filename)
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read().strip()
                ap_queries.append(sql_content)
        
        sql_queries.append(ap_queries)

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    # 根据数据库规模选择数据文件路径
    if args.db == '10x':
        scan_plan_path = "../../../data/cost_col_hint_plan_hybench_10x.txt"
        join_plan_path = "../../../data/scan_aware_cost_join_type_hint_plan_hybench_10x.txt"
    else:
        scan_plan_path = "../../../data/cost_col_hint_plan_hybench.txt"
        join_plan_path = "../../../data/cost_join_type_hint_plan_hybench.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 160)
    scan_reg = train_and_save_model(args.scan_model, X_cost, y_cost)
    scan_reg = train_and_save_model(args.scan_model, X_latency, y_latency, reg=scan_reg)
    del scan_plan

    X_cost, y_cost, X_latency, y_latency = cost_split(join_plan, 7, -1, 160)
    join_reg = train_and_save_model(args.join_model, X_cost, y_cost, node_level=True)
    join_reg = train_and_save_model(args.join_model, X_latency, y_latency, reg=join_reg, node_level=True)
    del join_plan


    # print("Model saved, attempting load...")
    # scan_reg = model.BaoRegression()
    # scan_reg.load(args.scan_model)

    # join_reg = model.BaoRegression()
    # join_reg.load(args.join_model)

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

    latency, scan_latency, join_latency = [], [], []
    sql_latency, scan_sql_latency, join_sql_latency = np.zeros(13), np.zeros(13), np.zeros(13)
    failed, scan_failed, join_failed = [], [], []
    explain_time, inference_time = 0, 0
    k2 = 30
    # set_refresh_threshold(50)
    htapcontroller = HTAPController(htapcontroller_param, dbname=database_name)
    print("start tp")
    with Pool(100) as p1:
        for _ in range(k1 // 100):
            # print(_)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))

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
    tp_count = 0
    start_time = time.time()
    # 每分钟统计变量
    last_report_time = time.time()
    minute_tp_count = 0  # 最近一分钟的 tp_results 数量之和
    minute_query_count = 0  # 最近一分钟处理的查询数量
    minute_statistics = []  # 存储每分钟统计信息的列表
    
    with Pool(k2 + 1) as p2:
        for i in range(40):
            for j in range(13):  # AP-1 到 AP-13
            # for j in range(6, 7):
                id = j + 1
                # 从sql_queries获取SQL（使用第 i*5 个查询）
                ap_queries = sql_queries[id - 1]
                query_idx = i * 5

                sql = ap_queries[query_idx]
                explain_sql = "EXPLAIN (FORMAT JSON)\n" + sql

                start = time.time()
                cur.execute(explain_sql)
                plan = cur.fetchone()[0][0]
                end = time.time()
                explain_time += end - start

                start = time.time()
                logits = scan_reg.predict(plan)[0]  # shape: (35,)
                # print("shape(logits):", logits.shape)
                
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
                
                # ====== 获取当前系统资源竞争信息 R(t) ======
                R_t = get_contention_vector()  # [r_cpu, r_mem, r_io]
                
                # ====== 定义路径特定的资源消耗向量 ======
                # 根据论文中的示例值
                s_row = np.array([0.6, 1.0, 0.5])  # Row scan的资源消耗 [cpu, mem, io]
                s_col = np.array([0.4, 0.6, 0.2])  # Column scan的资源消耗 [cpu, mem, io]
                
                # ====== 可学习参数向量 η ======
                # 初始化为均匀权重，后续可以通过训练学习
                eta = np.array([0, 0, 0])
                
                theta = {t: 0 for t in table}
                adjusted_logits = logits.copy()
            
                for hint_id in range(32):
                    total_delta_cost = 0.0
                    
                    # ====== 计算聚合资源消耗 S(q,h) ======
                    S_qh = np.zeros(3)  # [cpu, mem, io]
                    
                    for table_name in table:
                        
                        # ====== 计算 w_T: 查询在该表上扫描的行数占总扫描行数的比例 ======
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
                        
                        # Delta store cost
                        C_delta_T = theta[table_name] * m_T * use_columnar
                        total_delta_cost += w_T * C_delta_T
                        
                        # ====== 累加资源消耗: S(q,h) = Σ_T w_T * s_p(h,t) ======
                        if use_columnar:
                            S_qh += w_T * s_col
                        else:
                            S_qh += w_T * s_row
                    
                    # ====== 计算 Contention Cost: D_contention = η^T (R(T) · S(q,h)) ======
                    # 元素级乘法 R(T) · S(q,h)，然后加权求和
                    contention_cost = np.dot(eta, R_t * S_qh)
                    
                    # ====== 综合调整 logits ======
                    adjusted_logits[hint_id] = logits[hint_id] - total_delta_cost - contention_cost
                
                scan_hint_id = int(np.argmax(adjusted_logits))
                
                end = time.time()
                # inference_time += end - start

                scan_hint = "/*+\n" + scan_hint_set[scan_hint_id] + "*/\n"
                explain_sql = scan_hint + "EXPLAIN (FORMAT JSON)\n" + sql

                start = time.time()
                cur.execute(explain_sql)
                plan = cur.fetchone()[0][0]
                end = time.time()
                explain_time += end - start

                start = time.time()
                join_hint = join_reg.predict(plan)
                end = time.time()
                inference_time += end - start

                # join_hint = ""
                # scan_hint_id = 32
                hint = "/*+\n" + scan_hint_set[scan_hint_id] + join_hint + "*/\n"
                execute_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + execute_sql

                print("execute stream, sql_id:", i, id)
                # print(hint)
                # cur.execute("set google_columnar_engine.enable_columnar_scan=on")
                # cur.execute("set google_columnar_engine.enable_vectorized_join=on")

                plan, tp_results = run_ap_with_tp(hint_sql, None, k2, htapcontroller, timeout=timeout, pool=p2)
                if plan is None:
                    failed.append(id)
                    continue
                latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']
                tp_count += len(tp_results)
                
                # 更新每分钟统计
                minute_tp_count += len(tp_results)
                minute_query_count += 1
                
                # 检查是否已经过了一分钟
                current_time = time.time()
                if current_time - last_report_time >= 60:
                    elapsed_minutes = (current_time - last_report_time) / 60
                    # 保存统计信息到列表
                    minute_statistics.append({
                        'time': current_time,
                        'elapsed_minutes': elapsed_minutes,
                        'tp_count': minute_tp_count,
                        'query_count': minute_query_count
                    })
                    # 重置计数器
                    minute_tp_count = 0
                    minute_query_count = 0
                    last_report_time = current_time
    
    # 程序结束，保存最后一段时间的统计（如果有剩余）
    end_time = time.time()
    if minute_tp_count > 0 or minute_query_count > 0:
        elapsed_minutes = (end_time - last_report_time) / 60
        minute_statistics.append({
            'time': end_time,
            'elapsed_minutes': elapsed_minutes,
            'tp_count': minute_tp_count,
            'query_count': minute_query_count
        })
    
    # 输出列表格式统计信息
    if len(minute_statistics) > 0:
        tp_list = [s['tp_count'] for s in minute_statistics]
        query_list = [s['query_count'] for s in minute_statistics]
        print("\n【每分钟统计信息】")
        print(f"TP结果数量列表: {tp_list}")
        print(f"处理查询数量列表: {query_list}")
        
        # 保存到文件
        output_filename = f"minute_statistics_TCNN_{args.db}_{time.strftime('%Y%m%d_%H%M%S', time.localtime(start_time))}.txt"
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"TP结果数量列表: {tp_list}\n")
                f.write(f"处理查询数量列表: {query_list}\n")
            print(f"统计信息已保存到文件: {output_filename}")
        except Exception as e:
            print(f"警告: 无法保存统计信息到文件: {e}")

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

    # set_refresh_threshold(50)

    print(sql_latency)
    print(scan_sql_latency)
    print(join_sql_latency)
    print(explain_time, inference_time)
    
    print("tp count:", tp_count)
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
    