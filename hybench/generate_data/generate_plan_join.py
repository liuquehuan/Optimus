import psycopg2
import datetime
import os
import time
os.environ['PGOPTIONS'] = '-c statement_timeout=3000'

start = time.time()

# 从 ../sql 目录读取SQL查询文件
sql_queries = []
sql_dir = "../sql"
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


operator_type = ["enable_hashjoin", "enable_mergejoin", "enable_nestloop"]
hint_set = []
for i in range(1, 8):
    hint = "/*+\n"

    for j in range(0, 3):
        k = i >> j & 1
        if k == 1:
            hint += "Set(" + operator_type[j] +" on)\n"
        else:
            hint += "Set(" + operator_type[j] +" off)\n"
    
    hint += "*/\n"
    hint_set.append(hint)
print(len(hint_set)) ## 7

for i in range(0, 200):
    if i % 5 == 0:
        continue
    
    stream, hint_id = [], 0
    for hint in hint_set:
        hint_plan = []
        for id in range(1, 14):  # AP-1 到 AP-13
            conn = psycopg2.connect(
                host="localhost",
                database="hybench_sf1",
                user="postgres",
                port=5555,
                password="postgres"
            )
            cur = conn.cursor()
            cur.execute("load \'pg_hint_plan\'")
            cur.execute("set google_columnar_engine.enable_vectorized_join=on")
            cur.execute("set google_columnar_engine.enable_columnar_scan=on")

            # 从sql_queries获取SQL（第i个查询）
            ap_queries = sql_queries[id - 1]
            
            sql = ap_queries[i]
            if i % 10 == 1:
                hint_sql = hint + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
            else:
                hint_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql

            print("execute stream, hint_id, sql_id:", i, hint_id, id)
            plan = None

            try:
                cur.execute(hint_sql)
                plan = cur.fetchone()[0][0]
            except psycopg2.extensions.QueryCanceledError as e:
                conn.rollback()
                hint_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql
                cur.execute(hint_sql)
                plan = cur.fetchone()[0][0]
                plan['Execution Time'] = None

            hint_plan.append(plan)
            
            cur.close()
            conn.close()
        stream.append(hint_plan)
        hint_id += 1
        # cur.execute("show google_columnar_engine.enable_vectorized_join")
        # print(cur.fetchone())
    
    
    with open("../../data/cost_join_type_hint_plan_hybench.txt", "a+") as file:
        for hint in stream:
            for plan in hint:
                file.write(str(plan) + '\n')

end = time.time()
print("labelling time:", end - start)
