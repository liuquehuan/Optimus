import xgboost as xgb # type: ignore
import numpy as np
import time
import psycopg2
import os
import datetime
from sentence_transformers import SentenceTransformer # type: ignore
from sklearn.pipeline import Pipeline
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import copy
from htap_query import HTAPController
from multiprocessing import Pool
from restore import restore
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('pca', PCA())
])
model = SentenceTransformer("../sentence-transformers/all-MiniLM-L6-v2")
# os.environ['PGOPTIONS'] = '-c statement_timeout=20000'
os.environ['TOKENIZERS_PARALLELISM'] = "True"

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


def generate_embeddings(model):
    sqls = []
    for i in range(0, 100):
        for id in range(1, 23):
            path = "../data/queries_for_plan/" + str(id) + ".sql.template"
            with open(path, "r") as file:
                sql = file.read()
                sql = sql.replace("\n", " ")
                param = params[id - 1]

                if len(param) != 0:
                    sql = sql % param[i]
                sqls.append(sql)
    start = time.time()
    embeddings = model.encode(sqls)
    end = time.time()
    print(embeddings.shape)
    print((end - start) / 5)
    return embeddings


def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list


def split(embeddings, plans, n):
    X_train_cost, y_train_cost, X_train_lat, y_train_lat, X_test = [], [], [], [], []

    for i in range(100):

        if i % 5 == 0:
            for id in range(22):
                X_test.append(embeddings[22 * i + id])

        else:
            start = (i - (i // 5 + 1)) * 22 * n
            for id in range(22):
                cur_id = start + id
                if i % 10 == 1:
                    X_train_lat.append(embeddings[22 * i + id])
                else:
                    X_train_cost.append(embeddings[22 * i + id])
                cost = []
                
                for _ in range(n):
                    if i % 10 == 1:
                        y = plans[cur_id]['Execution Time']
                        if y is not None:
                            cost.append(y)
                        else:
                            cost.append(6000)

                    else:
                        y = plans[cur_id]['Plan']['Total Cost']
                        cost.append(y)

                    cur_id += 22
                if i % 10 == 1:
                    y_train_lat.append(cost.index(min(cost)))
                else:
                    y_train_cost.append(cost.index(min(cost)))

    return  X_train_cost, y_train_cost, X_train_lat, y_train_lat, X_test


def train(X_train_lat, y_train_lat, X_train_cost, y_train_cost, n):
        
    print(len(X_train_cost), len(y_train_cost))
    dtrain_lat = xgb.DMatrix(X_train_lat, y_train_lat)
    dtrain_cost = xgb.DMatrix(X_train_cost, y_train_cost)
    params = {
            'objective': 'multi:softmax',
            'num_class': n,
            'eta': 0.1,
            'max_depth': 6,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'verbosity': 1,
            'lambda': 1,
            'alpha': 0
            }

    start = time.time()
    model = xgb.train(params, dtrain_cost, num_boost_round=100)
    model = xgb.train(params, dtrain_lat, num_boost_round=100, xgb_model=model)
    end = time.time()
    print("training:", end - start)
    return model


def test(model, X):

    dtest = xgb.DMatrix(X)
    start = time.time()
    pred = model.predict(dtest)
    end = time.time()
    print("inference:", end - start)
    return pred


if __name__ == "__main__":

    restore()
    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")
    col_plan_path = "../data/cost_data.txt"
    embedding, col_plan = generate_embeddings(model), load_plans(col_plan_path)

    X_train_cost, y_train_cost, X_train_lat, y_train_lat, X_test = split(embedding, col_plan, 35)
    X_train = copy.deepcopy(X_train_cost)
    X_train.extend(X_train_lat)
    X_train = pipeline.fit_transform(X_train)[:,:120]
    start = time.time()
    X_test = pipeline.transform(X_test)[:,:120]
    end = time.time()
    print(end - start)
    X_train_cost = pipeline.transform(X_train_cost)[:,:120]
    X_train_lat = pipeline.transform(X_train_lat)[:,:120]

    col_model = train(X_train_lat, y_train_lat, X_train_cost, y_train_cost, 35)
    col_pred = test(col_model, X_test)
    col_pred = [int(x) for x in col_pred]

    table = ["order_line", "stock", "customer", "orders", "item"]
    col_hint_set = []
    for i in range(0, 32):
        hint = ""

        for j in range(0, 5):
            k = i >> j & 1
            if k == 1:
                hint += "ColumnarScan(" + table[j] +")\n"
            else:
                hint += "NoColumnarScan(" + table[j] +")\n"
        
        col_hint_set.append(hint)
    col_hint_set.append("")
    col_hint_set.append('''
                        Set(google_columnar_engine.enable_columnar_scan on)\n
                        Set(google_columnar_engine.enable_vectorized_join on)\n
                        Set(enable_indexscan off)\n
                        Set(enable_seqscan off)\n
                        Set(enable_indexonlyscan off)\n
                    ''')
    col_hint_set.append('''
                        Set(google_columnar_engine.enable_columnar_scan off)\n
                        Set(google_columnar_engine.enable_vectorized_join off)\n
                        Set(enable_indexscan on)\n
                        Set(enable_seqscan on)\n
                        Set(enable_indexonlyscan on)\n
                    ''')

    col_latency = []
    col_sql_latency = np.zeros(22)
    col_failed = []
    k = 10

    for i in range(20):
        for j in range(22):
            col_hint_id = col_pred[i * 22 + j]
            hint = "/*+\n" + col_hint_set[col_hint_id] + "*/\n"
            id = j + 1
            path = "../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = hint + exp_sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]
                htapcontroller = HTAPController()

                print("execute stream, sql_id:", i, id)
                print(hint)

                p = Pool(k + 1)
                tp = p.map_async(htapcontroller.oltp_worker, range(k))
                ap = p.starmap_async(htapcontroller.olap_worker, [(hint_sql, param, 20000)])
                p.close()
                p.join()
                plan = ap.get()[0]
                if plan is None:
                    col_failed.append(id)
                    continue
                col_latency.append(plan['Execution Time'])
                col_sql_latency[j] += plan['Execution Time']


        cur.execute("show google_columnar_engine.enable_vectorized_join")
        print(cur.fetchone())

    print(col_failed)
    print(col_sql_latency)

    col_latency = np.sort(np.array(col_latency))
    print("sum latency:", np.sum(col_latency))
    print("median latency:", np.median(col_latency))
    idx75, idx95 = int(max(0, len(col_latency) * 0.75 - 1)), int(max(0, len(col_latency) * 0.95 - 1))
    print("75 latency:", col_latency[idx75])
    print("95 latency:", col_latency[idx95])

cur.close()
conn.close()
