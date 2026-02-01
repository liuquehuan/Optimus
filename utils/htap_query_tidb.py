import pymysql
import time
import random
import math
import datetime
import multiprocessing
from dateutil.parser import isoparse ## type: ignore
import os
import json
import re

# TiDB连接配置基础模板
def get_tidb_config(database_name):
    """根据数据库名称返回TiDB连接配置"""
    return {
        "host": "localhost",
        "database": database_name,
        "user": "root",
        "port": 4000,
        "password": "",
        "charset": "utf8mb4",
        "autocommit": True
    }


class Random:
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.C_255 = self.rng.randint(0, 256)
        self.C_1023 = self.rng.randint(0, 1024)
        self.C_8191 = self.rng.randint(0, 8192)

    def nurand(self, A, x, y):
        if A == 255:
            C = self.C_255
        elif A == 1023:
            C = self.C_1023
        elif A == 8191:
            C = self.C_8191

        return (((self.rng.randint(0, A + 1) | self.rng.randint(x, y + 1)) + C) % (y - x + 1)) + x

    def decision(self, frac):
        return self.rng.random() < frac

    def randint_inclusive(self, low, high):
        return self.rng.randint(low, high)

    def sample(self):
        return self.rng.random()

    def gaussian(self, mean, variance):
        return self.rng.gauss(mean, variance)

    def shuffle(self, l):
        self.rng.shuffle(l)

    def from_list(self, l, length=1):
        return self.rng.choices(l, k=length)


ALPHA_LOWER = list('abcdefghijklmnopqrstuvwxyz')

ALPHA_UPPER = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

NUM = list('0123456789')

ALPHA = ALPHA_LOWER + ALPHA_UPPER

ALPHANUM = ALPHA + NUM

ALPHA54 = ALPHA_LOWER + list('?@') + ALPHA_UPPER

ALPHANUM64 = ALPHA54 + NUM

NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']


class OLTPText:
    def __init__(self, random):
        self.random = random

    def lastname(self, num):
        part_a = NAMES[math.floor(num / 100)]
        part_b = NAMES[math.floor(num / 10) % 10]
        part_c = NAMES[num % 10]
        return part_a + part_b + part_c

    def string(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(ALPHA, length))

    def numstring(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(NUM, length))

    def alnumstring(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(ALPHANUM, length))

    def alnum64string(self, length, prefix=''):
        return prefix + ''.join(self.random.from_list(ALPHANUM64, length))

    def data(self, min_length, max_length):
        length = self.random.randint_inclusive(min_length, max_length)
        return self.alnum64string(length)

    def data_original(self, min_length, max_length):
        original = 'ORIGINAL'
        length = self.random.randint_inclusive(min_length, max_length - len(original))
        alphanum64 = self.alnum64string(length)
        l1 = self.random.randint_inclusive(0, length - len(original))
        return '{}{}{}'.format(alphanum64[0:l1], original, alphanum64[l1:length])

    def state(self):
        return self.alnumstring(2).upper()
    

DIST_PER_WARE = 10
CUST_PER_DIST = 3000
NUM_ORDERS = 3000
MAX_ITEMS = 100000
TPCH_DATE_RANGE = [isoparse('1992-01-01'), isoparse('1998-12-31')]
class TimestampGenerator:
    def __init__(self, start_date, random, scalar = 1.0):
        # use the start date as value directly so that we can easily use shared counters
        # as well by simply feeding in the right type
        self.current = start_date
        self.random = random

        orders_per_warehouse = NUM_ORDERS * DIST_PER_WARE
        date_range = TPCH_DATE_RANGE[1] - TPCH_DATE_RANGE[0]
        self.increment = (date_range / orders_per_warehouse) * scalar

    def next(self):
        # support both process-local counters as well as multiprocessing value proxies
        if isinstance(self.current, datetime.datetime):
            self.current += self.increment * self.random.gaussian(mean=1, variance=0.05)
            return self.current
        elif isinstance(self.current, multiprocessing.sharedctypes.Synchronized):
            with self.current.get_lock():
                self.current.value += self.increment.total_seconds() * self.random.gaussian(mean=1, variance=0.05)
                return datetime.datetime.fromtimestamp(self.current.value)
        else:
            raise ValueError("Unsupported datatype for TimestampGenerator")
        

class TransactionalWorker:
    def __init__(self, seed, num_warehouses, latest_timestamp, conn):
        self.conn = conn
        self.random = Random(seed)
        self.oltp_text = OLTPText(self.random)
        self.num_warehouses = num_warehouses

        # the loader only generates timestamps for the orders table, and
        # generates a timestamp stream per warehouse.
        # here we generate a tsx for any warehouse and therefore have to scale
        # for both: 10/23 and warehouse-count. the 10/23 comes from next_transaction
        # and is the ratio between calls to new_order() and timestamp_generator.next()
        timestamp_scalar = (10/23.0) / self.num_warehouses

        self.timestamp_generator = TimestampGenerator(
                latest_timestamp, self.random, timestamp_scalar
        )
        self.ok_count = 0
        self.err_count = 0
        self.new_order_count = 0

    def other_ware(self, home_ware):
        if self.num_warehouses == 1:
            return home_ware

        while True:
            tmp = self.random.randint_inclusive(1, self.num_warehouses)
            if tmp != home_ware:
                return tmp

    def execute_sql(self, sql, args, explain=False, analyze=False):
        # do not catch timeouts because we want that to stop the benchmark.
        # if we get timeouts the benchmark gets inbalanced and we eventually get
        # to a complete halt.
        if not explain:
            self.conn.cursor.execute(sql, args)
            return None
        
        if analyze:
            sql = "EXPLAIN ANALYZE FORMAT='tidb_json'\n" + sql
        else:
            sql = "EXPLAIN FORMAT='tidb_json'\n" + sql
        self.conn.cursor.execute(sql, args)
        result = self.conn.cursor.fetchall()
        if result:
            plan = json.loads(result[0][0])[0] if result[0][0] else None
            return plan
        return None
    
    def execute_sql_new_order(self, sql, args, explain=False, analyze=False):
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        # do not catch timeouts because we want that to stop the benchmark.
        # if we get timeouts the benchmark gets inbalanced and we eventually get
        # to a complete halt.
        if not explain:
            self.conn.cursor.execute(sql, args)
            result = self.conn.cursor.fetchone()
            if result and result[0] == True:
                self.conn.conn.commit()
            else:
                self.conn.conn.rollback()
            self.conn.conn.autocommit = old_autocommit
            return None
        
        if analyze:
            sql = "EXPLAIN ANALYZE FORMAT='tidb_json'\n" + sql
        else:
            sql = "EXPLAIN FORMAT='tidb_json'\n" + sql

        self.conn.cursor.execute(sql, args)
        result = self.conn.cursor.fetchall()
        if analyze:
            itemid = args[5]
            if itemid and len(itemid) > 1 and itemid[-2] == -1:
                self.conn.conn.rollback()
            else:
                self.conn.conn.commit()
        else:
            # explain 但不 analyze 时也需要结束事务
            self.conn.conn.rollback()
                
        self.conn.conn.autocommit = old_autocommit
        if result and result[0][0]:
            plan = json.loads(result[0][0])[0] if result[0][0] else None
            return plan
        return None

    def new_order(self, timestamp, explain=False, analyze=False):
        w_id = self.random.randint_inclusive(1, self.num_warehouses)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        order_line_count = self.random.randint_inclusive(5, 15)
        rbk = self.random.randint_inclusive(1, 100)
        itemid = []
        supware = []
        qty = []
        all_local = 1

        for order_line in range(1, order_line_count + 1):
            itemid.append(self.random.nurand(8191, 1, MAX_ITEMS))
            if (order_line == order_line_count - 1) and (rbk == 1):
                itemid[-1] = -1

            if self.random.randint_inclusive(1, 100) != 1:
                supware.append(w_id)
            else:
                supware.append(self.other_ware(w_id))
                all_local = 0

            qty.append(self.random.randint_inclusive(1, 10))

        # TiDB使用JSON格式传递数组参数
        import json
        itemid_json = json.dumps(itemid)
        supware_json = json.dumps(supware)
        qty_json = json.dumps(qty)
        
        sql = 'SELECT new_order(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        args = (w_id, c_id, d_id, order_line_count, all_local, itemid_json, supware_json, qty_json, timestamp)
        # rolled back or commit tsxs they both count
        self.new_order_count += 1
        return self.execute_sql_new_order(sql, args, explain, analyze)

    def payment(self, timestamp, explain=False, analyze=False):
        w_id = self.random.randint_inclusive(1, self.num_warehouses)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        h_amount = self.random.randint_inclusive(1, 5000)
        c_last = self.oltp_text.lastname(self.random.nurand(255, 0, 999))

        byname = self.random.randint_inclusive(1, 100) <= 60
        if self.random.randint_inclusive(1, 100) <= 85:
            c_w_id = w_id
            c_d_id = d_id
        else:
            c_w_id = self.other_ware(w_id)
            c_d_id = self.random.randint_inclusive(1, DIST_PER_WARE)

        # payment是存储过程，使用CALL语法
        sql = 'CALL payment(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        args = (w_id, d_id, c_d_id, c_id, c_w_id, h_amount, byname, c_last, timestamp)
        return self.execute_sql(sql, args, explain, analyze)

    def order_status(self, explain=False, analyze=False):
        w_id = self.random.randint_inclusive(1, self.num_warehouses)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        c_last = self.oltp_text.lastname(self.random.nurand(255, 0, 999))
        byname = self.random.randint_inclusive(1, 100) <= 60

        # order_status是存储过程，使用CALL语法
        sql = 'CALL order_status(%s, %s, %s, %s, %s)'
        args = (w_id, d_id, c_id, c_last, byname)
        return self.execute_sql(sql, args, explain, analyze)

    def delivery(self, timestamp, explain=False, analyze=False):
        w_id = self.random.randint_inclusive(1, self.num_warehouses)
        o_carrier_id = self.random.randint_inclusive(1, 10)

        # delivery是存储过程，使用CALL语法
        sql = 'CALL delivery(%s, %s, %s, %s)'
        args = (w_id, o_carrier_id, DIST_PER_WARE, timestamp)
        return self.execute_sql(sql, args, explain, analyze)

    def stock_level(self, explain=False, analyze=False):
        w_id = self.random.randint_inclusive(1, self.num_warehouses)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        level = self.random.randint_inclusive(10, 20)

        # stock_level是函数，使用SELECT语法
        sql = 'SELECT stock_level(%s, %s, %s)'
        args = (w_id, d_id, level)
        return self.execute_sql(sql, args, explain, analyze)

    def next_transaction(self, explain=False, analyze=False):
        timestamp_to_use = self.timestamp_generator.next()

        # WARNING: keep in sync with initialization of scalar of timestamp generator!
        trx_type = self.random.randint_inclusive(1, 23)
        if trx_type <= 10:
                return self.new_order(timestamp_to_use, explain, analyze)
        elif trx_type <= 20:
                return self.payment(timestamp_to_use, explain, analyze)
        elif trx_type <= 21:
                return self.order_status(explain, analyze)
        elif trx_type <= 22:
                return self.delivery(timestamp_to_use, explain, analyze)
        elif trx_type <= 23:
                return self.stock_level(explain, analyze)


from multiprocessing import Pool, Value
import pandas as pd
import time


class DBConn:
    def __init__(self, database_name, statement_timeout=0, max_retries=3, retry_delay=1):
        self.conn = None
        self.cursor = None
        self.database_name = database_name
        self.statement_timeout = statement_timeout

    def __enter__(self):
        config = get_tidb_config(self.database_name)
        self.conn = pymysql.connect(**config)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
        return self

    def __exit__(self, *args):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


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


def parse_execution_time(execute_info):
    """从TiDB的executeInfo中解析执行时间"""
    # 支持微秒(µs)、毫秒(ms)和秒(s)三种单位
    time_match = re.search(r'time:([0-9.]+)(µs|ms|s)', execute_info)
    if time_match:
        time_value = float(time_match.group(1))
        unit = time_match.group(2)
        if unit == 's':
            return time_value * 1000  # 秒转毫秒
        elif unit == 'ms':
            return time_value  # 毫秒直接返回
        elif unit == 'µs':
            return time_value / 1000  # 微秒转毫秒
    raise ValueError(f"Invalid execution info: {execute_info}")


class HTAPController:
    latest_timestamp = Value('d', 0) # database record timestamp

    def __init__(self, database_name="htap_sf1"):
        self.database_name = database_name
        self.num_warehouses = self._query_num_warehouses()
        self.range_delivery_date = self._query_range_delivery_date()

        # update the shared value to the actual last ingested timestamp
        self.latest_timestamp.value = self.range_delivery_date[1].timestamp()

        print(f'Database: {database_name}')
        print(f'Warehouses: {self.num_warehouses}')

    def _query_num_warehouses(self):
        with DBConn(self.database_name) as conn:
            conn.cursor.execute('SELECT count(distinct(w_id)) from warehouse')
            return conn.cursor.fetchone()[0]

    def _query_range_delivery_date(self):
        with DBConn(self.database_name) as conn:
            conn.cursor.execute('SELECT min(ol_delivery_d), max(ol_delivery_d) FROM order_line')
            result = conn.cursor.fetchone()
            return (result[0], result[1])

    def oltp_worker(self, worker_id, explain=False, analyze=False):
        # do NOT introduce timeouts for the oltp queries! this will make that
        # the workload gets inbalanaced and eventually the whole benchmark stalls
        with DBConn(self.database_name) as conn:
            oltp_worker = TransactionalWorker(worker_id, self.num_warehouses, self.latest_timestamp, conn)
            return oltp_worker.next_transaction(explain, analyze)
    
    def olap_worker(self, sql, param, timeout=0, mode='hybrid'):
        """OLAP工作负载 - 运行分析查询
        mode: 'hybrid', 'row', 或 'col'
        """
        max_retries = 7
        for attempt in range(max_retries):
            try:
                with DBConn(self.database_name, timeout) as conn:
                    cursor = conn.cursor
                    
                    # 根据模式设置TiDB配置
                    if mode == 'row':
                        # ROW模式：只使用tikv（行引擎），MPP关闭
                        cursor.execute("SET @@session.tidb_isolation_read_engines='tikv,tidb'")
                        cursor.execute("SET @@session.tidb_allow_mpp=OFF")
                        cursor.execute("SET @@session.tidb_enforce_mpp=OFF")
                    elif mode == 'col':
                        # COL模式：只使用tiflash（列引擎），MPP强制开启
                        cursor.execute("SET @@session.tidb_isolation_read_engines='tiflash,tidb'")
                        cursor.execute("SET @@session.tidb_allow_mpp=ON")
                        cursor.execute("SET @@session.tidb_enforce_mpp=ON")
                    else:  # hybrid
                        # HYBRID模式：允许使用tikv和tiflash，MPP可选
                        cursor.execute("SET @@session.tidb_isolation_read_engines='tikv,tiflash,tidb'")
                        cursor.execute("SET @@session.tidb_allow_mpp=ON")
                        cursor.execute("SET @@session.tidb_enforce_mpp=OFF")
                    
                    # 添加EXPLAIN ANALYZE
                    exp_sql = "EXPLAIN ANALYZE FORMAT='tidb_json'\n" + sql
                    
                    if param is None:
                        cursor.execute(exp_sql)
                    else:
                        cursor.execute(exp_sql, param)
                    
                    result = cursor.fetchall()
                    if result and result[0][0]:
                        plan = json.loads(result[0][0])[0] if result[0][0] else None
                        
                        if plan and 'executeInfo' in plan:
                            executeInfo = plan['executeInfo']
                            execution_time = parse_execution_time(executeInfo)
                            # 转换为类似PostgreSQL的格式
                            return {'Execution Time': execution_time}
                    return None
            except pymysql.err.OperationalError as e:
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                else:
                    print(f"OLAP worker 连接失败，已达到最大重试次数: {e}")
                    return None
            except Exception as e:
                print(f"OLAP worker 发生未知错误: {type(e).__name__}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                else:
                    print(f"OLAP worker 连接失败，已达到最大重试次数: {e}")
                    return None

                    
if __name__ == '__main__':
    k = 50 ## 并发度
    htapcontroller = HTAPController()

    dyn, sta = 0, 0

    for id in range(1, 22):
        sta += htapcontroller.olap_worker(id)
        p = Pool(k + 1)
        tp = p.map_async(htapcontroller.oltp_worker, range(k))
        ap = p.map_async(htapcontroller.olap_worker, range(id, id + 1))

        p.close()
        p.join()
        if ap.ready():
            dyn += ap.get()[0]

    print(dyn, sta)

