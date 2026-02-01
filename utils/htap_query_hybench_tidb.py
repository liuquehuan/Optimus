import pymysql
import time
import random
import math
import datetime
import os
import json
import re
from multiprocessing import Pool, Value
from typing import Any, Dict, List, Optional, Tuple

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

# ========== 辅助函数 ==========
def _round2(value: float) -> float:
    """四舍五入到小数点后两位"""
    return math.floor(value * 100 + 0.5) / 100.0

# ========== HyBench 随机数生成器 ==========
class HyBenchRandom:
    def __init__(self, seed: Optional[int] = None):
        self.rng = random.Random(seed)

    def randint_exclusive(self, lower: int, upper: int) -> int:
        """返回 [lower, upper) 范围内的随机整数"""
        if upper <= lower:
            raise ValueError("upper must be greater than lower")
        return self.rng.randrange(lower, upper)

    def random_double(self, bound: float) -> float:
        """返回 [0, bound) 范围内的随机浮点数，保留两位小数"""
        return _round2(self.rng.random() * bound)

    def sample_bool(self, p_true: float) -> bool:
        """以 p_true 概率返回 True"""
        return self.rng.random() < p_true

    def choice(self, seq: List[Any]) -> Any:
        """从序列中随机选择一个元素"""
        return self.rng.choice(seq)

    def random_between(self, start_ms: int, end_ms: int) -> int:
        """返回 [start_ms, end_ms) 范围内的随机整数(毫秒)"""
        if end_ms <= start_ms:
            return start_ms
        return self.rng.randrange(start_ms, end_ms)

# ========== HyBench 配置 ==========
class HyBenchConfig:
    def __init__(self, sf: str, parameters_file: str = "/home/ruc/HyBench-2024/src/main/resources/parameters.toml"):
        self.sf = os.environ.get("HYBENCH_SF", sf)
        self.parameters_file = parameters_file

        # 默认配置值
        self.customer_number: int = 1500000
        self.company_number: int = 10000
        self.customer_savingbalance: float = 2000
        self.customer_checkingbalance: float = 5000
        self.customer_loanbalance: float = 10000
        self.company_savingbalance: float = 10000
        self.company_checkingbalance: float = 100000
        self.company_loanbalance: float = 1000000

        self.startDate: datetime.datetime = datetime.datetime.fromisoformat("2014-01-01")
        self.midPointDate: datetime.datetime = datetime.datetime.fromisoformat("2019-01-01")
        self.endDate: datetime.datetime = datetime.datetime.fromisoformat("2023-12-31")
        self.loanDate: datetime.datetime = datetime.datetime.fromisoformat("2023-10-01")

        self._load()

    def _load(self):
        """从配置文件加载参数（如果存在）"""
        if not os.path.exists(self.parameters_file):
            # 如果配置文件不存在，使用默认值
            return
        target_header = f"[{self.sf}]".strip()
        in_block = False
        with open(self.parameters_file, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("[") and line.endswith("]"):
                    in_block = (line == target_header)
                    continue
                if not in_block:
                    continue
                if "=" not in line:
                    continue
                key, val = [x.strip() for x in line.split("=", 1)]
                if val.startswith("\"") and val.endswith("\""):
                    val = val[1:-1]
                if key == "customer_number":
                    self.customer_number = int(val)
                elif key == "company_number":
                    self.company_number = int(val)
                elif key == "customer_savingbalance":
                    self.customer_savingbalance = float(val)
                elif key == "customer_checkingbalance":
                    self.customer_checkingbalance = float(val)
                elif key == "customer_loanbalance":
                    self.customer_loanbalance = float(val)
                elif key == "company_savingbalance":
                    self.company_savingbalance = float(val)
                elif key == "company_checkingbalance":
                    self.company_checkingbalance = float(val)
                elif key == "company_loanbalance":
                    self.company_loanbalance = float(val)
                elif key == "startDate":
                    self.startDate = datetime.datetime.fromisoformat(val)
                elif key == "midPointDate":
                    self.midPointDate = datetime.datetime.fromisoformat(val)
                elif key == "endDate":
                    self.endDate = datetime.datetime.fromisoformat(val)
                elif key == "LoanDate":
                    self.loanDate = datetime.datetime.fromisoformat(val)

# ========== HyBench SQL 定义（TiDB版本，使用?占位符）==========
SQLS: Dict[str, Any] = {
    "TP-1": "select * from customer where custid=?",
    "TP-2": "select * from company where companyID=?",
    "TP-3": "select * from SAVINGACCOUNT where accountid=?",
    "TP-4": "select * from CHECKINGACCOUNT where accountid=?",
    "TP-5": "select * from transfer where sourceid = ? or targetid = ? order by timestamp desc limit 1",
    "TP-6": "select * from checking where sourceid = ? or targetid = ? order by timestamp desc limit 1",
    "TP-7": "select * from loanapps where applicantid=? order by timestamp desc limit 1",
    "TP-8": "select * from loantrans where applicantid=? order by timestamp desc limit 1",
    "TP-9": [
        "SELECT balance FROM savingAccount WHERE accountID = ?",
        "UPDATE savingAccount SET balance = balance - ? where accountID = ?",
        "UPDATE savingAccount SET balance = balance + ? where accountID = ?",
        "INSERT INTO transfer VALUES (DEFAULT, ?,?,?,?,?,?)",
    ],
    "TP-10": [
        "SELECT custID FROM CUSTOMER WHERE COMPANYID = ?",
        "UPDATE SAVINGACCOUNT SET BALANCE = BALANCE - ? WHERE ACCOUNTID = ?",
        "UPDATE SAVINGACCOUNT SET BALANCE = BALANCE + ? WHERE ACCOUNTID = ?",
        "INSERT INTO TRANSFER VALUES (DEFAULT, ?,?,?,?,?)",
    ],
    "TP-11": [
        "SELECT balance FROM CHECKINGACCOUNT WHERE accountID = ?",
        "UPDATE CHECKINGACCOUNT SET balance = balance - ? where accountID = ?",
        "UPDATE CHECKINGACCOUNT SET balance = balance + ? where accountID = ?",
        "INSERT INTO CHECKING VALUES (DEFAULT, ?,?,?,?,?)",
    ],
    "TP-12": [
        "SELECT loan_balance FROM customer WHERE custID = ?",
        "UPDATE customer SET loan_balance = loan_balance - ? where custID = ?",
        "SELECT loan_balance FROM company WHERE companyID = ?",
        "UPDATE company SET loan_balance = loan_balance - ? where companyID = ?",
        "INSERT INTO LOANAPPS  VALUES (DEFAULT, ?,?,?,?,?)",
    ],
    "TP-13": [
        "SELECT ID, APPLICANTID, AMOUNT, DURATION, TIMESTAMP FROM LOANAPPS WHERE status='under_review' ORDER BY RAND() LIMIT 1",
        "INSERT INTO LOANTRANS VALUES(DEFAULT, ?,?,?,?,?,?,?,?)",
        "UPDATE CUSTOMER SET loan_balance = loan_balance + ? where custID = ?",
        "UPDATE COMPANY SET loan_balance = loan_balance + ? where companyID = ?",
        "UPDATE LOANAPPS SET STATUS=? WHERE ID=?",
    ],
    "TP-14": [
        "SELECT id, APPLICANTID, AMOUNT, contract_timestamp FROM LOANTRANS WHERE status='accept' ORDER BY RAND() LIMIT 1",
        "UPDATE SAVINGACCOUNT SET BALANCE = BALANCE + ? WHERE ACCOUNTID = ?",
        "UPDATE LOANTRANS SET STATUS='lent', timestamp=? WHERE ID=?",
    ],
    "TP-15": [
        "SELECT id, APPLICANTID, duration, timestamp FROM LOANTRANS WHERE status='lent' order by timestamp limit 1",
        "UPDATE LOANTRANS SET delinquency=1, timestamp=? WHERE id=?",
    ],
    "TP-16": [
        "SELECT id, APPLICANTID, duration, amount, timestamp FROM LOANTRANS WHERE status='lent' ORDER BY RAND() LIMIT 1",
        "SELECT balance from SAVINGACCOUNT WHERE ACCOUNTID = ?",
        "UPDATE SAVINGACCOUNT SET BALANCE = BALANCE - ? WHERE ACCOUNTID = ?",
        "UPDATE LOANTRANS SET status='repaid',timestamp=? WHERE id=?",
    ],
    "TP-17": [
        "select balance,isblocked from savingAccount where accountid = ?",
        "select balance,isblocked from checkingAccount where accountid =?",
        "UPDATE SAVINGACCOUNT SET BALANCE = BALANCE - ? WHERE ACCOUNTID = ?",
        "update checkingAccount set balance = balance + ? where accountid = ?",
    ],
    "TP-18": [
        "select balance,isblocked from savingAccount where accountid = ?",
        "select balance,isblocked from checkingAccount where accountid =?",
        "update checkingAccount set balance = balance - ? where accountid = ?",
        "UPDATE SAVINGACCOUNT SET BALANCE = BALANCE + ? WHERE ACCOUNTID = ?",
    ],
}

# ========== 数据库连接类 ==========
class DBConn:
    def __init__(self, database_name, statement_timeout: int = 0):
        self.conn: Optional[pymysql.connections.Connection] = None
        self.cursor: Optional[pymysql.cursors.Cursor] = None
        self.statement_timeout = statement_timeout
        self.database_name = database_name

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

# ========== HyBench 事务 Worker ==========
class TransactionalWorker:
    def __init__(self, rng: HyBenchRandom, conn: DBConn, cfg: HyBenchConfig):
        self.rng = rng
        self.conn = conn
        self.cfg = cfg
        self.customer_no: int = self.cfg.customer_number + 1
        self.company_no: int = self.cfg.company_number
        # 事务概率分布: TP-1到TP-8不执行(概率为0), TP-9到TP-18按指定概率执行
        self.tp_p = [
            0, 0, 0, 0, 0, 0, 0, 0,  # TP-1 到 TP-8
            10, 10, 10, 10, 9, 6, 6, 5, 17, 17  # TP-9 到 TP-18
        ]
        self._tp_prefix = self._build_prefix(self.tp_p)

    def _build_prefix(self, arr: List[int]) -> List[int]:
        """构建前缀和数组，用于根据概率选择事务"""
        s = 0
        p: List[int] = []
        for v in arr:
            s += v
            p.append(s)
        return p

    def _pick_txn_id(self) -> int:
        """根据概率分布随机选择一个事务 ID"""
        x = self.rng.randint_exclusive(1, 101)
        for idx, bound in enumerate(self._tp_prefix):
            if x <= bound:
                return idx + 1
        return len(self._tp_prefix)

    def _ts_between(self, start_dt: datetime.datetime, end_dt: datetime.datetime) -> datetime.datetime:
        """在两个日期之间生成随机时间戳"""
        ms = self.rng.random_between(int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000))
        return datetime.datetime.fromtimestamp(ms / 1000.0)

    def _loan_current_ts(self, base_ts: Optional[datetime.datetime]) -> datetime.datetime:
        """生成贷款相关的当前时间戳"""
        if base_ts is None:
            return self.cfg.endDate
        base_ms = int(base_ts.timestamp() * 1000)
        end_ms = int(self.cfg.endDate.timestamp() * 1000)
        if base_ms > end_ms:
            ms = self.rng.random_between(end_ms, base_ms)
        else:
            ms = self.rng.random_between(base_ms, end_ms)
        return datetime.datetime.fromtimestamp(ms / 1000.0)

    def _rand_account_id(self) -> int:
        """随机生成一个账户 ID"""
        return self.rng.randint_exclusive(1, self.customer_no + self.company_no)

    def _execute_sql(self, sql: str, args: Optional[Tuple] = None, explain: bool = False, analyze: bool = False):
        """执行SQL，根据explain和analyze参数决定是否返回执行计划"""
        if not explain:
            self.conn.cursor.execute(sql, args)
            return None
        
        if analyze:
            explain_sql = "EXPLAIN ANALYZE FORMAT='tidb_json'\n" + sql
        else:
            explain_sql = "EXPLAIN FORMAT='tidb_json'\n" + sql
        
        self.conn.cursor.execute(explain_sql, args)
        result = self.conn.cursor.fetchall()
        if result and result[0][0]:
            plan = json.loads(result[0][0])[0] if result[0][0] else None
            return plan
        return None

    # ========== 18 个 HyBench 事务 ==========
    def txn1(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-1"]
        custid = self.rng.randint_exclusive(1, self.customer_no)
        return self._execute_sql(sql, (custid,), explain, analyze)

    def txn2(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-2"]
        companyid = self.rng.randint_exclusive(self.customer_no, self.customer_no + self.company_no)
        return self._execute_sql(sql, (companyid,), explain, analyze)

    def txn3(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-3"]
        account_id = self._rand_account_id()
        return self._execute_sql(sql, (account_id,), explain, analyze)

    def txn4(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-4"]
        account_id = self._rand_account_id()
        return self._execute_sql(sql, (account_id,), explain, analyze)

    def txn5(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-5"]
        account_id = self._rand_account_id()
        return self._execute_sql(sql, (account_id, account_id), explain, analyze)

    def txn6(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-6"]
        account_id = self._rand_account_id()
        return self._execute_sql(sql, (account_id, account_id), explain, analyze)

    def txn7(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-7"]
        applicantid = self._rand_account_id()
        return self._execute_sql(sql, (applicantid,), explain, analyze)

    def txn8(self, explain: bool = False, analyze: bool = False):
        sql = SQLS["TP-8"]
        applicantid = self._rand_account_id()
        return self._execute_sql(sql, (applicantid,), explain, analyze)

    def txn9(self, explain: bool = False, analyze: bool = False):
        """储蓄账户转账"""
        sql1, sql2, sql3, sql4 = SQLS["TP-9"]
        sourceid = self._rand_account_id()
        targetid = self._rand_account_id()
        if sourceid < self.customer_no:
            amount = self.rng.random_double(self.cfg.customer_savingbalance * 0.0001)
            tr_type = self.rng.choice(["transfer", "red_packet", "donate", "invest"])
        else:
            amount = self.rng.random_double(self.cfg.company_savingbalance * 0.0001)
            tr_type = self.rng.choice(["business", "salary", "service", "invest"])
        ts = self._ts_between(self.cfg.midPointDate, self.cfg.endDate)

        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, (sourceid,), True, False)
                if analyze:
                    self.conn.cursor.execute(sql1, (sourceid,))
            else:
                self.conn.cursor.execute(sql1, (sourceid,))
            
            if explain and not analyze:
                return plan_result
            row = self.conn.cursor.fetchone()
            balance = float(row[0]) if row else 0.0
            if balance < amount:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql2, (amount, sourceid))
            self.conn.cursor.execute(sql3, (amount, targetid))
            self.conn.cursor.execute(sql4, (sourceid, targetid, amount, tr_type, ts, None))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn10(self, explain: bool = False, analyze: bool = False):
        """公司批量发薪"""
        sql1, sql2, sql3, sql4 = SQLS["TP-10"]
        companyid = self.rng.randint_exclusive(self.customer_no, self.customer_no + self.company_no)
        salary = self.rng.random_double(self.cfg.company_savingbalance * 0.01)
        tr_type = "salary"
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, (companyid,), True, False)
                if analyze:
                    self.conn.cursor.execute(sql1, (companyid,))
            else:
                self.conn.cursor.execute(sql1, (companyid,))
            if explain and not analyze:
                return plan_result
            employees = self.conn.cursor.fetchall()
            for (custid,) in employees:
                self.conn.cursor.execute(sql2, (salary, companyid))
                self.conn.cursor.execute(sql3, (salary, custid))
                ts = self._ts_between(self.cfg.midPointDate, self.cfg.endDate)
                self.conn.cursor.execute(sql4, (companyid, custid, salary, tr_type, ts))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn11(self, explain: bool = False, analyze: bool = False):
        """支票账户转账"""
        sql1, sql2, sql3, sql4 = SQLS["TP-11"]
        sourceid = self._rand_account_id()
        targetid = self._rand_account_id()
        if sourceid < self.customer_no:
            amount = self.rng.random_double(self.cfg.customer_checkingbalance * 0.0001)
            tr_type = self.rng.choice(["check", "others"])
        else:
            amount = self.rng.random_double(self.cfg.company_checkingbalance * 0.0001)
            tr_type = self.rng.choice(["business", "service", "invest"])
        ts = self._ts_between(self.cfg.midPointDate, self.cfg.endDate)

        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, (sourceid,), True, False)
                if analyze:
                    self.conn.cursor.execute(sql1, (sourceid,))
            else:
                self.conn.cursor.execute(sql1, (sourceid,))
            
            if explain and not analyze:
                return plan_result
            row = self.conn.cursor.fetchone()
            balance = float(row[0]) if row else 0.0
            if balance < amount:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql2, (amount, sourceid))
            self.conn.cursor.execute(sql3, (amount, targetid))
            self.conn.cursor.execute(sql4, (sourceid, targetid, amount, tr_type, ts))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn12(self, explain: bool = False, analyze: bool = False):
        """贷款申请"""
        sql1, sql2, sql3, sql4, sql5 = SQLS["TP-12"]
        applicantid = self._rand_account_id()
        if applicantid < self.customer_no:
            amount = self.rng.random_double(self.cfg.customer_loanbalance)
        else:
            amount = self.rng.random_double(self.cfg.company_loanbalance)
        duration = self.rng.choice([30, 60, 90, 180, 365])
        status = "under_review"
        ts = self._ts_between(self.cfg.loanDate, self.cfg.endDate)

        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if applicantid < self.customer_no:
                if explain:
                    plan_result = self._execute_sql(sql1, (applicantid,), True, False)
                    if analyze:
                        self.conn.cursor.execute(sql1, (applicantid,))
                else:
                    self.conn.cursor.execute(sql1, (applicantid,))
                
                if explain and not analyze:
                    return plan_result
                row = self.conn.cursor.fetchone()
                balance = float(row[0]) if row else 0.0
                if balance < amount:
                    self.conn.conn.rollback()
                    if explain:
                        return plan_result
                    else:
                        return None
                self.conn.cursor.execute(sql2, (amount, applicantid))
            else:
                if explain:
                    plan_result = self._execute_sql(sql3, (applicantid,), True, False)
                    if analyze:
                        self.conn.cursor.execute(sql3, (applicantid,))
                else:
                    self.conn.cursor.execute(sql3, (applicantid,))
                
                if explain and not analyze:
                    return plan_result
                row = self.conn.cursor.fetchone()
                balance = float(row[0]) if row else 0.0
                if balance < amount:
                    self.conn.conn.rollback()
                    if explain:
                        return plan_result
                    else:
                        return None
                self.conn.cursor.execute(sql4, (amount, applicantid))

            self.conn.cursor.execute(sql5, (applicantid, amount, duration, status, ts))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn13(self, explain: bool = False, analyze: bool = False):
        """贷款审批"""
        sql1, sql2, sql3, sql4, sql5 = SQLS["TP-13"]
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, None, True, False)
                if analyze:
                    self.conn.cursor.execute(sql1)
            else:
                self.conn.cursor.execute(sql1)
            
            if explain and not analyze:
                return plan_result
            row = self.conn.cursor.fetchone()
            if not row:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            appid, applicantID, amount, duration, appts = row
            accept = self.rng.sample_bool(0.5)
            if accept:
                contract_ts = self._loan_current_ts(appts)
                self.conn.cursor.execute(
                    sql2,
                    (applicantID, appid, amount, "accept", appts, duration, contract_ts, 0),
                )
            else:
                if applicantID < self.customer_no:
                    self.conn.cursor.execute(sql3, (amount, applicantID))
                else:
                    self.conn.cursor.execute(sql4, (amount, applicantID))
            self.conn.cursor.execute(sql5, ("accept" if accept else "reject", appid))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn14(self, explain: bool = False, analyze: bool = False):
        """贷款发放"""
        sql1, sql2, sql3 = SQLS["TP-14"]
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, None, True, False)
                if analyze:
                    self.conn.cursor.execute(sql1)
            else:
                self.conn.cursor.execute(sql1)
            
            if explain and not analyze:
                return plan_result
            row = self.conn.cursor.fetchone()
            if not row:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            id_, applicantid, amount, contract_ts = row
            current_ts = self._loan_current_ts(contract_ts)
            self.conn.cursor.execute(sql2, (amount, applicantid))
            self.conn.cursor.execute(sql3, (current_ts, id_))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn15(self, explain: bool = False, analyze: bool = False):
        """贷款逾期检查"""
        sql1, sql2 = SQLS["TP-15"]
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, None, True, False)
                if analyze:
                    self.conn.cursor.execute(sql1)
            else:
                self.conn.cursor.execute(sql1)
            
            if explain and not analyze:
                return plan_result
            row = self.conn.cursor.fetchone()
            if not row:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            id_, applicantID, duration, ts = row
            current_date_ms = int(self.cfg.endDate.timestamp() * 1000)
            contract_ms = 0 if ts is None else int(ts.timestamp() * 1000)
            if contract_ms + duration * 24 * 60 * 60 * 1000 < current_date_ms:
                current_ts = self.cfg.endDate
                self.conn.cursor.execute(sql2, (current_ts, id_))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn16(self, explain: bool = False, analyze: bool = False):
        """贷款偿还"""
        sql1, sql2, sql3, sql4 = SQLS["TP-16"]
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, None, True, False)
                if analyze:
                    self.conn.cursor.execute(sql1)
            else:
                self.conn.cursor.execute(sql1)
            
            if explain and not analyze:
                return plan_result
            row = self.conn.cursor.fetchone()
            if not row:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            id_, applicantID, duration, amount, ts = row
            current_ts = self._loan_current_ts(ts)
            self.conn.cursor.execute(sql2, (applicantID,))
            r = self.conn.cursor.fetchone()
            balance = float(r[0]) if r else 0.0
            if balance < amount:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql3, (amount, applicantID))
            self.conn.cursor.execute(sql4, (current_ts, id_))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn17(self, explain: bool = False, analyze: bool = False):
        """储蓄账户到支票账户转账"""
        sql1, sql2, sql3, sql4 = SQLS["TP-17"]
        accountid = self._rand_account_id()
        amount = self.rng.random_double(
            self.cfg.customer_savingbalance * 0.0001 
            if accountid < self.customer_no 
            else self.cfg.company_savingbalance * 0.0001
        )
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, (accountid,), True, False)
                if analyze:
                    self.conn.cursor.execute(sql1, (accountid,))
            else:
                self.conn.cursor.execute(sql1, (accountid,))
            
            if explain and not analyze:
                return plan_result
            r = self.conn.cursor.fetchone()
            if not r:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            balance_sav, is_blocked_sav = float(r[0]), int(r[1])
            if balance_sav < amount or is_blocked_sav == 1:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql2, (accountid,))
            r2 = self.conn.cursor.fetchone()
            if not r2:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            _, is_blocked_chk = float(r2[0]), int(r2[1])
            if is_blocked_chk == 1:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql3, (amount, accountid))
            self.conn.cursor.execute(sql4, (amount, accountid))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def txn18(self, explain: bool = False, analyze: bool = False):
        """支票账户到储蓄账户转账"""
        sql1, sql2, sql3, sql4 = SQLS["TP-18"]
        accountid = self._rand_account_id()
        amount = self.rng.random_double(
            self.cfg.customer_savingbalance * 0.0001 
            if accountid < self.customer_no 
            else self.cfg.company_savingbalance * 0.0001
        )
        plan_result = None
        old_autocommit = self.conn.conn.autocommit
        self.conn.conn.autocommit = False
        try:
            if explain:
                plan_result = self._execute_sql(sql1, (accountid,), True, False)
                if analyze:
                    self.conn.cursor.execute(sql1, (accountid,))
            else:
                self.conn.cursor.execute(sql1, (accountid,))
            
            if explain and not analyze:
                return plan_result
            r = self.conn.cursor.fetchone()
            if not r:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            balance_sav, is_blocked_sav = float(r[0]), int(r[1])
            if is_blocked_sav == 1:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql2, (accountid,))
            r2 = self.conn.cursor.fetchone()
            if not r2:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            balance_chk, is_blocked_chk = float(r2[0]), int(r2[1])
            if balance_chk < amount or is_blocked_chk == 1:
                self.conn.conn.rollback()
                if explain:
                    return plan_result
                else:
                    return None
            self.conn.cursor.execute(sql3, (amount, accountid))
            self.conn.cursor.execute(sql4, (amount, accountid))
            self.conn.conn.commit()
        except Exception:
            self.conn.conn.rollback()
            raise
        finally:
            self.conn.conn.autocommit = old_autocommit
            if explain:
                return plan_result
            else:
                return None

    def next_transaction(self, explain: bool = False, analyze: bool = False):
        """执行下一个随机选择的事务"""
        txn_id = self._pick_txn_id()
        result = getattr(self, f"txn{txn_id}")(explain=explain, analyze=analyze)
        return result

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

# ========== HTAP 控制器 ==========
class HTAPController:
    def __init__(self, sf: str, database_name: Optional[str] = None):
        self.cfg = HyBenchConfig(sf=sf)
        self._rng_by_worker: Dict[int, HyBenchRandom] = {}
        self.database_name = database_name or f"hybench_sf{sf}"
        print(f'HyBench Scale Factor: {self.cfg.sf}')
        print(f'Customer Number: {self.cfg.customer_number}')
        print(f'Company Number: {self.cfg.company_number}')
        print(f'Database Name: {self.database_name}')

    def oltp_worker(self, worker_id: int, explain: bool = False, analyze: bool = False):
        """OLTP 事务执行器"""
        with DBConn(self.database_name) as conn:
            rng = self._rng_by_worker.get(worker_id)
            if rng is None:
                rng = HyBenchRandom(seed=worker_id)
                self._rng_by_worker[worker_id] = rng
            worker = TransactionalWorker(rng, conn, self.cfg)
            return worker.next_transaction(explain=explain, analyze=analyze)

    def olap_worker(self, sql: str, param: Optional[Tuple] = None, timeout: int = 0, mode: str = 'hybrid'):
        """OLAP 查询执行器
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
                        # COL模式：优先使用tiflash（列引擎），如果没有tiflash副本则回退到tikv，MPP强制开启
                        cursor.execute("SET @@session.tidb_isolation_read_engines='tiflash,tikv,tidb'")
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
                    return None

