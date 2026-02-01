代码整合思路
将 Bao 的“枚举 49 hint + 选最优”流程替换为 gpredictor 的模型预测。
候选 hint 构造在 run_gpredictor.py 的 build_hint_set，与 run_bao.py 中一致的 49 组合。
gpredictor中除了模型接口和数据转化部分在 gpredictor/adapter.py，其余是一栋师兄写的代码。
模型/数据的简化主要是：
1）并发关系的开始时间简化：目前所有计划起点都设为 0
2）gpredictor模型输出是每个节点（算子）的预测值，在 predict_latency_for_plan 中，只取目标计划的所有节点输出的最大值作为整个计划的预测延迟。

用了绝对路径基准：
项目根：/home/ruc/Optimus/gpredictor-wzk
代码目录：CODE_DIR = PROJ_ROOT/code
模型保存：gpredictor/gp_model.pt（不存在则训练并保存）

健壮性处理（导致跳过样本的改动）
1) 执行计划字段缺失
gpredictor/nodeutils.py
compute_time：优先用 Actual Total/Startup Time，异常回退到 (Total Cost - Startup Cost)/1e6，再不行返回 0.0，避免 KeyError/类型错误。
extract_plan：Actual Startup Time 改为 get(..., 0.0)，防止缺字段崩溃。
2) 路径可靠性
run_alloydb.py / run_gpredictor.py：params、queries_for_plan 全部用 CODE_DIR 拼绝对路径，避免工作目录变化导致 FileNotFoundError。
3) OLAP 计划获取失败
run_alloydb.py / run_gpredictor.py：run_ap_with_tp 返回 plan is None 或缺少 Execution Time 时，打印 warn 并跳过该样本，避免 TypeError 中断。
4) JSON 解析失败
code/utils/htap_query.py 的 HTAPController.olap_worker：捕获 json.JSONDecodeError，最多重试 7 次，失败返回 None 交由上层跳过。
5) TP worker 异常
code/utils/utils.py 的 tp_worker_loop：捕获所有异常（连接断开、smallint out of range 等）打印后继续循环，防止线程崩溃。

运行
python code/baseline/run_gpredictor.py

k1: 50000 k2: 20
[  27765.916   31211.926   72115.727   43821.871   76893.473   26682.158
   70629.579   20069.307   58135.583   68924.922   53240.462   26593.387
   16685.149       0.      69917.709  139483.59  1311865.42    69093.872
   27441.103   21766.089  152789.872    9909.885]
sum latency: 2395037.0
median latency: 2074.6890000000003
95 latency: 12658.168
99 latency: 92546.022
995 latency: 92966.374