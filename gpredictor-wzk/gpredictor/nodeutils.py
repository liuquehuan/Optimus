def compute_cost(node):
    return (float(node["Total Cost"]) - float(node["Startup Cost"])) / 1e6

def compute_time(node):
    """
    计算节点的运行时间。
    优先使用实际时间字段；在字段缺失时做兼容处理，避免 KeyError 直接中断训练。
    """
    # 优先使用 pg EXPLAIN ANALYZE 中提供的实际时间
    actual_total = node.get("Actual Total Time")
    actual_startup = node.get("Actual Startup Time")

    if actual_total is not None:
        try:
            # 如果有启动时间，就用总时间减去启动时间；否则直接用总时间
            if actual_startup is not None:
                return float(actual_total) - float(actual_startup)
            return float(actual_total)
        except (TypeError, ValueError):
            # 字段存在但类型异常时，后续走降级逻辑
            pass

    # 某些计划节点可能没有 Actual Total Time，保守地使用估计代替
    total_cost = node.get("Total Cost")
    startup_cost = node.get("Startup Cost")
    try:
        if total_cost is not None and startup_cost is not None:
            # 与 compute_cost 一致的数量级（秒级时间）
            return (float(total_cost) - float(startup_cost)) / 1e6
    except (TypeError, ValueError):
        pass

    # 实在没有合适字段时，返回 0，保证训练流程不中断
    return 0.0


def get_used_tables(node):
    tables = []

    stack = [node]
    while stack != []:
        parent = stack.pop(0)

        if "Relation Name" in parent:
            tables.append(parent["Relation Name"])

        if "Plans" in parent:
            for n in parent["Plans"]:
                stack.append(n)

    return tables

def overlap(node_i, node_j):
    if (node_j[1] < node_i[2] and node_i[2] < node_j[2]):

        return (node_i[2] - node_j[1]) / (node_j[2] - min(node_i[1], node_j[1]))

    elif (node_i[1] < node_j[2] and node_j[2] < node_i[2]):

        return (node_j[2] - node_i[1]) / (node_i[2] - min(node_i[1], node_j[1]))

    else:
        return 0


def extract_plan(sample, conflict_operators, mp_optype, oid, min_timestamp):
    # global mp_optype, oid, min_timestamp
    if min_timestamp < 0:
        min_timestamp = float(sample["start_time"])
        start_time = 0
    else:
        start_time = float(sample["start_time"]) - min_timestamp
    # function: extract SQL feature
    # return: start_time, node feature, edge feature

    plan = sample["plan"]
    while isinstance(plan, list):
        plan = plan[0]
    # Features: print(plan.keys())
    # start time = plan["start_time"]
    # node feature = [Node Type, Total Cost:: Actual Total Time]
    # node label = [Actual Startup Time, Actual Total Time]

    plan = plan["Plan"]  # root node
    node_matrix = []
    edge_matrix = []
    node_merge_matrix = []

    # add oid for each operator
    stack = [plan]
    while stack != []:
        parent = stack.pop(0)
        parent["oid"] = oid
        oid = oid + 1

        if "Plans" in parent:
            for node in parent["Plans"]:
                stack.append(node)

    stack = [plan]
    while stack != []:
        parent = stack.pop(0)
        run_cost = compute_cost(parent)
        run_time = compute_time(parent)
        # print(parent["Actual Total Time"], parent["Actual Startup Time"], run_time)

        if parent["Node Type"] not in mp_optype:
            mp_optype[parent["Node Type"]] = len(mp_optype)

        tables = get_used_tables(parent)
        # print("[tables]", tables)

        operator_info = [parent["oid"], start_time + parent["Startup Cost"] / 1e6,
                         start_time + parent["Total Cost"] / 1e6]

        for table in tables:
            if table not in conflict_operators:
                conflict_operators[table] = [operator_info]
            else:
                conflict_operators[table].append(operator_info)

        # feature layout: [oid, op_type_id, run_cost, label(run_time)]
        node_feature = [parent["oid"], mp_optype[parent["Node Type"]], run_cost, run_time]

        node_matrix = [node_feature] + node_matrix

        # 启动时间字段有些计划缺失
        actual_startup = float(parent.get("Actual Startup Time", 0.0))

        node_merge_feature = [parent["oid"], start_time + parent["Startup Cost"] / 1e6,
                              start_time + parent["Total Cost"] / 1e6, mp_optype[parent["Node Type"]], run_cost,
                              start_time + actual_startup, run_time]
        node_merge_matrix = [node_merge_feature] + node_merge_matrix
        # [id?, l, r, ....]


        if "Plans" in parent:
            for node in parent["Plans"]:
                stack.append(node)
                edge_matrix = [[node["oid"], parent["oid"], 1]] + edge_matrix

    # node: 18 * featuers
    # edge: 18 * 18

    return start_time, node_matrix, edge_matrix, conflict_operators, node_merge_matrix, mp_optype, oid, min_timestamp


def add_across_plan_relations(conflict_operators, ematrix, knobs=None):

    if knobs is None:
        knobs = [0.09090909090909091, 1.0, 0.1241446725317693,
                0.011764705882352941, 0.060665362035225046, 0.5]

    data_weight = 0.1
    for knob in knobs:
        data_weight *= knob

    # add relations [rw/ww, rr, config]
    for table in conflict_operators:
        for i in range(len(conflict_operators[table])):
            for j in range(i + 1, len(conflict_operators[table])):

                node_i = conflict_operators[table][i]
                node_j = conflict_operators[table][j]

                time_overlap = overlap(node_i, node_j)
                if time_overlap:
                    ematrix = ematrix + [[node_i[0], node_j[0], -data_weight * time_overlap]]
                    ematrix = ematrix + [[node_j[0], node_i[0], -data_weight * time_overlap]]

                '''
                if overlap(i, j) and ("rw" or "ww"):
                    ematrix = ematrix + [[conflict_operators[table][i], conflict_operators[table][j], data_weight * time_overlap]]
                    ematrix = ematrix + [[conflict_operators[table][j], conflict_operators[table][i], data_weight * time_overlap]]
                '''

    return ematrix
