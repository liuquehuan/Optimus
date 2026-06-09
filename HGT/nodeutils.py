def compute_cost(node):
    return (float(node["Total Cost"]) - float(node["Startup Cost"])) / 1e6


def get_used_tables(node): ## 涉及的所有表名
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


JOIN_TYPES = ["Nested Loop", "Hash Join", "Merge Join"]
LEAF_TYPES = ["Seq Scan", "Index Scan", "Custom Scan", "Bitmap Index Scan", "Index Only Scan", "CTE Scan"]
ALL_TYPES = JOIN_TYPES + LEAF_TYPES


def is_join(node):
    return node["Node Type"] in JOIN_TYPES


def is_scan(node):
    return node["Node Type"] in LEAF_TYPES


def __featurize_join(node):
    assert is_join(node)
    arr = [0] * len(ALL_TYPES)
    arr[ALL_TYPES.index(node["Node Type"])] = 1
    return arr


def __featurize_scan(node):
    assert is_scan(node)
    arr = [0] * len(ALL_TYPES)
    arr[ALL_TYPES.index(node["Node Type"])] = 1
    return arr


class TreeBuilderError(Exception):
    def __init__(self, msg):
        self.__msg = msg


def extract_plan(sample, conflict_operators, oid, is_ap=False):

    plan = sample["Plan"]
    while isinstance(plan, list):
        plan = plan[0]

    node_matrix = []
    edge_matrix = []

    def plan_to_feature_tree(plan):
        nonlocal oid
        nonlocal node_matrix
        nonlocal edge_matrix
        children = plan["Plans"] if "Plans" in plan else []

        if len(children) == 1:
            return plan_to_feature_tree(children[0])
        
        if plan["Node Type"] == "Append":
            return plan_to_feature_tree(children[0])
        
        if plan["Node Type"] == "BitmapOr":
            return plan_to_feature_tree(children[0])
        if plan["Node Type"] == "Aggregate":
            if len(children) > 2:
                raise TreeBuilderError("The number of children is greater than 2: " + str(plan))
            return plan_to_feature_tree(children[1])
        
        if plan["Node Type"] == "Bitmap Heap Scan":
            return plan_to_feature_tree(children[0])
        
        oid = oid + 1
        operator_info = [oid, plan["Startup Cost"] / 1e6, plan["Total Cost"] / 1e6]
        tables = get_used_tables(plan)
        for table in tables:
            if table not in conflict_operators:
                conflict_operators[table] = [operator_info]
            else:
                conflict_operators[table].append(operator_info)

        run_cost = compute_cost(plan)
        if plan["Node Type"] == "Sort":
            if len(children) > 3:
                raise TreeBuilderError("Children number of sort is greater than 3: " + str(plan))
            if len(children) == 3:
                children[1] = children[2] ## InitPlan2
            arr = [0] * len(ALL_TYPES)
            node_feature = [oid] + [run_cost] + [plan["Plan Rows"]] + arr
            node_matrix = [node_feature] + node_matrix
            left = plan_to_feature_tree(children[0])
            right = plan_to_feature_tree(children[1])
            return (oid, left, right)
        
        if is_join(plan):
            if len(children) > 2:
                if len(children) != 4:
                    raise TreeBuilderError("The number of children is greater than 2: " + str(plan))
                children.reverse()
            
            node_feature = [oid] + [run_cost] + [plan["Plan Rows"]] + __featurize_join(plan)
            node_matrix = [node_feature] + node_matrix
            left = plan_to_feature_tree(children[0])
            right = plan_to_feature_tree(children[1])
            return (oid, left, right)

        if is_scan(plan):
            assert not children
            node_feature = [oid] + [run_cost] + [plan["Plan Rows"]] + __featurize_scan(plan)
            node_matrix = [node_feature] + node_matrix
            return (oid, )
        
        raise TreeBuilderError("Node wasn't transparent, a join, or a scan: " + str(plan))

    if is_ap:
        tree = plan_to_feature_tree(plan)

        def recurse(tree):
            nonlocal edge_matrix
            ## leaf
            if len(tree) == 1:
                return

            edge_matrix = [[tree[0], tree[1][0], 1], [tree[0], tree[2][0], 1]] + edge_matrix
            recurse(tree[1])
            recurse(tree[2])

        recurse(tree)

        return node_matrix, edge_matrix, conflict_operators, oid


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

        tables = get_used_tables(parent)

        operator_info = [parent["oid"], parent["Startup Cost"] / 1e6, parent["Total Cost"] / 1e6]

        for table in tables:
            if table not in conflict_operators:
                conflict_operators[table] = [operator_info]
            else:
                conflict_operators[table].append(operator_info)

        arr = [0] * len(ALL_TYPES)
        node_feature = [parent["oid"]] + [run_cost] + [parent["Plan Rows"]] + arr

        node_matrix = [node_feature] + node_matrix

        if "Plans" in parent:
            for node in parent["Plans"]:
                stack.append(node)
                edge_matrix = [[node["oid"], parent["oid"], 1]] + edge_matrix

    return node_matrix, edge_matrix, conflict_operators, oid


def add_across_plan_relations(conflict_operators, ematrix):
    # TODO better implementation
    data_weight = 0.1
    # print(conflict_operators)

    # add relations [rw/ww, rr, config]
    for table in conflict_operators:
        for i in range(len(conflict_operators[table])):
            for j in range(i + 1, len(conflict_operators[table])):

                node_i = conflict_operators[table][i]
                node_j = conflict_operators[table][j]

                # time_overlap = overlap(node_i, node_j)
                # if time_overlap:
                #     ematrix = ematrix + [[node_i[0], node_j[0], -data_weight * time_overlap]]
                #     ematrix = ematrix + [[node_j[0], node_i[0], -data_weight * time_overlap]]

                ematrix = ematrix + [[node_i[0], node_j[0], -data_weight]]
                ematrix = ematrix + [[node_j[0], node_i[0], -data_weight]]

                '''
                if overlap(i, j) and ("rw" or "ww"):
                    ematrix = ematrix + [[conflict_operators[table][i], conflict_operators[table][j], data_weight * time_overlap]]
                    ematrix = ematrix + [[conflict_operators[table][j], conflict_operators[table][i], data_weight * time_overlap]]
                '''

    return ematrix
