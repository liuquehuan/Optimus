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


## node_feature改了，需要重新写
def extract_plan(sample, conflict_operators, oid):

    plan = sample["Plan"]
    while isinstance(plan, list):
        plan = plan[0]

    node_matrix = []
    edge_matrix = []

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

        for table in tables: ## 按表把会造成冲突的节点组织在一起
            if table not in conflict_operators:
                conflict_operators[table] = [operator_info]
            else:
                conflict_operators[table].append(operator_info)

        ## node_feature第一维是oid
        # arr = [0] * len(ALL_TYPES)
        # node_feature = [parent["oid"]] + [run_cost] + [parent["Plan Rows"]] + arr

        # ## node_matrix的元素是node_feature
        # node_matrix = [node_feature] + node_matrix

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
