import numpy as np # type: ignore

JOIN_TYPES = ["Nested Loop", "Hash Join", "Merge Join", "Sort", "Append"]
LEAF_TYPES = ["Seq Scan", "Index Scan", "Custom Scan", "Bitmap Index Scan", "Index Only Scan", "CTE Scan"]
ALL_TYPES = JOIN_TYPES + LEAF_TYPES
TABLES = ["customer", "district", "history", "item", "nation", "new_orders", "order_line", "orders", "region", "stock", "supplier", "warehouse"]


class TreeBuilderError(Exception):
    def __init__(self, msg):
        self.__msg = msg

def is_join(node):
    return node["Node Type"] in JOIN_TYPES

def is_scan(node):
    return node["Node Type"] in LEAF_TYPES


## todo：1.去掉type 2.加上width 3.加上tables 4.加上preds 5.加上labels
## 编码tables的逻辑：0.只有scan才有一个relation name 2.relation name为一个列表，父亲的列表是儿子的列表的拼接 3.特征长度为表数量+1，多的一维表示未知。每一维的值为该表的count。
class TreeBuilder:
    def __init__(self, stats_extractor, relations):
        self.__stats = stats_extractor
        ## 按表名称长度降序排序
        self.__relations = sorted(relations, key=lambda x: len(x), reverse=True)

    def __relation_name(self, node):
        if "Relation Name" in node:
            return node["Relation Name"]

        if node["Node Type"] == "Bitmap Index Scan":
            # find the first (longest) relation name that appears in the index name
            name_key = "Index Name" if "Index Name" in node else "Relation Name"
            if name_key not in node:
                print(node)
                raise TreeBuilderError("Bitmap operator did not have an index name or a relation name")
            for rel in self.__relations:
                if rel in node[name_key]:
                    return rel

            raise TreeBuilderError("Could not find relation name for bitmap index scan")
        
        if node["Node Type"] == "CTE Scan":
            if "CTE Name" in node:
                return node["CTE Name"]

        raise TreeBuilderError("Cannot extract relation type from node: " + str(node))
                
    
    ## 特征向量的构成：pred，cost，card，width，tables
    def __featurize_join(self, node, rels):
        assert is_join(node)
        # arr = np.zeros(len(ALL_TYPES))
        # arr[ALL_TYPES.index(node["Node Type"])] = 1
        # return np.concatenate((arr, self.__stats(node)))

        rel_vec = np.zeros(len(TABLES) + 1)
        for rel in rels:
            if rel in TABLES:
                rel_vec[TABLES.index(rel)] += 1
            else:
                rel_vec[-1] += 1
        return np.concatenate((self.__stats(node), rel_vec))

    ## scan会多返回一个表名称
    def __featurize_scan(self, node):
        assert is_scan(node)
        # arr = np.zeros(len(ALL_TYPES))
        # arr[ALL_TYPES.index(node["Node Type"])] = 1
        # return (np.concatenate((arr, self.__stats(node))),
        #         self.__relation_name(node))

        rels = [self.__relation_name(node)]
        rel_vec = np.zeros(len(TABLES) + 1)
        for rel in rels:
            if rel in TABLES:
                rel_vec[TABLES.index(rel)] += 1
            else:
                rel_vec[-1] += 1

        return (np.concatenate((self.__stats(node), rel_vec)), rels)


    ## todo: how to deal with operator "Append"?
    def plan_to_feature_tree(self, plan):
        children = plan["Plans"] if "Plans" in plan else []

        if is_scan(plan):
            # assert not children
            return self.__featurize_scan(plan)

        if len(children) == 1:
            return self.plan_to_feature_tree(children[0])

        # if is_join(plan):
        if plan["Node Type"] in JOIN_TYPES[:3]:
            if len(children) > 2:
                if len(children) != 4:
                    raise TreeBuilderError("The number of children is greater than 2: " + str(plan))
                children.reverse()

            rels = []
            left = self.plan_to_feature_tree(children[0])
            right = self.plan_to_feature_tree(children[1])

            rels.extend(left[-1])
            rels.extend(right[-1])
            my_vec = self.__featurize_join(plan, rels)
            return (my_vec, left, right, JOIN_TYPES.index(plan["Node Type"]), rels)
        
        if plan["Node Type"] == "Append":
            # if len(children) > 2:
            #     raise TreeBuilderError("The number of children is greater than 2: " + str(plan))
            # if children[0]["Node Type"] != "Custom Scan":
            #     raise TreeBuilderError("The first child of Append must be a Custom Scan: " + str(plan))
            
            # rels = []
            # left = self.plan_to_feature_tree(children[0])
            # right = self.plan_to_feature_tree(children[1])
            # rels.extend(left[-1])
            # rels.extend(right[-1])
            # my_vec = self.__featurize_join(plan, rels)
            # return (my_vec, left, right, JOIN_TYPES.index(plan["Node Type"]), rels)
            return self.plan_to_feature_tree(children[0])
        
        if plan["Node Type"] == "Sort":
            if len(children) > 3:
                raise TreeBuilderError("Children number of sort is greater than 3: " + str(plan))
            if len(children) == 3:
                children[1] = children[2] ## InitPlan2
            # arr = np.zeros(len(ALL_TYPES))
            # my_vec = np.concatenate((arr, self.__stats(plan)))

            rels = []
            left = self.plan_to_feature_tree(children[0])
            right = self.plan_to_feature_tree(children[1])
            rels.extend(left[-1])
            rels.extend(right[-1])
            my_vec = self.__featurize_join(plan, rels)
            return (my_vec, left, right, JOIN_TYPES.index(plan["Node Type"]), rels)
        
        if plan["Node Type"] == "BitmapOr":
            return self.plan_to_feature_tree(children[0])
        if plan["Node Type"] == "Aggregate":
            if len(children) > 2:
                raise TreeBuilderError("The number of children is greater than 2: " + str(plan))
            return self.plan_to_feature_tree(children[1])
        
        if plan["Node Type"] == "Bitmap Heap Scan":
            return self.plan_to_feature_tree(children[0])
        
        raise TreeBuilderError("Node wasn't transparent, a join, or a scan: " + str(plan))


## 范数：加1取对数，然后进行归一化
def norm(x, lo, hi):
    return (np.log(x + 1) - lo) / (hi - lo)


## 看上去是为了对数据进行归一化用的
class StatExtractor:
    def __init__(self, fields, mins, maxs):
        self.__fields = fields
        self.__mins = mins
        self.__maxs = maxs

    ## inp应该是一个字典，这一步把inp中的对应字段进行归一化
    def __call__(self, inp):
        res = []
        for f, lo, hi in zip(self.__fields, self.__mins, self.__maxs):
            if f not in inp:
                res.append(0)
            else:
                res.append(norm(inp[f], lo, hi))
        return res


## 获得输入的所有计划的所有节点的total costs和plan rows的最值，初始化一个StatExtractor以便后续进行归一化
def get_plan_stats(data):
    costs = []
    rows = []
    width = []
    
    def recurse(n):
        costs.append(n["Total Cost"])
        rows.append(n["Plan Rows"])
        width.append(n["Plan Width"])

        if "Plans" in n:
            for child in n["Plans"]:
                recurse(child)

    for plan in data:
        recurse(plan["Plan"])

    costs = np.array(costs)
    rows = np.array(rows)
    width = np.array(width)
    costs = np.log(costs + 1)
    rows = np.log(rows + 1)
    width = np.log(width + 1)
    costs_min = np.min(costs)
    costs_max = np.max(costs)
    rows_min = np.min(rows)
    rows_max = np.max(rows)
    width_min = np.min(width)
    width_max = np.max(width)
    return StatExtractor(
        ["Total Cost", "Plan Rows", "Plan Width"],
        [costs_min, rows_min, width_min],
        [costs_max, rows_max, width_max]
    )
        

## 返回输入的所有计划涉及的表的集合
def get_all_relations(data):
    all_rels = []
    
    def recurse(plan):
        if "Relation Name" in plan:
            yield plan["Relation Name"]

        if "Plans" in plan:
            for child in plan["Plans"]:
                yield from recurse(child)

    for plan in data:
        all_rels.extend(list(recurse(plan["Plan"])))
        
    return set(all_rels)


class TreeFeaturizer:
    def __init__(self):
        self.__tree_builder = None

    ## 输入一批计划时，要进行初始化
    def fit(self, trees):
        all_rels = get_all_relations(trees)
        stats_extractor = get_plan_stats(trees)
        self.__tree_builder = TreeBuilder(stats_extractor, all_rels)


    ## 1.每棵树是一个字典，其plan字段表示了真正的计划
    ## 2.输入是一个由树组成的列表
    def transform(self, trees):
        return [self.__tree_builder.plan_to_feature_tree(x["Plan"]) for x in trees]

    def num_operators(self):
        return len(ALL_TYPES)
