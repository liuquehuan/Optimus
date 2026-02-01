import os
import json
import pandas as pd

input_dir = "plan/sql_plan on size5"
output_csv = "data/plan_size5_queryformer.csv"

data = []
idx = 0

for fname in os.listdir(input_dir):
    if not fname.endswith("_column.txt"):
        continue
    with open(os.path.join(input_dir, fname), "r", encoding="utf-8") as fin:
        lines = fin.readlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("[列存组合]:"):
            colstore = line.split(":", 1)[1].strip()
            plan_json = json.loads(lines[i+1].strip())[0]
            # 增加列存信息
            plan_json["columnstore_setting"] = colstore
            # 生成一条训练样本
            data.append({
                "id": idx,
                "json": json.dumps(plan_json, ensure_ascii=False)
            })
            idx += 1
            i += 2
        else:
            i += 1

df = pd.DataFrame(data)
os.makedirs("data", exist_ok=True)
df.to_csv(output_csv, index=False)
print(f"已生成训练数据文件: {output_csv}，共{len(df)}条样本")