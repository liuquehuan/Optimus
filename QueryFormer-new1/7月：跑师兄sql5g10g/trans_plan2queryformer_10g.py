import os
import json
import csv

plan_dir = 'plan/sql_plan on sieze10'
output_dir = 'data/10g'
os.makedirs(output_dir, exist_ok=True)

for fname in os.listdir(plan_dir):
    if not fname.endswith('.txt'):
        continue
    sql_id = fname.split('_')[0]
    with open(os.path.join(plan_dir, fname), 'r', encoding='utf-8') as f:
        lines = f.readlines()
    rows = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('[列存组合]:'):
            storage_type = line.replace('[列存组合]:', '').strip()
            # 下一个非空行是JSON
            i += 1
            while i < len(lines) and lines[i].strip() == '':
                i += 1
            if i < len(lines):
                json_line = lines[i].strip()
                try:
                    plan_json = json.loads(json_line)
                    # 如果最外层是list，取第一个元素
                    if isinstance(plan_json, list):
                        plan_json = plan_json[0]
                    plan_json_str = json.dumps(plan_json, ensure_ascii=False, separators=(',', ':'))
                    row_id = f"{sql_id}_{storage_type}"
                    rows.append({
                        'id': row_id,
                        'json': plan_json_str
                    })
                except Exception as e:
                    print(f"Error parsing JSON in {fname} at line {i+1}: {e}")
        i += 1
    # 写入csv，每个txt对应一个csv
    output_csv = os.path.join(output_dir, fname.replace('.txt', '.csv'))
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'json']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"已生成 {output_csv}，共{len(rows)}条记录。")

print("全部转换完成。") 