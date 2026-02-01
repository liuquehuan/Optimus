#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将data-9-8文件夹中的新数据转换为QueryFormer训练所需的CSV格式
"""

import pandas as pd
import json
import os
import numpy as np
from tqdm import tqdm

def convert_txt_to_csv(txt_file_path, output_csv_path):
    """
    将txt文件转换为QueryFormer训练所需的CSV格式
    
    Args:
        txt_file_path: 输入的txt文件路径
        output_csv_path: 输出的CSV文件路径
    """
    print(f"正在处理文件: {txt_file_path}")
    
    data = []
    with open(txt_file_path, 'r', encoding='utf-8') as f:
        for idx, line in enumerate(tqdm(f, desc="处理数据")):
            line = line.strip()
            if not line:
                continue
                
            # 分割JSON计划和执行时间
            parts = line.split('\t')
            if len(parts) != 2:
                print(f"警告: 第{idx+1}行格式不正确: {line[:100]}...")
                continue
                
            json_plan = parts[0].strip()
            execution_time = float(parts[1].strip())
            
            try:
    
                plan_data = json.loads(json_plan)
                
                if isinstance(plan_data, list) and len(plan_data) > 0:
                    plan = plan_data[0]['Plan']
                else:
                    plan = plan_data['Plan']
                
                # 修改计划中的字段名，将 Plan Rows 改为 Actual Rows
                if 'Plan Rows' in plan:
                    plan['Actual Rows'] = plan.pop('Plan Rows')
                
                # 构建QueryFormer的JSON格式
                corrected_json = json.dumps({
                    "Plan": plan,
                    "Execution Time": execution_time
                })
                
                # 构建数据行
                row = {
                    'id': idx,
                    'json': corrected_json,  
                    'Execution Time': execution_time,
                    'Actual Rows': plan.get('Actual Rows', 0),  # 从计划中提取行数
                    'Total Cost': plan.get('Total Cost', 0)   # 从计划中提取总成本
                }
                
                data.append(row)
                
            except json.JSONDecodeError as e:
                print(f"警告: 第{idx+1}行JSON解析失败: {e}")
                continue
            except Exception as e:
                print(f"警告: 第{idx+1}行处理失败: {e}")
                continue
    
    # 创建DataFrame并保存
    df = pd.DataFrame(data)
    df.to_csv(output_csv_path, index=False)
    print(f"成功转换 {len(data)} 条数据到 {output_csv_path}")
    return df

def main():
    """主函数：处理所有四个数据集"""
    
    # 输入和输出目录
    input_dir = "data/data-9-8"
    output_dir = "data/converted"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 数据集列表
    datasets = [
        ("hybench5.txt", "hybench5.csv"),
        ("hybench10.txt", "hybench10.csv"), 
        ("TPCH-5.txt", "TPCH-5.csv"),
        ("TPCH-10.txt", "TPCH-10.csv")
    ]
    
    # 处理每个数据集
    for input_file, output_file in datasets:
        input_path = os.path.join(input_dir, input_file)
        output_path = os.path.join(output_dir, output_file)
        
        if os.path.exists(input_path):
            df = convert_txt_to_csv(input_path, output_path)
            print(f"数据集 {input_file} 统计信息:")
            print(f"  总行数: {len(df)}")
            print(f"  执行时间范围: {df['Execution Time'].min():.2f} - {df['Execution Time'].max():.2f}")
            print(f"  平均执行时间: {df['Execution Time'].mean():.2f}")
            print("-" * 50)
        else:
            print(f"错误: 文件 {input_path} 不存在")

if __name__ == "__main__":
    main()
