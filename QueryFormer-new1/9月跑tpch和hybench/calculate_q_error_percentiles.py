#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计算所有数据集的q-error百分位数
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

def calculate_q_error_percentiles(preds, labels, percentiles=[25, 50, 75, 90, 95]):
    """
    计算q-error的百分位数
    
    Args:
        preds: 预测值数组
        labels: 真实值数组
        percentiles: 要计算的百分位数列表
    
    Returns:
        dict: 包含各百分位数的q-error值
    """
    # 计算q-error: max(pred/true, true/pred)
    q_errors = []
    for pred, true in zip(preds, labels):
        if true > 0 and pred > 0:
            q_error = max(pred / true, true / pred)
            q_errors.append(q_error)
        else:
            # 处理零值情况，设置为一个很大的值
            q_errors.append(1e6)
    
    q_errors = np.array(q_errors)
    q_errors = q_errors[np.isfinite(q_errors)]  # 移除无穷大值
    
    if len(q_errors) == 0:
        return {f'q_error_{p}th': float('inf') for p in percentiles}
    
    results = {}
    for p in percentiles:
        results[f'q_error_{p}th'] = np.percentile(q_errors, p)
    
    return results

def analyze_dataset_results(file_path, dataset_name):
    """
    分析单个数据集的结果
    
    Args:
        file_path: 结果文件路径
        dataset_name: 数据集名称
    
    Returns:
        dict: 包含统计信息的字典
    """
    print(f"\n分析数据集: {dataset_name}")
    print(f"文件路径: {file_path}")
    
    try:
        # 读取结果文件
        df = pd.read_csv(file_path)
        print(f"总样本数: {len(df)}")
        
        # 获取预测值和真实值
        # 处理预测值，它们可能是数组格式
        preds = []
        for pred in df['pred'].values:
            if isinstance(pred, str) and pred.startswith('[') and pred.endswith(']'):
                # 解析数组格式的预测值
                try:
                    pred_array = eval(pred)  # 将字符串转换为数组
                    preds.append(pred_array[0] if len(pred_array) > 0 else 0)
                except:
                    preds.append(0)
            else:
                preds.append(float(pred))
        
        preds = np.array(preds)
        labels = df['true'].values.astype(float)
        
        # 计算q-error百分位数
        q_error_stats = calculate_q_error_percentiles(preds, labels)
        
        # 计算其他统计信息
        errors = np.abs(preds - labels)
        relative_errors = errors / np.maximum(labels, 1e-8)  # 避免除零
        
        # 计算q-error的统计信息
        q_errors = []
        for pred, true in zip(preds, labels):
            if true > 0 and pred > 0:
                q_error = max(pred / true, true / pred)
                q_errors.append(q_error)
            else:
                q_errors.append(1e6)
        
        q_errors = np.array(q_errors)
        q_errors = q_errors[np.isfinite(q_errors)]
        
        stats = {
            'dataset': dataset_name,
            'total_samples': len(df),
            'mean_absolute_error': np.mean(errors),
            'median_absolute_error': np.median(errors),
            'mean_relative_error': np.mean(relative_errors),
            'median_relative_error': np.median(relative_errors),
            'max_error': np.max(errors),
            'min_error': np.min(errors),
            'mean_q_error': np.mean(q_errors),
            'median_q_error': np.median(q_errors),
            'max_q_error': np.max(q_errors),
            'min_q_error': np.min(q_errors),
            **q_error_stats
        }
        
        print(f"  平均绝对误差: {stats['mean_absolute_error']:.4f}")
        print(f"  中位数绝对误差: {stats['median_absolute_error']:.4f}")
        print(f"  平均相对误差: {stats['mean_relative_error']:.4f}")
        print(f"  平均Q-Error: {stats['mean_q_error']:.4f}")
        print(f"  中位数Q-Error: {stats['median_q_error']:.4f}")
        print(f"  Q-Error 25%: {stats['q_error_25th']:.4f}")
        print(f"  Q-Error 50%: {stats['q_error_50th']:.4f}")
        print(f"  Q-Error 75%: {stats['q_error_75th']:.4f}")
        print(f"  Q-Error 90%: {stats['q_error_90th']:.4f}")
        print(f"  Q-Error 95%: {stats['q_error_95th']:.4f}")
        
        return stats
        
    except Exception as e:
        print(f"  错误: {e}")
        return {
            'dataset': dataset_name,
            'error': str(e)
        }

def main():
    """主函数：分析所有数据集的结果"""
    
    # 数据集配置
    datasets = [
        ("hybench5", "all_val_results_hybench5.csv"),
        ("hybench10", "all_val_results_hybench10.csv"),
        ("TPCH-5", "all_val_results_TPCH5.csv"),
        ("TPCH-10", "all_val_results_TPCH10.csv")
    ]
    
    print("=" * 80)
    print("Q-Error 百分位数分析")
    print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    all_results = []
    
    for dataset_name, file_path in datasets:
        if os.path.exists(file_path):
            result = analyze_dataset_results(file_path, dataset_name)
            all_results.append(result)
        else:
            print(f"\n数据集: {dataset_name}")
            print(f"文件 {file_path} 不存在，跳过")
            all_results.append({
                'dataset': dataset_name,
                'error': f'File {file_path} not found'
            })
    
    # 创建结果DataFrame
    results_df = pd.DataFrame(all_results)
    
    # 保存结果到CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'q_error_analysis_{timestamp}.csv'
    results_df.to_csv(output_file, index=False)
    
    print(f"\n{'='*80}")
    print("分析完成")
    print(f"结果已保存到: {output_file}")
    print(f"{'='*80}")
    
    # 打印汇总表格
    print("\nQ-Error 百分位数汇总表:")
    print("-" * 100)
    
    # 只显示成功分析的结果
    success_results = results_df[~results_df['total_samples'].isna()]
    if len(success_results) > 0:
        # 创建汇总表
        summary_cols = ['dataset', 'total_samples', 'q_error_25th', 'q_error_50th', 
                       'q_error_75th', 'q_error_90th', 'q_error_95th', 'mean_q_error']
        summary_df = success_results[summary_cols].copy()
        
        # 重命名列以便显示
        summary_df.columns = ['数据集', '样本数', 'Q-Error 25%', 'Q-Error 50%', 
                             'Q-Error 75%', 'Q-Error 90%', 'Q-Error 95%', '平均Q-Error']
        
        print(summary_df.to_string(index=False, float_format='%.4f'))
        
        # 计算总体统计
        print(f"\n总体统计:")
        print(f"  成功分析的数据集数量: {len(success_results)}")
        print(f"  总样本数: {success_results['total_samples'].sum()}")
        
        # 计算各百分位数的平均值
        for p in [25, 50, 75, 90, 95]:
            col_name = f'q_error_{p}th'
            if col_name in success_results.columns:
                mean_val = success_results[col_name].mean()
                print(f"  平均 Q-Error {p}%: {mean_val:.4f}")
    else:
        print("没有成功分析的数据集")
    
    # 显示失败的结果
    failed_results = results_df[results_df['total_samples'].isna()]
    if len(failed_results) > 0:
        print(f"\n失败的数据集:")
        print(failed_results[['dataset', 'error']].to_string(index=False))

if __name__ == "__main__":
    main()
