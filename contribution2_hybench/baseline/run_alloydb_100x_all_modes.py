#!/usr/bin/env python3
"""
依次以三种扫描模式运行 hybench 100x 基准测试
扫描模式：row, column, mixed
"""

import subprocess
import sys
import os
import time
from datetime import datetime

def run_mode(scan_mode: str, script_path: str = "run_alloydb.py"):
    """
    运行指定扫描模式的基准测试
    
    Args:
        scan_mode: 扫描模式 ('row', 'column', 'mixed')
        script_path: run_alloydb.py 的路径
    """
    print(f"\n{'='*80}")
    print(f"开始运行模式: {scan_mode.upper()}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    # 构建命令
    cmd = [
        sys.executable,
        script_path,
        "--db", "100x",
        "--scan-mode", scan_mode
    ]
    
    try:
        # 获取脚本所在目录作为工作目录
        script_dir = os.path.dirname(os.path.abspath(script_path))
        if not script_dir:
            script_dir = os.getcwd()
        
        # 运行命令，实时输出到控制台
        result = subprocess.run(
            cmd,
            check=True,
            cwd=script_dir,
        )
        print(f"\n{'='*80}")
        print(f"模式 {scan_mode.upper()} 运行完成")
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n{'='*80}")
        print(f"错误: 模式 {scan_mode.upper()} 运行失败")
        print(f"返回码: {e.returncode}")
        print(f"{'='*80}\n")
        return False
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print(f"用户中断: 模式 {scan_mode.upper()} 被取消")
        print(f"{'='*80}\n")
        return False


def main():
    """主函数：依次运行三种扫描模式"""
    # 确定脚本路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "run_alloydb.py")
    
    if not os.path.exists(script_path):
        print(f"错误: 找不到脚本文件 {script_path}")
        sys.exit(1)
    
    # 三种扫描模式
    scan_modes = ["row", "column", "mixed"]
    
    # 记录开始时间
    overall_start_time = time.time()
    print(f"\n{'='*80}")
    print(f"开始运行 hybench 100x 基准测试 - 所有模式")
    print(f"总开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"模式列表: {', '.join(scan_modes)}")
    print(f"{'='*80}\n")
    
    # 记录每个模式的运行结果
    results = {}
    
    # 依次运行每种模式
    for i, mode in enumerate(scan_modes, 1):
        mode_start_time = time.time()
        print(f"\n进度: [{i}/{len(scan_modes)}] 准备运行模式: {mode}")
        
        success = run_mode(mode, script_path)
        mode_elapsed = time.time() - mode_start_time
        
        results[mode] = {
            'success': success,
            'elapsed_time': mode_elapsed
        }
        
        if not success:
            print(f"警告: 模式 {mode} 运行失败，但继续运行下一个模式...")
        
        # 如果不是最后一个模式，稍作停顿
        if i < len(scan_modes):
            print(f"\n等待 5 秒后继续下一个模式...\n")
            time.sleep(5)
    
    # 输出总结
    overall_elapsed = time.time() - overall_start_time
    print(f"\n{'='*80}")
    print(f"所有模式运行完成")
    print(f"总结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {overall_elapsed / 3600:.2f} 小时 ({overall_elapsed / 60:.2f} 分钟)")
    print(f"\n运行结果总结:")
    print(f"{'='*80}")
    for mode, result in results.items():
        status = "✓ 成功" if result['success'] else "✗ 失败"
        elapsed_hours = result['elapsed_time'] / 3600
        elapsed_mins = result['elapsed_time'] / 60
        print(f"  {mode:8s}: {status:6s} | 耗时: {elapsed_hours:6.2f} 小时 ({elapsed_mins:6.2f} 分钟)")
    print(f"{'='*80}\n")
    
    # 如果有失败的，返回非零退出码
    if not all(r['success'] for r in results.values()):
        print("警告: 部分模式运行失败")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断: 程序被取消")
        sys.exit(130)

