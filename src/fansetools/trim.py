#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import shutil
from pathlib import Path

def is_cutadapt_available():
    """检查系统是否安装了 cutadapt"""
    try:
        cutadapt_path = shutil.which("cutadapt")
        if cutadapt_path:
            return True
    #return shutil.which("cutadapt") is not None
    except:
        pass
        
    try:
        result = subprocess.run(
            ["cutadapt", "--version"], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        return result.returncode == 0
    except:
        pass
    
    # 方法2: 检查 Python 模块
    try:
        import cutadapt
        return True
    except ImportError:
        pass
    
    return False
    
def run_cutadapt_directly(args):
    """直接将所有参数传递给 cutadapt"""
    try:
        # 构建 cutadapt 命令
        cmd = ["cutadapt"]
        
        # 过滤掉我们自己的特殊参数（如 --cutadapt、--cutadapter）
        filtered_args = []
        skip_next = False
        for i, arg in enumerate(args.remaining_args):
            if skip_next:
                skip_next = False
                continue
                
            if arg in ['--cutadapt', '--cutadapter']:
                # 跳过工具选择参数
                continue
            else:
                filtered_args.append(arg)
        
        # 直接运行 cutadapt，不再检查帮助参数（因为帮助已在 handle_trim_command 中处理）
        if not filtered_args:
            print("错误：没有提供 cutadapt 参数")
            print("使用 'fanse trim --help' 查看帮助")
            return 1
        
        cmd.extend(filtered_args)
        
        # 显示执行的命令
        print(f"执行命令: {' '.join(cmd)}")
        
        # 运行 cutadapt
        result = subprocess.run(cmd, check=False)  # 不抛出异常，手动处理
        if result.returncode != 0:
            print(f"cutadapt 执行完成，退出代码: {result.returncode}")
            return result.returncode
        else:
            print("cutadapt 执行成功！")
            return 0
        
    except FileNotFoundError:
        print("错误：未找到 cutadapt，请确保已安装")
        print("安装命令: pip install cutadapt")
        return 1
    except Exception as e:
        print(f"运行 cutadapt 时发生错误: {e}")
        return 1

def cutadapter_fallback(args):
    """cutadapt 不可用时的回退函数（暂未实现）"""
    print("错误：cutadapt 未安装，且内置去接头功能暂未实现")
    print("请通过以下命令安装 cutadapt:")
    print("  pip install cutadapt")
    print()
    print("或者使用预编译的二进制版本:")
    print("  conda install cutadapt")
    return 1

def show_trim_help(cutadapt_available):
    """显示 trim 命令的帮助信息"""
    print("fanse trim - FASTQ 文件接头去除工具")
    print("=" * 60)
    print("此命令是 cutadapt 的包装器，所有参数直接传递给 cutadapt")
    print()
    print("基本用法:")
    print("  单端去接头: fanse trim -a ADAPTER -o output.fq.gz input.fq.gz")
    print("  双端去接头: fanse trim -a ADAPTER1 -A ADAPTER2 -o out1.fq.gz -p out2.fq.gz in1.fq.gz in2.fq.gz")
    print()
    
    if cutadapt_available:
        print("cutadapt 状态: ✓ 已安装")
        print()
        print("查看 cutadapt 完整帮助:")
        print("  fanse trim --help")
        print("  或直接运行: cutadapt --help")
    else:
        print("cutadapt 状态: ✗ 未安装")
        print("请先安装: pip install cutadapt")
    
    print()
    print("工具选择选项:")
    print("  --cutadapt     强制使用 cutadapt（默认）")
    print("  --cutadapter   强制使用内置去接头函数（暂未实现）")
    print()
    print("示例:")
    print("  fanse trim -a AGATCGGAAGAGC -o trimmed.fq.gz input.fq.gz")
    print("  fanse trim -a ADAPTER1 -A ADAPTER2 -o out1.fq -p out2.fq in1.fq in2.fq")

def handle_trim_command(args):
    """处理 trim 命令的主函数"""
    # 检查是否有帮助请求：通过 -h/--help 或没有参数时
    help_requested = args.help or not args.remaining_args
    
    # 检查 cutadapt 可用性
    cutadapt_available = is_cutadapt_available()
    
    # 如果有帮助请求，显示帮助信息
    if help_requested:
        show_trim_help(cutadapt_available)
        
        # 如果 cutadapt 可用，也显示 cutadapt 的帮助
        if cutadapt_available:
            print("\n" + "="*60)
            print("cutadapt 帮助信息:")
            print("-" * 60)
            try:
                # 使用 subprocess.run 显示 cutadapt 帮助
                result = subprocess.run(["cutadapt", "--help"], check=False)
            except Exception as e:
                print(f"无法显示 cutadapt 帮助: {e}")
        return 0
    
    # 决定使用哪个工具
    use_cutadapt = True
    
    # 检查参数中是否指定了工具（注意：现在 --cutadapter 在 remaining_args 中）
    if '--cutadapter' in args.remaining_args:
        use_cutadapt = False
    
    # 检查 cutadapt 是否可用
    if use_cutadapt and not cutadapt_available:
        print("警告：cutadapt 未安装")
        print("将尝试使用内置去接头函数（功能有限）")
        use_cutadapt = False
    
    # 执行去接头
    if use_cutadapt:
        return run_cutadapt_directly(args)
    else:
        return cutadapter_fallback(args)

def add_trim_subparser(subparsers):
    """添加 trim 子命令解析器"""
    parser = subparsers.add_parser(
        'trim',
        help='使用 cutadapt 去除 FASTQ 文件中的接头',
        description='cutadapt 包装器 - 所有参数直接传递给 cutadapt',
        add_help=False  # 禁用自动添加帮助参数
    )
    
    # 添加显式帮助选项
    parser.add_argument('-h', '--help', action='store_true',
                       help='显示此帮助信息并退出')
    
    # 添加一个参数来捕获所有剩余参数
    parser.add_argument('remaining_args', nargs='*', 
                       help='传递给 cutadapt 的参数')
    
    # 添加工具选择参数
    parser.add_argument('--cutadapt', action='store_true',
                       help='强制使用 cutadapt（默认）')
    parser.add_argument('--cutadapter', action='store_true',
                       help='强制使用内置去接头函数（暂未实现）')
    
    parser.set_defaults(func=handle_trim_command)
    return parser