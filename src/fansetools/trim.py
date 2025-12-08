#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import shutil
import argparse
from pathlib import Path
from rich.console import Console
from .utils.rich_help import CustomHelpFormatter, print_colored_text
from .cli import find_and_execute_binary

def get_binary_path(name):
    """
    获取二进制文件路径
    优先查找 src/fansetools/bin/windows/name.exe
    然后查找系统 PATH
    """
    # 1. 尝试使用 cli.py 中的逻辑查找内置 bin 目录
    # 由于 find_and_execute_binary 是直接执行，我们需要复用其查找逻辑
    # 这里简单重新实现查找逻辑
    package_dir = Path(__file__).parent
    bin_base = package_dir / "bin"
    
    if os.name == 'nt':
        platform_dir = 'windows'
        name_ext = f"{name}.exe"
    elif sys.platform == 'darwin':
        platform_dir = 'macos'
        name_ext = name
    else:
        platform_dir = 'linux'
        name_ext = name
        
    bin_path = bin_base / platform_dir / name_ext
    if bin_path.exists() and bin_path.is_file():
        return str(bin_path)
        
    # 2. 查找系统 PATH
    return shutil.which(name)

def auto_generate_output_name(input_file, suffix="_trimmed.fq.gz"):
    """
    根据输入文件名自动生成输出文件名
    """
    if not input_file:
        return None
    
    p = Path(input_file)
    stem = p.name
    # 处理常见的压缩后缀
    if stem.endswith('.gz'):
        stem = stem[:-3]
    if stem.endswith('.fastq'):
        stem = stem[:-6]
    elif stem.endswith('.fq'):
        stem = stem[:-3]
        
    return str(p.parent / (stem + suffix))

def run_fastp(args, fastp_path):
    """
    运行 fastp
    """
    cmd = [fastp_path]
    
    # 1. 处理输入输出文件
    # 单端
    if args.input:
        cmd.extend(["-i", args.input])
        if args.output:
            cmd.extend(["-o", args.output])
        else:
            out_name = auto_generate_output_name(args.input)
            cmd.extend(["-o", out_name])
            print(f"自动设置输出文件: {out_name}")
            
    # 双端
    elif args.r1 and args.r2:
        cmd.extend(["-i", args.r1, "-I", args.r2])
        if args.output and args.paired_output:
            cmd.extend(["-o", args.output, "-O", args.paired_output])
        else:
            out1 = auto_generate_output_name(args.r1, "_trimmed_R1.fq.gz")
            out2 = auto_generate_output_name(args.r2, "_trimmed_R2.fq.gz")
            cmd.extend(["-o", out1, "-O", out2])
            print(f"自动设置输出文件: {out1}, {out2}")
            
    # 2. 映射通用参数
    if args.threads:
        cmd.extend(["-w", str(args.threads)])
    else:
        # 默认线程数 4
        cmd.extend(["-w", "4"])
        
    if args.quality:
        cmd.extend(["-q", str(args.quality)])
        
    if args.length:
        cmd.extend(["-l", str(args.length)])
        
    # 3. 处理接头参数 (-a/-A)
    # fastp 自动识别接头，但如果用户指定了，我们需要传递
    # -a 在 fastp 中通常是 --adapter_sequence (对于 R1) 或 --adapter_fasta (如果文件存在)
    if args.adapter:
        if os.path.exists(args.adapter):
            # 这是一个文件，假定为 adapter fasta
            cmd.extend(["--adapter_fasta", args.adapter])
        else:
            # 这是一个序列 string
            cmd.extend(["--adapter_sequence", args.adapter])
            
    if args.adapter2:
        # fastp 的 R2 接头序列参数
        cmd.extend(["--adapter_sequence_r2", args.adapter2])
    
    # 生成 HTML/JSON 报告
    # 如果没有指定报告参数，我们自动生成
    has_report_args = any(x in args.remaining_args for x in ['-h', '--html', '-j', '--json'])
    if not has_report_args:
        report_base = "fastp"
        if args.input:
            report_base = Path(args.input).stem
        elif args.r1:
            report_base = Path(args.r1).stem
            
        html_report = f"{report_base}.html"
        json_report = f"{report_base}.json"
        cmd.extend(["-h", html_report, "-j", json_report])
        print(f"自动生成报告: {html_report}, {json_report}")

    # 4. 传递剩余的原生参数
    # 注意：如果 args.remaining_args 中有冲突的参数（如 -i, -o），fastp 可能会报错或覆盖
    # 用户应该小心使用
    if args.remaining_args:
        # 过滤掉 --fastp, --cutadapt 等
        cleaned_args = [a for a in args.remaining_args if a not in ['--fastp', '--cutadapt']]
        cmd.extend(cleaned_args)
        
    # 显示带颜色的执行命令
    console = Console()
    cmd_str = ' '.join(cmd)
    console.print(f"[bold green]执行命令:[/bold green] [yellow]{cmd_str}[/yellow]")
    
    return subprocess.call(cmd)

def run_cutadapt(args, cutadapt_path):
    """
    运行 cutadapt
    """
    # 如果 cutadapt_path 是 "cutadapt" (系统命令) 或 具体路径
    cmd = [cutadapt_path] if cutadapt_path else ["cutadapt"]
    
    # 1. 处理输入输出
    # cutadapt 的输出通常通过 -o 指定 R1，-p 指定 R2
    
    # 双端
    if args.r1 and args.r2:
        if args.output:
            cmd.extend(["-o", args.output])
        else:
            out1 = auto_generate_output_name(args.r1, "_trimmed_R1.fq.gz")
            cmd.extend(["-o", out1])
            print(f"自动设置输出文件: {out1}")
            
        if args.paired_output:
            cmd.extend(["-p", args.paired_output])
        else:
            out2 = auto_generate_output_name(args.r2, "_trimmed_R2.fq.gz")
            cmd.extend(["-p", out2])
            print(f"自动设置输出文件: {out2}")
            
        # 输入文件放在最后，但在参数列表中，我们先构建选项
        input_files = [args.r1, args.r2]
        
    # 单端
    elif args.input:
        if args.output:
            cmd.extend(["-o", args.output])
        else:
            out_name = auto_generate_output_name(args.input)
            cmd.extend(["-o", out_name])
            print(f"自动设置输出文件: {out_name}")
        input_files = [args.input]
        
    else:
        # 如果没有通过 -i/-1/-2 指定输入，可能在 remaining_args 中
        input_files = []

    # 2. 映射通用参数
    if args.threads:
        cmd.extend(["-j", str(args.threads)]) # cutadapt 使用 -j 指定核心数
    else:
        cmd.extend(["-j", "4"])
        
    if args.quality:
        cmd.extend(["-q", str(args.quality)])
        
    if args.length:
        cmd.extend(["-m", str(args.length)])
        
    # 3. 处理接头参数 (-a/-A)
    if args.adapter:
        # cutadapt -a 用于 3' 接头
        cmd.extend(["-a", args.adapter])
    
    if args.adapter2:
        # cutadapt -A 用于 R2 3' 接头
        cmd.extend(["-A", args.adapter2])

    # 4. 传递剩余参数
    if args.remaining_args:
        cleaned_args = [a for a in args.remaining_args if a not in ['--fastp', '--cutadapt']]
        cmd.extend(cleaned_args)
        
    # 输入文件必须放在最后
    cmd.extend(input_files)
    
    # 显示带颜色的执行命令
    console = Console()
    cmd_str = ' '.join(cmd)
    console.print(f"[bold green]执行命令:[/bold green] [yellow]{cmd_str}[/yellow]")
    
    return subprocess.call(cmd)

def print_help():
    """打印自定义帮助信息"""
    help_text = """
[bold cyan]fanse trim[/bold cyan] - 自动接头去除与质控工具

[bold yellow]简介:[/bold yellow]
  集成 [green]fastp[/green] 和 [green]cutadapt[/green] 的通用接头去除工具。
  默认优先使用 fastp（速度快，功能全），如果未安装则尝试使用 cutadapt。
  
  支持 "无脑操作"：只需指定输入文件，自动推断输出文件名、检测接头、生成报告。

[bold yellow]用法:[/bold yellow]
  fanse trim [选项] [输入文件]

[bold yellow]常用选项:[/bold yellow]
  [bold cyan]-i, --input[/bold cyan] FILE        输入文件 (单端)
  [bold cyan]-1, --r1[/bold cyan] FILE           输入文件 R1 (双端)
  [bold cyan]-2, --r2[/bold cyan] FILE           输入文件 R2 (双端)
  [bold cyan]-o, --output[/bold cyan] FILE       输出文件 (单端 或 R1)
  [bold cyan]-O, --paired-output[/bold cyan] FILE 输出文件 R2 (仅双端)
  [bold cyan]-p, --threads[/bold cyan] INT       线程数 (默认: 4)
  [bold cyan]-q, --quality[/bold cyan] INT       质量阈值 (Phred score)
  [bold cyan]-l, --length[/bold cyan] INT        最小长度过滤
  
  [bold cyan]-a, --adapter[/bold cyan] STR/FILE  正向接头序列 或 接头fasta文件
  [bold cyan]-A, --adapter2[/bold cyan] STR      反向接头序列 (双端)

[bold yellow]工具选择:[/bold yellow]
  [bold cyan]--fastp[/bold cyan]               强制使用 fastp
  [bold cyan]--cutadapt[/bold cyan]            强制使用 cutadapt

[bold yellow]原生帮助与高级参数:[/bold yellow]
  如果您想查看工具的原生帮助或使用更多高级参数：
  
  [green]fanse trim --help --fastp[/green]     查看 fastp 原生帮助 (彩色)
  [green]fanse trim --help --cutadapt[/green]  查看 cutadapt 原生帮助 (彩色)
  
  任何未被 fanse 识别的参数都将直接传递给底层工具。
  例如: [italic]fanse trim -i in.fq --umi --umi_loc=read1[/italic] (传递给 fastp)

[bold yellow]示例:[/bold yellow]
  1. 最简单的单端处理 (自动命名输出，自动检测接头):
     [green]fanse trim -i input.fq.gz[/green]

  2. 双端处理,并指定线程:
     [green]fanse trim -1 R1.fq.gz -2 R2.fq.gz -p 8[/green]

  3. 指定接头序列:
     [green]fanse trim -i input.fq.gz -a AGATCGGAAGAG[/green]
     
  4. 使用 cutadapt 并指定特定参数（默认fastp）:
     [green]fanse trim --cutadapt -i input.fq.gz -a AGATCGGAAGAG --nextseq-trim=20[/green]
"""
    from rich.console import Console
    console = Console()
    console.print(help_text)

def handle_trim_command_wrapper(args):
    """
    包装器：处理参数解析，决定调用哪个工具
    """
    # 0. 特殊处理：如果用户请求 --help 并且带有 --fastp 或 --cutadapt
    # Argparse 已经在 add_trim_subparser 中处理了 -h/--help，如果是这种情况，func 不会被调用
    # 但如果 add_help=False，我们需要自己处理
    
    # 检查 args 中是否有 help
    if args.help:
        # 检查是否请求原生帮助
        if args.fastp:
            fastp_path = get_binary_path("fastp")
            if fastp_path:
                print(f"调用 fastp 原生帮助 ({fastp_path}):\n")
                try:
                    # 尝试捕获 stdout 和 stderr
                    # 在 Windows 上 findstr 可能需要 shell=True，但这里直接运行
                    res = subprocess.run([fastp_path, "--help"], capture_output=True, text=True)
                    output = res.stdout + res.stderr
                    print_colored_text(output)
                except Exception as e:
                    print(f"无法获取 fastp 帮助: {e}")
            else:
                print("未找到 fastp。")
            return 0
        
        if args.cutadapt:
            cutadapt_path = get_binary_path("cutadapt")
            # cutadapt 可能是系统命令
            if not cutadapt_path and shutil.which("cutadapt"):
                cutadapt_path = "cutadapt"
            
            if cutadapt_path:
                print(f"调用 cutadapt 原生帮助 ({cutadapt_path}):\n")
                try:
                    res = subprocess.run([cutadapt_path, "--help"], capture_output=True, text=True)
                    output = res.stdout + res.stderr
                    print_colored_text(output)
                except Exception as e:
                    print(f"无法获取 cutadapt 帮助: {e}")
            else:
                print("未找到 cutadapt。")
            return 0
        
        # 默认显示我们的增强帮助
        print_help()
        return 0

    # 1. 自动检测工具
    fastp_path = get_binary_path("fastp")
    cutadapt_path = get_binary_path("cutadapt")
    if not cutadapt_path and shutil.which("cutadapt"):
        cutadapt_path = "cutadapt"
        
    tool_to_use = None
    
    if args.fastp:
        if fastp_path:
            tool_to_use = "fastp"
        else:
            print("错误: 指定了 --fastp 但未找到 fastp 可执行文件。")
            return 1
    elif args.cutadapt:
        if cutadapt_path:
            tool_to_use = "cutadapt"
        else:
            print("错误: 指定了 --cutadapt 但未找到 cutadapt。")
            return 1
    else:
        # 默认优先级: fastp > cutadapt
        if fastp_path:
            tool_to_use = "fastp"
        elif cutadapt_path:
            tool_to_use = "cutadapt"
        else:
            print("错误: 未找到 fastp 或 cutadapt。请安装其中之一。")
            return 1
            
    # 2. 验证输入文件 (如果没有提供 -i/-1/-2，尝试从 remaining_args 获取?)
    # 为了支持 "fanse trim input.fq.gz"，我们需要检查 remaining_args
    # 但 argparse 的 remaining_args 可能包含文件名
    if not (args.input or args.r1):
        # 尝试从 remaining_args 解析位置参数
        # 假设剩下的参数中，不以 - 开头的是文件
        files = [x for x in args.remaining_args if not x.startswith('-')]
        if len(files) == 1:
            args.input = files[0]
            print(f"检测到输入文件: {args.input}")
            # 从 remaining_args 中移除该文件，避免重复传递
            args.remaining_args.remove(files[0])
        elif len(files) == 2:
            args.r1 = files[0]
            args.r2 = files[1]
            print(f"检测到双端输入文件: {args.r1}, {args.r2}")
            args.remaining_args.remove(files[0])
            args.remaining_args.remove(files[1])
            
    if not (args.input or (args.r1 and args.r2)):
        # 如果还是没有输入文件，且不是仅查看版本等操作
        print("错误: 请指定输入文件 (-i 或 -1/-2)。")
        print_help()
        return 1

    # 3. 执行工具
    if tool_to_use == "fastp":
        return run_fastp(args, fastp_path)
    elif tool_to_use == "cutadapt":
        return run_cutadapt(args, cutadapt_path)
        
    return 1

def add_trim_subparser(subparsers):
    parser = subparsers.add_parser(
        'trim',
        help='去除 FASTQ 接头 (支持 fastp/cutadapt)',
        description='自动选择 fastp 或 cutadapt 进行接头去除和质控',
        formatter_class=CustomHelpFormatter,
        add_help=False # 我们自己处理 help
    )
    
    # Common Args
    parser.add_argument('-h', '--help', action='store_true', help='显示帮助信息')
    
    group = parser.add_argument_group('常用选项')
    group.add_argument('-i', '--input', metavar='FILE', help='输入文件 (单端)')
    group.add_argument('-1', '--r1', metavar='FILE', help='输入文件 R1')
    group.add_argument('-2', '--r2', metavar='FILE', help='输入文件 R2')
    
    group.add_argument('-o', '--output', metavar='FILE', help='输出文件 (R1)')
    group.add_argument('-O', '--paired-output', metavar='FILE', help='输出文件 (R2)')
    
    group.add_argument('-a', '--adapter', metavar='STR/FILE', help='正向接头/文件')
    group.add_argument('-A', '--adapter2', metavar='STR', help='反向接头 (双端)')
    
    group.add_argument('-p', '--threads', type=int, help='线程数')
    group.add_argument('-q', '--quality', type=int, help='质量阈值')
    group.add_argument('-l', '--length', type=int, help='最小长度')
    
    tool_group = parser.add_argument_group('工具选择')
    tool_group.add_argument('--fastp', action='store_true', help='强制使用 fastp')
    tool_group.add_argument('--cutadapt', action='store_true', help='强制使用 cutadapt')
    
    # Catch-all
    parser.add_argument('remaining_args', nargs=argparse.REMAINDER, help='输入文件和其他参数')
    
    parser.set_defaults(func=handle_trim_command_wrapper)
    return parser
