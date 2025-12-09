# -*- coding: utf-8 -*-
import argparse
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from .utils.path_utils import PathProcessor

def handle_path_command(args):
    """
    处理 path 命令：测试路径解析与通配符匹配
    """
    console = Console(force_terminal=True)
    
    input_str = args.input
    output_dir = args.output
    
    # 初始化路径处理器
    processor = PathProcessor()
    
    console.print(Panel(
        f"[bold]正在解析输入路径:[/bold] [yellow]{input_str}[/yellow]",
        title="路径解析测试",
        border_style="cyan"
    ))
    
    # 1. 解析输入路径
    try:
        # 这里不限制扩展名，或者根据用户需要可以添加参数限制
        # 为了通用性，暂不限制扩展名，显示所有匹配文件
        valid_exts = None 
        # 如果用户想模拟特定模块（如count），可能需要指定后缀，这里暂且列出所有文件
        
        # 捕获解析过程中的日志/警告可能比较困难，除非我们传入自定义logger
        # 但PathProcessor会打印到标准输出或logger，这里我们主要关注结果
        
        input_paths = processor.parse_input_paths(input_str, valid_extensions=valid_exts)
        
        # 统计信息
        total_files = len(input_paths)
        
        if total_files == 0:
            console.print("[bold red]❌ 未找到任何匹配的文件！[/bold red]")
            console.print("请检查：")
            console.print("  1. 路径拼写是否正确")
            console.print("  2. 通配符模式是否匹配")
            console.print("  3. 目录是否存在且有权限访问")
            console.print("  4. (如果是网络路径) 是否已正确连接")
            return 1
            
        console.print(f"✅ [bold green]成功找到 {total_files} 个文件[/bold green]")
        
        # 2. 生成输出映射预览
        path_map = {}
        if output_dir:
            # 模拟生成输出文件名
            # 使用一个通用的后缀，仅作演示
            path_map = processor.generate_output_mapping(input_paths, output_dir, default_suffix=".output_test")
        
        # 3. 展示结果（前5个 + 后5个，或者全部如果很少）
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("序号", justify="right", style="dim")
        table.add_column("输入文件路径", style="green")
        
        if output_dir:
            table.add_column("预期的输出路径", style="blue")
            
        # 决定显示的条目
        display_items = []
        if total_files <= 10:
            display_items = list(enumerate(input_paths, 1))
        else:
            # 前5个
            display_items.extend(list(enumerate(input_paths[:5], 1)))
            # 分隔符（用None标记）
            display_items.append(None)
            # 后3个
            display_items.extend(list(enumerate(input_paths[-3:], total_files - 2)))
            
        for item in display_items:
            if item is None:
                if output_dir:
                    table.add_row("...", "...", "...")
                else:
                    table.add_row("...", "...")
                continue
                
            idx, p = item
            if output_dir:
                out_p = path_map.get(p, "N/A")
                table.add_row(str(idx), str(p), str(out_p))
            else:
                table.add_row(str(idx), str(p))
                
        console.print(table)
        
        if output_dir:
             console.print(f"\n[dim]输出目录设置为: {output_dir}[/dim]")
             if total_files > 1 and not Path(output_dir).is_dir() and not Path(output_dir).exists():
                  console.print("[yellow]注意: 对于多个输入文件，输出路径应当是一个目录。[/yellow]")

    except Exception as e:
        console.print(f"[bold red]解析过程发生错误:[/bold red] {str(e)}")
        return 1
        
    return 0

def add_path_subparser(subparsers):
    parser = subparsers.add_parser(
        'path',
        help='测试路径解析与通配符匹配',
        description='测试路径解析功能，帮助调试通配符和文件查找问题'
    )
    parser.add_argument('-i', '--input', required=True, help='输入路径模式 (支持通配符 * ?)')
    parser.add_argument('-o', '--output', help='(可选) 测试输出目录，用于预览输出文件路径')
    parser.set_defaults(func=handle_path_command)
    return parser
