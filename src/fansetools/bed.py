#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FANSe3 to BED format converter
Usage: python bed.py input.fanse3 [-o output.bed] [-n 10000]
zhongjiayong，zhaojing
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FANSe3 to BED format converter for fansetools
"""

import os
import argparse
import sys
from pathlib import Path
from .utils.rich_help import CustomHelpFormatter
from .utils.path_utils import PathProcessor
from rich.console import Console
from tqdm import tqdm

def parse_fanse_line(line1, line2):
    """Parse two-line FANSe3 record into BED entries"""
    fields1 = line1.strip().split('\t')
    fields2 = line2.strip().split('\t')
    
    # Input validation
    if len(fields1) < 2 or len(fields2) < 4:
        raise ValueError(f"Invalid FANSe3 lines:\n{line1}\n{line2}")
    
    tag_name = '#' + fields1[0]
    sequence = fields1[1]
    
    # Process multi-mapping results
    strands = fields2[0].split(',')
    chroms = fields2[1].split(',')
    starts = [s.rstrip('\n') for s in fields2[3].split(',')]
    
    bed_entries = []
    for strand, chrom, start in zip(strands, chroms, starts):
        end = str(int(start) + len(sequence))
        strand = '+' if strand == 'F' else '-'
        bed_entries.append(f"{chrom}\t{start}\t{end}\t{tag_name}\t0\t{strand}\n")
    
    return bed_entries

def process_file(input_path, output_path, max_reads=None, console=None):
    """Convert FANSe3 file to BED format"""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    with open(input_path, 'r') as f_in, open(output_path, 'w') as f_out:
        # Estimate total lines for progress bar
        try:
            total_lines = sum(1 for _ in f_in) // 2
        except Exception:
            total_lines = 0
        f_in.seek(0)
        
        processed = 0
        pbar = tqdm(total=total_lines, desc=f"Converting {os.path.basename(input_path)}", disable=console is not None)
        
        while True:
            line1 = f_in.readline()
            line2 = f_in.readline()
            
            if not line1 or not line2:
                break
            
            try:
                bed_entries = parse_fanse_line(line1, line2)
                f_out.writelines(bed_entries)
                processed += len(bed_entries)
                if not console:
                    pbar.update(1)
                elif processed % 10000 == 0:
                     # For rich console, we might just print progress occasionally or use a Progress context manager
                     # But for simplicity, we'll rely on the caller to handle rich progress or just silent processing here
                     pass
                
                if max_reads and processed >= max_reads:
                    break
                    
            except ValueError as e:
                if console:
                     console.print(f"[yellow]Skipping invalid record: {str(e)}[/yellow]")
                else:
                     tqdm.write(f"Skipping invalid record: {str(e)}")
                continue
        
        if not console:
            pbar.close()

def fanse2bed(args):
    """Handle the bed subcommand"""
    console = Console(force_terminal=True)
    processor = PathProcessor()
    
    # 1. 解析输入文件
    try:
        input_files = processor.parse_input_paths(args.input, ['.fanse3', '.fanse'])
    except Exception as e:
        console.print(f"[bold red]错误: 解析输入文件失败 - {e}[/bold red]")
        sys.exit(1)

    if not input_files:
        console.print(f"[bold red]错误: 未找到有效的输入文件: {args.input}[/bold red]")
        sys.exit(1)
        
    # 2. 处理输出
    output_path = Path(args.output) if args.output else None
    
    # 批量模式检查
    if len(input_files) > 1:
        if output_path and output_path.suffix:
             console.print(f"[bold red]错误: 批量处理 {len(input_files)} 个文件时，输出路径必须是目录 (如果指定)[/bold red]")
             sys.exit(1)
        
        if output_path and not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
        
        console.print(f"检测到批量模式，将处理 {len(input_files)} 个文件...")
        
        for infile in input_files:
            console.print(f"[blue]处理任务: {infile.name}[/blue]")
            outfile_name = infile.stem + ".bed"
            if output_path:
                outfile = output_path / outfile_name
            else:
                outfile = infile.with_suffix('.bed')
            
            try:
                process_file(str(infile), str(outfile), args.max_reads, console)
            except Exception as e:
                console.print(f"[bold red]处理 {infile.name} 失败: {e}[/bold red]")
            
    else:
        # 单文件模式
        infile = input_files[0]
        if output_path:
            if output_path.is_dir():
                 outfile = output_path / (infile.stem + ".bed")
            else:
                 outfile = output_path
        else:
            outfile = infile.with_suffix('.bed')
        
        try:
            process_file(str(infile), str(outfile), args.max_reads, console)
            console.print(f"[bold green]完成! 输出文件: {outfile}[/bold green]")
        except Exception as e:
            console.print(f"[bold red]错误: {e}[/bold red]")
            sys.exit(1)

def add_bed_subparser(subparsers):
    """Add bed subcommand to the main parser"""
    parser = subparsers.add_parser(
        'bed', 
        help='Convert FANSe3 results to BED format',
        formatter_class=CustomHelpFormatter
    )
    parser.add_argument('-i', '--input', required=True, 
                       help='Input FANSe3 file or directory (supports wildcards)')
    parser.add_argument('-o', '--output', 
                       help='Output BED file path (or directory for batch mode)')
    parser.add_argument('-n', '--max-reads', type=int, 
                       help='Max reads to process')
    parser.set_defaults(func=fanse2bed)