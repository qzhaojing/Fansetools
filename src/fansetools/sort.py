# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 12:13:56 2025
sam sort
@author: Administrator
"""
#!/usr/bin/env python3
import sys
import os
import argparse
from pathlib import Path
from rich.console import Console
from .utils.rich_help import CustomHelpFormatter
from .utils.path_utils import PathProcessor
from collections import defaultdict
import tempfile
import heapq
    
def parse_sam_header(line):
    """解析SAM头部，返回分类后的头部行字典"""
    header_types = {
        '@HD': 'version',
        '@SQ': 'reference',
        '@RG': 'read_group',
        '@PG': 'program',
        '@CO': 'comment'
    }
    header_dict = defaultdict(list)
    if line.startswith('@'):
        fields = line.strip().split('\t')
        key = fields[0]
        header_dict[header_types.get(key, 'other')].append(line)
    return header_dict


def parse_sam_record(line):
    """解析单条SAM记录，返回结构化数据"""
    fields = line.strip().split('\t')
    record = {
        'qname': fields[0],     # Read名称
        'flag': int(fields[1]),  # SAM flag
        'rname': fields[2],     # 参考序列名
        'pos': int(fields[3]),  # 比对起始位置 (1-based)
        'mapq': int(fields[4]),  # 比对质量
        'cigar': fields[5],     # CIGAR字符串
        'rnext': fields[6],     # 配对read的参考序列
        'pnext': int(fields[7]),  # 配对read的位置
        'tlen': int(fields[8]),  # 模板长度
        'seq': fields[9],       # 序列
        'qual': fields[10],     # 质量值
        'tags': fields[11:]     # 可选标签
    }
    return record

def coord_sort_key(record):
    """生成染色体坐标排序键：参考序列名 → 位置 → 方向"""
    # 处理未比对到参考序列的记录
    rname = record['rname'] if record['rname'] != '*' else 'zzz_unmapped'
    pos = record['pos'] if record['pos'] > 0 else 2**31
    # 根据FLAG判断方向 (0x10表示反向互补)
    strand = 1 if (record['flag'] & 0x10) else 0
    return (rname, pos, strand)


def name_sort_key(record):
    """生成read名称排序键"""
    return record['qname']

def write_chunk_to_file(records, file_obj):
    """将记录块写入临时文件"""
    for record in records:
        fields = [
            record['qname'],
            str(record['flag']),
            record['rname'],
            str(record['pos']),
            str(record['mapq']),
            record['cigar'],
            record['rnext'],
            str(record['pnext']),
            str(record['tlen']),
            record['seq'],
            record['qual']
        ] + record['tags']
        file_obj.write("\t".join(fields) + "\n")

def merge_sorted_files(file_paths, output_file, sort_by):
    """多路归并排序核心算法"""

    
    # 初始化文件迭代器
    readers = []
    for path in file_paths:
        reader = open(path, 'r')
        try:
            first_line = next(reader)
            record = parse_sam_record(first_line)
            readers.append((record, reader))
        except StopIteration:
            reader.close()
    
    # 构建堆
    heap = []
    for idx, (record, reader) in enumerate(readers):
        key_func = coord_sort_key if sort_by == 'coord' else name_sort_key
        heapq.heappush(heap, (key_func(record), idx, record, reader))
    
    # 归并排序
    while heap:
        _, idx, record, reader = heapq.heappop(heap)
        
        # 写入当前记录
        fields = [
            record['qname'],
            str(record['flag']),
            record['rname'],
            str(record['pos']),
            str(record['mapq']),
            record['cigar'],
            record['rnext'],
            str(record['pnext']),
            str(record['tlen']),
            record['seq'],
            record['qual']
        ] + record['tags']
        output_file.write("\t".join(fields) + "\n")
        
        # 读取下一条记录
        try:
            next_line = next(reader)
            new_record = parse_sam_record(next_line)
            key_func = coord_sort_key if sort_by == 'coord' else name_sort_key
            heapq.heappush(heap, (key_func(new_record), idx, new_record, reader))
        except StopIteration:
            reader.close()
            
def sort_sam(input_sam, output_sam, sort_by='coord'):
    """主排序函数，支持内存与外存混合排序"""
    header_dict = defaultdict(list)
    records = []
    chunk_size = 1000000  # 每100万条记录分块处理
    temp_files = []
    
    # 第一遍：读取并分块排序
    with open(input_sam, 'r') as f:
        for line in f:
            if line.startswith('@'):
                header_type = parse_sam_header(line)
                for k, v in header_type.items():
                    header_dict[k].extend(v)
                continue

            record = parse_sam_record(line)
            records.append(record)

            if len(records) >= chunk_size:
                sort_key = coord_sort_key if sort_by == 'coord' else name_sort_key
                records.sort(key=sort_key)
                
                # 保存到临时文件（关键修复点）
                temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
                write_chunk_to_file(records, temp_file)
                temp_files.append(temp_file.name)
                temp_file.close()
                
                records = []  # 清空内存

        # 处理剩余记录
        if records:
            sort_key = coord_sort_key if sort_by == 'coord' else name_sort_key
            records.sort(key=sort_key)
            temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
            write_chunk_to_file(records, temp_file)
            temp_files.append(temp_file.name)
            temp_file.close()

# 第二遍：多路归并
    with open(output_sam, 'w') as out_f:
        # 写入头部
        for key in ['version', 'reference', 'read_group', 'program', 'comment', 'other']:
            for line in header_dict.get(key, []):
                out_f.write(line)
        
        # 多路归并排序
        if temp_files:
            merge_sorted_files(temp_files, out_f, sort_by)
            
        # 清理临时文件
        for f in temp_files:
            os.unlink(f)

def run_sort_command(args):
    """处理sort子命令"""
    console = Console(force_terminal=True)
    processor = PathProcessor()
    
    # 1. 解析输入文件
    try:
        input_files = processor.parse_input_paths(args.input, ['.sam'])
    except Exception as e:
        console.print(f"[bold red]错误: 解析输入文件失败 - {e}[/bold red]")
        sys.exit(1)

    if not input_files:
        console.print(f"[bold red]错误: 未找到有效的输入文件: {args.input}[/bold red]")
        sys.exit(1)

    sort_by = 'coord' if args.coord_sort else 'name'
    
    # 2. 处理输出逻辑
    output_path = Path(args.output)
    
    if len(input_files) > 1:
        # 批量模式
        if output_path.suffix: 
             # 如果看起来像文件（有后缀），则报错，因为批量输出需要目录
             console.print(f"[bold red]错误: 批量处理 {len(input_files)} 个文件时，输出路径必须是目录: {args.output}[/bold red]")
             sys.exit(1)
        
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
            
        console.print(f"检测到批量模式，将处理 {len(input_files)} 个文件，输出到: {output_path}")

        for infile in input_files:
            outfile_name = infile.stem + ".sorted.sam"
            outfile = output_path / outfile_name
            
            console.print(f"正在排序 ({input_files.index(infile) + 1}/{len(input_files)}): [bold green]{infile.name}[/bold green] -> [bold green]{outfile.name}[/bold green]...")
            try:
                sort_sam(str(infile), str(outfile), sort_by=sort_by)
                console.print(f"[bold green]完成[/bold green]")
            except Exception as e:
                console.print(f"[bold red]失败[/bold red] {infile.name}: {e}")
                
    else:
        # 单文件模式
        infile = input_files[0]
        final_output = output_path
        
        # 如果输出路径是现存目录，则拼接文件名
        if output_path.is_dir():
             final_output = output_path / (infile.stem + ".sorted.sam")
        
        console.print(f"开始排序 [bold green]{infile}[/bold green] -> [bold green]{final_output}[/bold green]...")
        try:
            sort_sam(str(infile), str(final_output), sort_by=sort_by)
            console.print(f"排序完成! 输出文件: [bold]{final_output}[/bold]")
        except Exception as e:
            console.print(f"[bold red]错误[/bold red]: {e}")
            sys.exit(1)

def add_sort_subparser(subparsers):
    """添加sort子命令解析器"""
    sort_parser = subparsers.add_parser(
        'sort',
        help='SAM文件排序',
        description='对SAM文件进行排序，支持按坐标或名称排序。支持通配符批量处理。',
        formatter_class=CustomHelpFormatter
    )
    
    sort_parser.add_argument(
        '-i', '--input', required=True, help='输入SAM文件路径 (支持通配符 *.sam)')
    sort_parser.add_argument(
        '-o', '--output', required=True, help='输出SAM文件路径或目录')
    
    # 互斥参数：按坐标排序或按名称排序
    sort_group = sort_parser.add_mutually_exclusive_group(required=True)
    sort_group.add_argument(
        '--coord', dest='coord_sort', action='store_true', 
        help='按坐标排序（染色体→位置→方向）')
    sort_group.add_argument(
        '--name', dest='name_sort', action='store_true', 
        help='按read名称排序')
    
    sort_parser.set_defaults(func=run_sort_command)


    



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAM文件纯Python排序工具")
    parser.add_argument("-i", "--input", required=True, help="输入SAM文件")
    parser.add_argument("-o", "--output", required=True, help="输出SAM文件")
    parser.add_argument("-n", "--name-sort",
                        action='store_true', help="按read名称排序 (默认按坐标)")
    args = parser.parse_args()

    sort_type = 'name' if args.name_sort else 'coord'
    sort_sam(args.input, args.output, sort_by=sort_type)
    print(f"排序完成! 输出文件: {args.output}")

# input_sam = r'G:\verysync_zhaojing\承启-资料\20250702-试剂盒破解\赵晶_2110471412_测序结果\71114538972_S891_L001_R1_001.fastq\71114538972_S891_L001_R1_001-f-f-f_indel0.fanse3\71114538972_S891_L001_R1_001-f-f-f.sam'
# output_sam = r'G:\verysync_zhaojing\承启-资料\20250702-试剂盒破解\赵晶_2110471412_测序结果\71114538972_S891_L001_R1_001.fastq\71114538972_S891_L001_R1_001-f-f-f_indel0.fanse3\71114538972_S891_L001_R1_001-f-f-f.sorted.sam'
# sort_sam(input_sam, output_sam,)
# 测试10GB SAM文件处理
    run =0
    if run:
        sort_sam(
            input_sam=r"\\fs2\d\data\zhaoJing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\0825-S10_DELL10\species_168sp_188fa_combined_genomes-S10-dell10_08266.sam",
            output_sam=r"\\fs2\d\data\zhaoJing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\0825-S10_DELL10\species_168sp_188fa_combined_genomes-S10-dell10_08266.sorted.sam",
            sort_by='coord',
            # chunk_size=5_000_000  # 每500万条分块
        )