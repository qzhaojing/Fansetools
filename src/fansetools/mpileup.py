# -*- coding: utf-8 -*-
"""
mpileup.py - FANSe3转mpileup格式模块,暂时无indel处理功能
该实现将FANSe3比对结果转换为标准的mpileup格式
支持多参考序列处理
可以设置最小覆盖深度阈值
可以设置统一的碱基质量分数
输出格式兼容samtools mpileup
"""

import argparse
from .utils.rich_help import CustomHelpFormatter
from typing import Dict, List, Tuple
from collections import defaultdict
from .parser import fanse_parser
import sys

def add_mpileup_subparser(subparsers):
    """
    添加mpileup子命令解析器
    """
    parser = subparsers.add_parser(
        'mpileup',
        help='将FANSe3结果转换为mpileup格式',
        description='将FANSe3比对结果转换为samtools兼容的mpileup格式',
        formatter_class=CustomHelpFormatter
    )
    parser.add_argument('input_file', help='输入FANSe3文件路径')
    parser.add_argument('reference', help='参考基因组FASTA文件路径')
    parser.add_argument('-o', '--output', help='输出mpileup文件路径(默认: stdout)')
    parser.add_argument('--min-depth', type=int, default=0,
                       help='最小覆盖深度阈值(默认: 0)')
    parser.add_argument('--base-qual', type=int, default=30,
                       help='模拟碱基质量分数(默认: 30)')
    parser.set_defaults(func=convert_fanse_to_mpileup)

def convert_fanse_to_mpileup(args):
    """
    执行FANSe3到mpileup的转换
    """
    # 加载参考序列
    ref_seqs = load_reference_sequences(args.reference)
    
    # 初始化位置计数器
    pos_counts = defaultdict(lambda: defaultdict(int))
    pos_bases = defaultdict(lambda: defaultdict(list))
    
    # 解析FANSe3文件并统计每个位置的碱基
    for record in fanse_parser(args.input_file):
        process_fanse_record(record, ref_seqs, pos_counts, pos_bases)
    
    # 生成mpileup输出
    generate_mpileup_output(
        ref_seqs, 
        pos_counts, 
        pos_bases, 
        args.output, 
        args.min_depth,
        args.base_qual
    )

def load_reference_sequences(ref_file: str) -> Dict[str, str]:
    """
    加载参考基因组序列
    """
    sequences = {}
    current_id = None
    current_seq = []
    
    with open(ref_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            if line.startswith('>'):
                if current_id:
                    sequences[current_id] = "".join(current_seq).upper()
                current_id = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)
        if current_id:
            sequences[current_id] = "".join(current_seq).upper()
            
    return sequences

def process_fanse_record(
    record: 'FANSeRecord', 
    ref_seqs: Dict[str, str],
    pos_counts: Dict[str, Dict[int, int]],
    pos_bases: Dict[str, Dict[int, List[str]]]
):
    """
    处理单个FANSe记录，统计每个位置的碱基
    """
    for ref_name, pos, strand in zip(record.ref_names, record.positions, record.strands):
        if ref_name not in ref_seqs:
            continue
            
        ref_seq = ref_seqs[ref_name]
        read_pos = 0
        
        for i, (ref_base, read_base) in enumerate(zip(ref_seq[pos:], record.seq)):
            actual_pos = pos + i
            if actual_pos >= len(ref_seq):
                break
                
            # 统计碱基
            pos_counts[ref_name][actual_pos] += 1
            pos_bases[ref_name][actual_pos].append(read_base)

def generate_mpileup_output(
    ref_seqs: Dict[str, str],
    pos_counts: Dict[str, Dict[int, int]],
    pos_bases: Dict[str, Dict[int, List[str]]],
    output_file: str,
    min_depth: int,
    base_qual: int
):
    """
    生成mpileup格式输出
    """
    out_fh = open(output_file, 'w') if output_file else sys.stdout
    
    for ref_name, ref_seq in ref_seqs.items():
        for pos in range(len(ref_seq)):
            depth = pos_counts[ref_name].get(pos, 0)
            if depth < min_depth:
                continue
                
            ref_base = ref_seq[pos]
            bases = pos_bases[ref_name].get(pos, [])
            
            # 生成mpileup行
            bases_str = ''.join(bases)
            quals_str = chr(base_qual + 33) * len(bases)
            
            out_fh.write(f"{ref_name}\t{pos+1}\t{ref_base}\t{depth}\t{bases_str}\t{quals_str}\n")
    
    if output_file:
        out_fh.close()

if __name__ == '__main__':
    # 测试代码
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_mpileup_subparser(subparsers)
    args = parser.parse_args()
    args.func(args)