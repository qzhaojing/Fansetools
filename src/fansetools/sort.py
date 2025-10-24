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
    if not os.path.exists(args.input):
        print(f"错误: 输入文件 {args.input} 不存在")
        sys.exit(1)
    
    sort_by = 'coord' if args.coord_sort else 'name'
    
    print(f"开始排序 {args.input}...")
    sort_sam(args.input, args.output, sort_by=sort_by)
    print(f"排序完成! 输出文件: {args.output}")

def add_sort_subparser(subparsers):
    """添加sort子命令解析器"""
    sort_parser = subparsers.add_parser(
        'sort',
        help='SAM文件排序',
        description='对SAM文件进行排序，支持按坐标或名称排序'
    )
    
    sort_parser.add_argument(
        '-i', '--input', required=True, help='输入SAM文件路径')
    sort_parser.add_argument(
        '-o', '--output', required=True, help='输出SAM文件路径')
    
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