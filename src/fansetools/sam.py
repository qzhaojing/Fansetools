# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 11:56:35 2025
v0.1

@author: P.h.D., ZhaoJing, 

主要新增功能说明：
1. 区域解析功能 (parse_region_string)
支持samtools兼容的区域格式

自动处理边界检查和错误处理

返回结构化的区域字典

2. 记录过滤功能 (is_record_in_region)
高效检查记录是否在指定区域内

支持多重比对的区域检查

3. 命令行参数集成
使用 -R/--region参数（参考samtools习惯）

保持向后兼容性

4. 统计信息输出
显示过滤前后的记录数量

便于用户了解过滤效果

支持的区域格式：
chr1- 整个染色体

chr1:1000- 单点位置

chr1:1000-2000- 区间位置

chr1,chr2:500-1000- 多区域组合

Jinan University
"""

import os
import gzip
import sys
from tqdm import tqdm
from typing import Generator, Optional, Dict, Tuple, Iterator, Set, List
from .parser import FANSeRecord, fanse_parser

# 预编译转换表（全局变量）
_COMPLEMENT_TABLE = str.maketrans('ATCGNatcgn', 'TAGCNtagcn')

def reverse_complement(seq: str) -> str:
    """优化反向互补：使用str.translate"""
    return seq.translate(_COMPLEMENT_TABLE)[::-1]

# def reverse_complement(seq: str) -> str:   #上个函数为此函数的优化，更高效
#     """生成反向互补序列"""
#     complement = {'A': 'T', 'T': 'A', 'C': 'G', 'G': 'C', 'N': 'N'}
#     return ''.join([complement.get(base, 'N') for base in reversed(seq)])

# def generate_cigar(alignment: str, is_reverse: bool = False) -> str:
#     """
#     参数:
#         alignment: FANSe比对字符串
#         seq_len: 序列实际长度
#         is_reverse: 是否为反向链比对
#     返回:
#         符合规范的CIGAR字符串

#     CIGAR操作符说明:
#         M: 匹配/错配 (消耗参考序列和查询序列)
#         I: 插入 (仅消耗查询序列)
#         D: 缺失 (仅消耗参考序列)
#         N: 跳过 (同D但用于mRNA比对)
#         S: soft-clip (仅消耗查询序列)
#         H: hard-clip (不消耗序列)
#         =: 完全匹配
#         X: 错配
#     """
    
#     # if not alignment or seq_len <= 0:
#     #     return f"{seq_len}M"
    
#     # 对于反向链，需要反转比对字符串
#     if is_reverse:
#         alignment = alignment[::-1]
        
#     cigar = []
#     current_op = None
#     count = 0
#     # consumed_query = 0  # 已消耗的查询序列长度

#     for char in alignment:
#         # 确定操作类型
#         if char == '.':
#             op = 'M'
#             # consumed_query += 1
#         elif char == 'x':
#             op = 'X'
#             # consumed_query += 1
#         elif char == '-':
#             op = 'D'  # 不消耗查询序列
#         elif char.isalpha():
#             op = 'I'
#             # consumed_query += 1
#         else:
#             op = 'S'
#             # consumed_query += 1

#         # 统计连续操作
#         if op == current_op:
#             count += 1
#         else:
#             if current_op is not None:
#                 cigar.append(f"{count}{current_op}")
#             current_op = op
#             count = 1

#     # 添加最后一个操作
#     if current_op is not None:
#         cigar.append(f"{count}{current_op}")

#     return "".join(cigar)

def generate_cigar(alignment: str, is_reverse: bool = False) -> str:
    """优化CIGAR生成：使用更高效的算法"""
    """
    参数:
        alignment: FANSe比对字符串
        is_reverse: 是否为反向链比对
    返回:
        符合规范的CIGAR字符串

    CIGAR操作符说明:
        M: 匹配/错配 (消耗参考序列和查询序列)
        I: 插入 (仅消耗查询序列)
        D: 缺失 (仅消耗参考序列)
        N: 跳过 (同D但用于mRNA比对)
        S: soft-clip (仅消耗查询序列)
        H: hard-clip (不消耗序列)
        =: 完全匹配
        X: 错配
    """
    if is_reverse:
        alignment = alignment[::-1]
    
    if not alignment:
        return ""
    
    # 预编译操作符映射
    op_map = {'.': 'M', 'x': 'X', '-': 'D'}
    
    cigar_parts = []
    count = 1
    prev_char = alignment[0]
    
    for char in alignment[1:]:
        if char == prev_char:
            count += 1
        else:
            # 确定操作符
            op = op_map.get(prev_char, 'I' if prev_char.isalpha() else 'S')
            cigar_parts.append(f"{count}{op}")
            count = 1
            prev_char = char
    
    # 处理最后一个字符
    op = op_map.get(prev_char, 'I' if prev_char.isalpha() else 'S')
    cigar_parts.append(f"{count}{op}")
    
    return ''.join(cigar_parts)

# def calculate_flag(strand: str, is_secondary: bool = False) -> int:
#     """计算SAM FLAG值"""
#     flag = 0
#     if strand == 'R':
#         flag |= 0x10  # 反向互补
#     if is_secondary:
#         flag |= 0x100  # 辅助比对
#     return flag
def calculate_flag(
    strand: str,
    is_paired: bool = True,
    is_proper_pair: bool = True,
    is_mapped: bool = True,
    mate_mapped: bool = True,
    is_read1: bool = False,
    is_read2: bool = False,
    is_secondary: bool = False,
    is_qc_failed: bool = False,
    is_duplicate: bool = False
) -> int:
    """
    计算SAM FLAG值（基于SAM格式规范v1.6）

    参数说明：
    strand:      链方向 - 'F'正向 / 'R'反向互补
    is_paired:   是否为双端测序片段（默认True）
    is_proper_pair: 是否满足双端比对条件（默认True）
    is_mapped:   当前read是否比对成功（默认True）
    mate_mapped: 配对比对是否成功（默认True）
    is_read1:    是否为read1（双端中的第一条）
    is_read2:    是否为read2（双端中的第二条）
    is_secondary:是否为辅助比对（默认False）
    is_qc_failed:未通过质量控制（默认False）
    is_duplicate:是否为PCR重复序列（默认False）

    返回：完整SAM FLAG值（按位组合）
    """
    flag = 0

    # 0x1 (1): 模板包含多个测序片段（双端测序）
    if is_paired:
        flag |= 0x1

    # 0x2 (2): 所有片段均正确比对（仅当双端时有效）
    if is_paired and is_proper_pair:
        flag |= 0x2

    # 0x4 (4): 当前片段未比对到参考序列
    if not is_mapped:
        flag |= 0x4

    # 0x8 (8): 配对片段未比对到参考序列（仅当双端时有效）
    if is_paired and not mate_mapped:
        flag |= 0x8

    # 0x10 (16): 当前片段为反向互补链
    if strand == 'R':
        flag |= 0x10

    # 0x20 (32): 配对片段为反向互补链（仅当双端时有效）
    if is_paired and strand == 'F':  # 假设配对链方向相反
        flag |= 0x20

    # 0x40 (64): 第一条测序片段（read1）
    if is_read1:
        flag |= 0x40

    # 0x80 (128): 第二条测序片段（read2）
    if is_read2:
        flag |= 0x80

    # 0x100 (256): 辅助比对（非主要比对）
    if is_secondary:
        flag |= 0x100

    # 0x200 (512): 未通过QC过滤
    if is_qc_failed:
        flag |= 0x200

    # 0x400 (1024): PCR或光学重复
    if is_duplicate:
        flag |= 0x400

    return flag


def generate_sa_tag(record: FANSeRecord, primary_idx: int) -> str:
    """生成SA标签字符串"""
    sa_parts = []
    for i in range(len(record.ref_names)):
        if i == primary_idx:
            continue
        
        strand = 'R' if 'R' in record.strands[i] else 'F'
        is_reverse = (strand == 'R')
        cigar = generate_cigar(record.alignment[i], is_reverse)
        sa_parts.append(f"{record.ref_names[i]},{record.positions[i]+1},{strand}," +
                        f"{cigar},255,{record.mismatches[i]}")
    return f"SA:Z:{';'.join(sa_parts)}" if sa_parts else ""


# def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:
#     """将FANSeRecord转换为SAM格式行"""
#     if not record.ref_names:
#         return


#     # 找出主记录(错配最少)
#     primary_idx = min(range(len(record.mismatches)),
#                       key=lambda i: record.mismatches[i])

#     # 处理主记录
#     flag = calculate_flag(record.strands[primary_idx])
#     is_reverse = (record.strands[primary_idx] == 'R')
#     cigar = generate_cigar(record.alignment[primary_idx], is_reverse)
#     seq = reverse_complement(
#         record.seq) if 'R' in record.strands[primary_idx] else record.seq
#     sa_tag = generate_sa_tag(record, primary_idx)

#     sam_fields = [
#         record.header,
#         str(flag),
#         record.ref_names[primary_idx],
#         str(record.positions[primary_idx] + 1),  # 1-based
#         "255",  # MAPQ
#         cigar,
#         "*",    # RNEXT
#         "0",    # PNEXT
#         "0",    # TLEN
#         seq,
#         "*",    # QUAL
#         f"XM:i:{record.mismatches[primary_idx]}",
#         f"XN:i:{record.multi_count}"
#     ]

#     if sa_tag:
#         sam_fields.append(sa_tag)

#     yield "\t".join(sam_fields)

#     # 处理辅助记录
#     for i in range(len(record.ref_names)):
#         if i == primary_idx:
#             continue
#         flag = calculate_flag(record.strands[i], is_secondary=True)
#         is_reverse = (record.strands[i] == 'R')
#         cigar = generate_cigar(record.alignment[i], is_reverse)
#         seq = reverse_complement(
#             record.seq) if 'R' in record.strands[i] else record.seq

#         sam_fields = [
#             record.header,
#             str(flag),
#             record.ref_names[i],
#             str(record.positions[i] + 1),
#             "255",
#             cigar,
#             "*",
#             "0",
#             "0",
#             seq,
#             "*",
#             f"XM:i:{record.mismatches[i]}",
#             f"XN:i:{record.multi_count}"
#         ]
#         yield "\t".join(sam_fields)

# def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:  #优化版 #20251024
#     if not record.ref_names:
#         return

#     # 直接使用第一条记录作为主记录（多重比对的所有mismatches相同）
#     primary_idx = 0

#     # 预计算序列变体，避免重复计算
#     seq_cache = {
#         'F': record.seq,  # 正向序列
#         'R': reverse_complement(record.seq)  # 反向互补序列
#     }
    
#     # 预计算所有CIGAR字符串
#     cigars = []
#     for i in range(len(record.ref_names)):
#         is_reverse = (record.strands[i] == 'R')
#         cigars.append(generate_cigar(record.alignment[i], is_reverse))

#     # 处理主记录
#     strand = record.strands[primary_idx]
#     flag = calculate_flag(strand)
    
#     sam_fields = [
#         record.header,
#         str(flag),
#         record.ref_names[primary_idx],
#         str(record.positions[primary_idx] + 1),     # 1-based
#         "255",                                      # MAPQ
#         cigars[primary_idx],
#         "*",    # RNEXT
#         "0",    # PNEXT
#         "0",    # TLEN
#         seq_cache[strand],
#         "*",    # QUAL   fanse 文件没有质量值，因此忽略
#         f"XM:i:{record.mismatches[primary_idx]}",
#         f"XN:i:{record.multi_count}"
#     ]
    
#     # 生成SA标签（仅当有多重比对时）
#     if record.multi_count > 1:
#         sa_parts = []
#         for i in range(len(record.ref_names)):
#             if i == primary_idx:
#                 continue
#             strand_i = record.strands[i]
#             sa_parts.append(f"{record.ref_names[i]},{record.positions[i]+1},{strand_i},"
#                             f"{cigars[i]},255,{record.mismatches[i]}")
#         if sa_parts:
#             sam_fields.append(f"SA:Z:{';'.join(sa_parts)}")

#     yield "\t".join(sam_fields)

#     # 处理辅助记录（仅当有多重比对时）
#     if record.multi_count > 1:
#         for i in range(1, len(record.ref_names)):  # 从1开始，因为0是主记录，跳过第一个
#             strand_i = record.strands[i]
#             flag_i = calculate_flag(strand_i, is_secondary=True)
            
#             sam_fields_secondary = [
#                 record.header,
#                 str(flag_i),
#                 record.ref_names[i],
#                 str(record.positions[i] + 1),
#                 "255",
#                 cigars[i],
#                 "*", "0", "0",
#                 seq_cache[strand_i],
#                 "*",
#                 f"XM:i:{record.mismatches[i]}",
#                 f"XN:i:{record.multi_count}"
#             ]
#             yield "\t".join(sam_fields_secondary)

def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:  #20251024第二次优化
    if not record.ref_names:
        return

    # 直接使用第一条记录作为主记录
    primary_idx = 0
    primary_strand = record.strands[primary_idx]
    is_primary_reverse = (primary_strand == 'R')
    
    # 按需计算：只在需要时才计算反向互补序列
    primary_seq = record.seq
    if is_primary_reverse:
        primary_seq = reverse_complement(record.seq)
    
    # 主记录的CIGAR
    primary_cigar = generate_cigar(
        record.alignment[primary_idx], 
        is_primary_reverse
    )
    
    # 主记录的FLAG
    primary_flag = calculate_flag(primary_strand)
    
    # 构建主记录SAM行
    sam_fields = [
        record.header,
        str(primary_flag),
        record.ref_names[primary_idx],
        str(record.positions[primary_idx] + 1),
        "255",
        primary_cigar,
        "*", 
        "0", 
        "0",
        primary_seq,
        "*",
        f"XM:i:{record.mismatches[primary_idx]}",
        f"XN:i:{record.multi_count}"
    ]
    
    # 处理多重比对记录（只有multi_count > 1时才需要额外处理）
    if record.multi_count > 1:
        # 对于多重比对，预计算所有CIGAR和序列变体
        cigars = []
        seq_cache = {}
        
        # 预计算所有CIGAR
        for i in range(len(record.ref_names)):
            is_reverse = (record.strands[i] == 'R')
            cigars.append(generate_cigar(record.alignment[i], is_reverse))
        
        # 预计算序列变体（只在需要时）
        if any(strand == 'R' for strand in record.strands):
            seq_cache['R'] = reverse_complement(record.seq)
        if any(strand == 'F' for strand in record.strands):
            seq_cache['F'] = record.seq
        
        # 生成SA标签
        sa_parts = []
        for i in range(len(record.ref_names)):
            if i == primary_idx:
                continue
            strand_i = record.strands[i]
            sa_parts.append(f"{record.ref_names[i]},{record.positions[i]+1},{strand_i},"
                           f"{cigars[i]},255,{record.mismatches[i]}")
        
        if sa_parts:
            sam_fields.append(f"SA:Z:{';'.join(sa_parts)}")
        
        yield "\t".join(sam_fields)
        
        # 处理辅助记录
        for i in range(1, len(record.ref_names)):
            strand_i = record.strands[i]
            flag_i = calculate_flag(strand_i, is_secondary=True)
            seq_i = seq_cache[strand_i]  # 使用预计算的序列
            
            sam_fields_secondary = [
                record.header,
                str(flag_i),
                record.ref_names[i],
                str(record.positions[i] + 1),
                "255",
                cigars[i],
                "*", "0", "0",
                seq_i,
                "*",
                f"XM:i:{record.mismatches[i]}",
                f"XN:i:{record.multi_count}"
            ]
            yield "\t".join(sam_fields_secondary)
    else:
        # 单映射记录，直接生成主记录
        yield "\t".join(sam_fields)

def parse_fasta(fasta_path: str) -> Dict[str, int]:
    """
    解析FASTA文件获取参考序列名称和长度

    参数:
        fasta_path: FASTA文件路径(支持.gz压缩格式)

    返回:
        字典{序列名: 序列长度}
    """
    ref_info = {}
    current_seq = ""
    current_length = 0

    def _open_file(path):
        return gzip.open(path, 'rt') if path.endswith('.gz') else open(path, 'r')

    with _open_file(fasta_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                # 保存前一个序列的长度
                if current_seq:
                    ref_info[current_seq] = current_length
                # 开始新序列
                current_seq = line[1:].split()[0]  # 取>后的第一个单词作为名称
                current_length = 0
            else:
                current_length += len(line)

        # 添加最后一个序列
        if current_seq:
            ref_info[current_seq] = current_length

    return ref_info


# def generate_sam_header_from_fasta(fasta_path: str) -> str:
#     """
#     从FASTA文件生成完整的SAM头部

#     参数:
#         fasta_path: FASTA文件路径

#     返回:
#         完整的SAM头部字符串
#     """
#     ref_info = parse_fasta(fasta_path)

#     header_lines = [
#         "@HD\tVN:1.6\tSO:unsorted",
#         "@PG\tID:fanse3\tPN:fanse3\tVN:3.0\tCL:fanse3"
#     ]

#     # 添加参考序列信息
#     for ref_name, length in ref_info.items():
#         header_lines.append(f"@SQ\tSN:{ref_name}\tLN:{length}")

#     return '\n'.join(header_lines) + '\n'


# def fanse2sam(fanse_file, fasta_path, output_sam: Optional[str] = None):
#     """
#     将FANSe3文件转换为SAM格式

#     参数:
#         fanse_file: 输入FANSe3文件路径
#         output_sam: 输出SAM文件路径(如果为None则打印到标准输出)
#     """
#     # print('Start fanse2sam: {}'.format(fanse_file))
#     # 先读取ref_fasta的所有记录以生成头部
#     header = generate_sam_header_from_fasta(fasta_path)
#     # 组合两者
#     if output_sam:
#         with open(output_sam, 'w') as out_f:
#             # 写入SAM头
#             out_f.write(header)
#             print('Header write done.')
            
#             # 处理记录
#             # 批量处理，减少I/O调用
#             batch_size = 1000   #按照fanse记录数来计数batch
#             batch_count = 0     #按照fanse记录数来写入，而不是sam记录数（遇到多重比对会变多很多，影响整体效率）
#             batch_lines = []
            
#             file_read_size = os.path.getsize(fanse_file)/450    #粗略估计平均 450字节一个fanse记录
#             with tqdm(total=file_read_size, unit='reads', mininterval=5, unit_scale=True) as pbar:
#                 for record in fanse_parser(fanse_file):
#                     for sam_line in fanse_to_sam_type(record):
#                         batch_lines.append(sam_line)
#                         batch_count += 1
                    
#                     #当处理的够多，将缓存写入磁盘，减少IO次数
#                     if batch_count >= batch_size:
#                         out_f.write('\n'.join(batch_lines) + '\n')
#                         batch_lines = []
#                         batch_count = 0    #fanse记录清零重新计数，下一个batch
#                         # out_f.write(sam_line + "\n")
#                     pbar.update(1)
#                 # 写入剩余批次
#                 if batch_lines:
#                     out_f.write('\n'.join(batch_lines) + '\n')  
                    
#     else:
#         # 修复管道输出兼容性
#         try:
#             # 尝试直接写入标准输出缓冲区
#             sys.stdout.buffer.write(header.encode())
#             # 批量写入标准输出
#             batch_lines = []
#             for record in fanse_parser(fanse_file):
#                 for sam_line in fanse_to_sam_type(record):
#                     batch_lines.append(sam_line)
#                 if len(batch_lines) >= 1000:
#                     sys.stdout.buffer.write(('\n'.join(batch_lines) + '\n').encode())
#                     batch_lines = []
#             if batch_lines:
#                 sys.stdout.buffer.write(('\n'.join(batch_lines) + '\n').encode())
#         except AttributeError:
#             # 回退方案：使用原始标准输出
#             sys.__stdout__.write(header)
#             for record in fanse_parser(fanse_file):
#                 for sam_line in fanse_to_sam_type(record):
#                     sys.__stdout__.write(sam_line + "\n")

def generate_sam_header_from_ref_info(ref_info: Dict[str, int]) -> str:
    """从参考信息生成SAM头部"""
    header_lines = [
        "@HD\tVN:1.6\tSO:unsorted",
        "@PG\tID:fanse3\tPN:fanse3\tVN:3.0\tCL:fanse3"
    ]

    for ref_name, length in ref_info.items():
        header_lines.append(f"@SQ\tSN:{ref_name}\tLN:{length}")

    return '\n'.join(header_lines) + '\n'

def generate_sam_header_from_fasta(fasta_path: str) -> str:
    """从FASTA文件生成完整的SAM头部"""
    ref_info = parse_fasta(fasta_path)
    return generate_sam_header_from_ref_info(ref_info)

def parse_region_string(region_str: str, ref_info: Dict[str, int]) -> Dict[str, List[Tuple[int, int]]]:
    """
    解析区域字符串，返回区域字典
    
    参数:
        region_str: 区域字符串，支持格式:
            - 单个位置: "chr1:1000"
            - 多个位置: "chr1:1000-2000,chr2:3000-4000"  
            - 单染色体: "chr1"
            - 基因/转录本: 暂不支持，需要额外注释文件
        ref_info: 参考序列信息字典
        
    返回:
        字典{序列名: [(start, end), ...]}
    """
    regions = {}
    
    # 分割多个区域
    for region_part in region_str.split(','):
        region_part = region_part.strip()
        if not region_part:
            continue
            
        # 检查是否包含位置信息
        if ':' in region_part:
            # 处理具体位置
            chr_part, pos_part = region_part.split(':', 1)
            chr_name = chr_part.strip()
            
            # 验证染色体是否存在
            if chr_name not in ref_info:
                print(f"警告: 参考序列 '{chr_name}' 不在FASTA文件中，跳过该区域")
                continue
                
            chr_length = ref_info[chr_name]
            
            # 检查是否为区间格式
            if '-' in pos_part:
                # 区间格式: chr1:1000-2000
                try:
                    start_str, end_str = pos_part.split('-', 1)
                    start = int(start_str.strip()) - 1  # 转换为0-based
                    end = int(end_str.strip())  # 保持1-based
                    
                    # 边界检查
                    if start < 0:
                        start = 0
                    if end > chr_length:
                        end = chr_length
                    if start >= end:
                        print(f"警告: 无效区间 {region_part}，跳过")
                        continue
                        
                except ValueError:
                    print(f"警告: 无法解析区间 {region_part}，跳过")
                    continue
            else:
                # 单点格式: chr1:1000
                try:
                    pos = int(pos_part.strip()) - 1  # 转换为0-based
                    if pos < 0:
                        pos = 0
                    if pos >= chr_length:
                        print(f"警告: 位置 {pos+1} 超出染色体 {chr_name} 长度，跳过")
                        continue
                    start = pos
                    end = pos + 1  # 单点转换为1bp区间
                except ValueError:
                    print(f"警告: 无法解析位置 {region_part}，跳过")
                    continue
        else:
            # 单染色体格式: chr1
            chr_name = region_part.strip()
            if chr_name not in ref_info:
                print(f"警告: 参考序列 '{chr_name}' 不在FASTA文件中，跳过")
                continue
                
            chr_length = ref_info[chr_name]
            start = 0
            end = chr_length
        
        # 添加到区域字典
        if chr_name not in regions:
            regions[chr_name] = []
        regions[chr_name].append((start, end))
    
    return regions

def is_record_in_region(record: FANSeRecord, regions: Dict[str, List[Tuple[int, int]]]) -> bool:
    """
    检查记录是否在指定区域内
    
    参数:
        record: FANSe记录
        regions: 区域字典
        
    返回:
        bool: 是否在区域内
    """
    if not regions:  # 无区域限制，返回所有记录
        return True
        
    for i, ref_name in enumerate(record.ref_names):
        if ref_name in regions:
            pos = record.positions[i]  # 已经是0-based
            for start, end in regions[ref_name]:
                if start <= pos < end:
                    return True
                    
    return False

def fanse2sam(fanse_file, fasta_path, output_sam: Optional[str] = None, region: Optional[str] = None):
    """
    将FANSe3文件转换为SAM格式，支持区域过滤

    参数:
        fanse_file: 输入FANSe3文件路径
        fasta_path: 参考基因组FASTA文件路径
        output_sam: 输出SAM文件路径(如果为None则打印到标准输出)
        region: 区域过滤字符串
    """
    # 解析参考序列信息
    ref_info = parse_fasta(fasta_path)
    
    # 解析区域过滤条件
    regions = {}
    if region:
        regions = parse_region_string(region, ref_info)
        if regions:
            print(f"区域过滤: 将只输出 {len(regions)} 个染色体的指定区域")
        else:
            print("警告: 未解析到有效区域，将输出所有记录")
    
    # 生成SAM头部
    header = generate_sam_header_from_ref_info(ref_info)
    
    if output_sam:
        with open(output_sam, 'w') as out_f:
            # 写入SAM头
            out_f.write(header)
            print('Write SAM header down.')
            
            # 处理记录
            batch_size = 1000
            batch_count = 0
            batch_lines = []
            filtered_count = 0
            total_count = 0
            
            file_read_size = os.path.getsize(fanse_file) / 450     #粗略估计平均 450字节一个fanse记录
            with tqdm(total=file_read_size, unit='reads', mininterval=5, unit_scale=True) as pbar:
                for record in fanse_parser(fanse_file):
                    total_count += 1
                    
                    # 区域过滤
                    if regions and not is_record_in_region(record, regions):
                        filtered_count += 1
                        pbar.update(1)
                        continue
                    
                    for sam_line in fanse_to_sam_type(record):
                        batch_lines.append(sam_line)
                        batch_count += 1
                    
                    if batch_count >= batch_size:
                        out_f.write('\n'.join(batch_lines) + '\n')
                        batch_lines = []
                        batch_count = 0
                    
                    pbar.update(1)
                
                # 写入剩余批次
                if batch_lines:
                    out_f.write('\n'.join(batch_lines) + '\n')
            
            # 输出统计信息
            print(f"处理完成: 总共 {total_count} 条记录，过滤 {filtered_count} 条，输出 {total_count - filtered_count} 条")
                    
    else:
        # 标准输出模式
        try:
            sys.stdout.buffer.write(header.encode())
            batch_lines = []
            filtered_count = 0
            total_count = 0
            
            for record in fanse_parser(fanse_file):
                total_count += 1
                
                # 区域过滤
                if regions and not is_record_in_region(record, regions):
                    filtered_count += 1
                    continue
                
                for sam_line in fanse_to_sam_type(record):
                    batch_lines.append(sam_line)
                
                if len(batch_lines) >= 1000:
                    sys.stdout.buffer.write(('\n'.join(batch_lines) + '\n').encode())
                    batch_lines = []
            
            if batch_lines:
                sys.stdout.buffer.write(('\n'.join(batch_lines) + '\n').encode())
                
            # 错误输出统计信息
            sys.stderr.write(f"处理完成: 总共 {total_count} 条记录，过滤 {filtered_count} 条，输出 {total_count - filtered_count} 条\n")
            
        except AttributeError:
            # 回退方案
            sys.__stdout__.write(header)
            for record in fanse_parser(fanse_file):
                if regions and not is_record_in_region(record, regions):
                    continue
                for sam_line in fanse_to_sam_type(record):
                    sys.__stdout__.write(sam_line + "\n")

def run_sam_command(args):
    """Handle sam subcommand"""
# def run_fanse2sam(args):
    fanse2sam(args.fanse_file, 
              args.fasta_path, 
              args.output_sam,
              region=args.region,
              )


def add_sam_subparser(subparsers):
    sam_parser = subparsers.add_parser(
        'sam',
        help='转换为 SAM 格式',
        description='将 FANSe3 文件转换为标准 SAM 格式, 在linux中不加-o参数可接 samtools 管道处理直接保存为bam格式，支持区域过滤'
    )
    
    sam_parser.add_argument(
        '-i', '--input', dest='fanse_file', required=True, help='输入文件路径（FANSe3 格式）')
    sam_parser.add_argument(
        '-r', '--fasta', dest='fasta_path', required=True, help='参考基因组 FASTA 文件路径')
    sam_parser.add_argument(
        '-o', '--output', dest='output_sam', help='输出文件路径（不指定输出位置，默认打印到终端）')
    sam_parser.add_argument(
        '-R', '--region', 
        help='区域过滤（参考samtools格式）: chr1, chr1:1000, chr1:1000-2000, chr1,chr2:500-1000'
    )
    sam_parser.add_argument(
        '--sort', choices=['coord', 'name'], 
        help='输出排序方式'
    )

    sam_parser.set_defaults(func=run_sam_command)



#后续还可以加入需要查看的位置信息，即只输出这个部分的sam文件，提升速度。位置支持单个位置，多个位置，单染色体，单基因，单转录本
#sort可以直接支持最好了，单独的那边已经有命令，这里可以集成那个命令直接用。

# 使用示例
if __name__ == "__main__":
    # 测试数据

    if len(sys.argv) < 2:
        print("Usage: python fanse2sam.py <input.fanse3> [output.sam]")
        sys.exit(1)
    fasta_path = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 2 else None
    fanse2sam(sys.argv[1], fasta_path, output_file)


# ---------------------------------
    # fanse_file = r'G:\verysync_zhaojing\sample.fanse3'
    fasta_path = r'\\fs2\d\data\zhaoJing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\2.ref_seq_0820_new_merge_fasta\species_188_and_hg38.fasta'
    fanse_file = r'\\fs2\d\data\zhaoJing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\4.1.merge-polya-addHuman\4.1.merge-polya.fanse3'
    output_sam = r'\\fs2\d\data\zhaoJing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\4.1.merge-polya-addHuman\4.1.merge-polya.sam'
    fanse2sam(fanse_file, fasta_path, output_sam)

    # 示例用法
    fasta_path = r'ref.fasta'
    fanse_file = r'input.fanse3'
    output_sam = r'output.sam'
    
    # 不同区域过滤示例
    # 1. 单染色体
    fanse2sam(fanse_file, fasta_path, "output_chr1.sam", region="chr1")
    
    # 2. 单位置
    fanse2sam(fanse_file, fasta_path, "output_pos.sam", region="chr1:1000")
    
    # 3. 区间
    fanse2sam(fanse_file, fasta_path, "output_region.sam", region="chr1:1000-2000")
    
    # 4. 多区域
    fanse2sam(fanse_file, fasta_path, "output_multi.sam", region="chr1:1000-2000,chr2:5000-6000")
    
    # 5. 无过滤
    fanse2sam(fanse_file, fasta_path, "output_all.sam")
    