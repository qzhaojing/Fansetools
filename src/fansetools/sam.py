# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 11:56:35 2025
v0.1
v0.2 优化的FANSe到SAM转换，包含精确MAPQ计算
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


#后续加入
1. unmapped reads 也纳入sam格式
2. 双端reads如何匹配到一起：
    - 1. -1, -2端reads去接头时候即采用配对模式，保留所有双端reads。然后比对后，进行fanse+unmapped合并，然后排序（需要硬盘，内存双多，不太合适的感觉），双端reads理论上可以实现按顺序排列。然后顺序读取两个文件中的reads，判断是双端后，确定pos是否唯一
    - 2， 转化sam/bam时候，纳入unmapped reads，然后给定文件-1-2的方向信息存入对应文件的tag；然后两个bam进行排序候，进行配对修复samtools fixmate？如此得到配对的双端reads---好像更简单，不用修改很多代码，利用现有工具实现双端


支持的区域格式：
chr1- 整个染色体
chr1:1000- 单点位置
chr1:1000-2000- 区间位置
chr1,chr2:500-1000- 多区域组合

Jinan University
"""
import math
import os
import gzip
import sys
from tqdm import tqdm
from typing import Generator, Optional, Dict, Tuple, Iterator, Set, List
from .parser import FANSeRecord, fanse_parser,fanse_parser_high_performance
from .utils.rich_help import CustomHelpFormatter
from .utils.path_utils import PathProcessor
from rich.console import Console
import pathlib

# 预编译转换表（全局变量）
_COMPLEMENT_TABLE = str.maketrans('ATCGNatcgn', 'TAGCNtagcn')

def reverse_complement(seq: str) -> str:
    """优化反向互补：使用str.translate"""
    return seq.translate(_COMPLEMENT_TABLE)[::-1]

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
        S: soft-clip (仅消耗查询序列)   #fanse不支持？
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

def calculate_flag(
    strand: str,
    is_paired: bool = False,   # 默认改为False，因为fanse目前支持单端测序
    is_proper_pair: bool = False,
    is_mapped: bool = True,
    mate_mapped: bool = False,
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

def calculate_nm(alignment: str) -> int:
    """计算编辑距离（不匹配+插入+缺失）"""
    nm = 0
    for char in alignment:
        if char == 'x':  # 错配
            nm += 1
        elif char == '-':  # 缺失
            nm += 1
        elif char.isalpha() and char not in 'x':  # 插入
            nm += 1
    return nm



def calculate_mapq(record: FANSeRecord, alignment_index: int, is_primary: bool = True) -> int:
    """
    计算精确的MAPQ值，基于FANSe3比对特征
    
    参数:
        record: FANSe记录
        alignment_index: 当前比对在记录中的索引
        is_primary: 是否为主要比对
    
    返回:
        MAPQ值 (0-60)
    """
    # 获取当前比对信息
    alignment_str = record.alignment[alignment_index]
    mismatches = record.mismatches[alignment_index]
    read_length = len(record.seq)
    multi_count = record.multi_count
    
    # 1. 计算基础比对质量（基于错配率）
    alignment_length = len(alignment_str)
    
    # 计算有效比对长度（排除缺失和软裁剪）
    effective_length = sum(1 for char in alignment_str if char not in '-S')
    
    if effective_length == 0:
        return 0  # 无效比对
    
    # 错配率
    mismatch_rate = mismatches / effective_length
    
    # 2. 基础质量得分（基于错配率，0-60分）
    # 完美比对: 60分，每增加1%错配率减少3分
    base_quality = max(0, 60 - (mismatch_rate * 100 * 3))
    
    # 3. 长度惩罚因子（短比对惩罚）
    length_factor = min(1.0, effective_length / 100.0)  # 以100bp为基准
    
    # 4. 多重比对惩罚因子
    if multi_count > 1:
        # 多重比对惩罚：每个额外比对减少质量
        multi_penalty = min(30, 10 * math.log2(multi_count))
    else:
        multi_penalty = 0
    
    # 5. 比对一致性得分（基于连续匹配）
    consistency_score = calculate_alignment_consistency(alignment_str)
    
    # 6. 最终MAPQ计算
    mapq = base_quality * length_factor - multi_penalty + consistency_score
    
    # 7. 如果是次要比对，进一步降低质量
    if not is_primary:
        mapq *= 0.7  # 次要比对质量降低30%
    
    # 边界检查和质量分级
    mapq = max(0, min(60, mapq))
    
    # 8. 质量分级（离散化到标准MAPQ级别）
    return discretize_mapq(mapq)

def calculate_alignment_consistency(alignment: str) -> float:
    """
    计算比对一致性得分，基于连续匹配块的质量
    
    参数:
        alignment: 比对字符串
    
    返回:
        一致性得分 (0-10)
    """
    if not alignment:
        return 0
    
    # 查找连续匹配块
    current_char = alignment[0]
    current_length = 1
    match_blocks = []
    
    for char in alignment[1:]:
        if char == current_char:
            current_length += 1
        else:
            if current_char in '.':  # 匹配块
                match_blocks.append(current_length)
            current_char = char
            current_length = 1
    
    # 处理最后一个块
    if current_char in '.':
        match_blocks.append(current_length)
    
    if not match_blocks:
        return 0
    
    # 计算平均匹配块长度和最大块长度
    avg_block_length = sum(match_blocks) / len(match_blocks)
    max_block_length = max(match_blocks)
    
    # 一致性得分：基于块长度和质量
    consistency = min(10, (avg_block_length + max_block_length) / 20.0)
    return consistency

def discretize_mapq(raw_mapq: float) -> int:
    """
    将原始MAPQ值离散化到标准级别
    
    参数:
        raw_mapq: 原始MAPQ值
    
    返回:
        离散化的MAPQ (0, 1, 3, 5, 10, 20, 30, 40, 50, 60)
    """
    # 标准MAPQ离散级别
    levels = [0, 1, 3, 5, 10, 20, 30, 40, 50, 60]
    
    for level in reversed(levels):
        if raw_mapq >= level:
            return level
    
    return 0

def calculate_mapq_advanced(record: FANSeRecord, alignment_index: int, 
                          is_primary: bool = True, scoring_system: dict = None) -> int:
    """
    高级MAPQ计算，支持自定义打分系统
    
    参数:
        record: FANSe记录
        alignment_index: 比对索引
        is_primary: 是否主要比对
        scoring_system: 自定义打分系统
    
    返回:
        MAPQ值
    """
    if scoring_system is None:
        scoring_system = {
            'match_score': 2,      # 匹配得分
            'mismatch_penalty': -4, # 错配惩罚
            'gap_open_penalty': -6, # 开空位惩罚
            'gap_extend_penalty': -1, # 空位延伸惩罚
            'min_mapq': 0,          # 最小MAPQ
            'max_mapq': 60          # 最大MAPQ
        }
    
    alignment = record.alignment[alignment_index]
    mismatches = record.mismatches[alignment_index]
    
    # 计算比对得分
    alignment_score = 0
    gap_open = False
    
    for char in alignment:
        if char == '.':
            alignment_score += scoring_system['match_score']
            gap_open = False
        elif char == 'x':
            alignment_score += scoring_system['mismatch_penalty']
            gap_open = False
        elif char == '-':
            if not gap_open:
                alignment_score += scoring_system['gap_open_penalty']
                gap_open = True
            else:
                alignment_score += scoring_system['gap_extend_penalty']
        else:  # 插入
            if not gap_open:
                alignment_score += scoring_system['gap_open_penalty']
                gap_open = True
            else:
                alignment_score += scoring_system['gap_extend_penalty']
    
    # 理论最大得分（完美比对）
    max_possible_score = len([c for c in alignment if c != '-']) * scoring_system['match_score']
    
    if max_possible_score == 0:
        return scoring_system['min_mapq']
    
    # 得分比例
    score_ratio = alignment_score / max_possible_score
    
    # 转换为MAPQ
    raw_mapq = score_ratio * scoring_system['max_mapq']
    
    # 多重比对惩罚,太多了就罚60分了，没分了
    if record.multi_count > 1:
        multi_penalty = min(60, math.log2(record.multi_count) * 5)
        raw_mapq -= multi_penalty
    
    # 次要比对惩罚
    if not is_primary:
        raw_mapq *= 0.6
    
    # 边界检查
    raw_mapq = max(scoring_system['min_mapq'], min(scoring_system['max_mapq'], raw_mapq))
    
    return discretize_mapq(raw_mapq)

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

def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:  #20251024第二次优化
    if not record.ref_names:
        return
    
        
    # 直接使用第一条记录作为主记录
    primary_idx = 0
    primary_strand = record.strands[primary_idx]
    is_primary_reverse = (primary_strand == 'R')

    # 预计算所有MAPQ值
    mapq_values = []
    for i in range(len(record.ref_names)):
        is_primary = (i == primary_idx)
        # 使用高级MAPQ计算
        mapq = calculate_mapq_advanced(record, i, is_primary)
        mapq_values.append(mapq)
    
    # 按需计算：只在需要时才计算反向互补序列
    primary_seq = record.seq
    if is_primary_reverse:
        primary_seq = reverse_complement(record.seq)
    
    # 主记录的CIGAR
    primary_cigar = generate_cigar(
        record.alignment[primary_idx], 
        is_primary_reverse
    )
    nm = calculate_nm(record.alignment[i])
    # 主记录的FLAG
    primary_flag = calculate_flag(primary_strand, is_secondary=False)
    
    # 构建主记录SAM行
    sam_fields = [
        record.header,
        str(primary_flag),
        record.ref_names[primary_idx],
        str(record.positions[primary_idx] + 1),
        str(mapq_values[primary_idx]),  # 使用计算的MAPQ
        primary_cigar,
        "*", 
        "0", 
        "0",
        primary_seq,
        "*",
        f"XM:i:{record.mismatches[primary_idx]}",
        f"XN:i:{record.multi_count}",
        f"NM:i:{nm}",  # 添加编辑距离
        f"XS:i:{mapq_values[primary_idx]}"  # 添加原始得分标签
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
                           f"{cigars[i]},{mapq_values[i]},{record.mismatches[i]}")
        
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
                str(mapq_values[i]),  # 辅助比对的MAPQ（通常较低）
                cigars[i],
                "*", 
                "0", 
                "0",
                seq_i,
                "*",
                f"XM:i:{record.mismatches[i]}",
                f"XN:i:{record.multi_count}",
                f"NM:i:{nm}",  # 添加编辑距离,这里目前与主记录一致，是否应该这样，后面再检查
                f"XS:i:{mapq_values[i]}",
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

def parse_region_string(region_str: str, ref_info: Dict[str, int], console=None) -> Dict[str, List[Tuple[int, int]]]:
    """
    解析区域字符串，返回区域字典
    
    参数:
        region_str: 区域字符串，支持格式:
            - 单个位置: "chr1:1000"
            - 多个位置: "chr1:1000-2000,chr2:3000-4000"  
            - 单染色体: "chr1"
            - 基因/转录本: 暂不支持，需要额外注释文件
        ref_info: 参考序列信息字典
        console: rich Console 对象
        
    返回:
        字典{序列名: [(start, end), ...]}
    """
    if console is None:
        console = Console(stderr=True)

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
                console.print(f"[bold yellow]警告: 参考序列 '{chr_name}' 不在FASTA文件中，跳过该区域[/bold yellow]")
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
                        console.print(f"[bold yellow]警告: 无效区间 {region_part}，跳过[/bold yellow]")
                        continue
                        
                except ValueError:
                    console.print(f"[bold yellow]警告: 无法解析区间 {region_part}，跳过[/bold yellow]")
                    continue
            else:
                # 单点格式: chr1:1000
                try:
                    pos = int(pos_part.strip()) - 1  # 转换为0-based
                    if pos < 0:
                        pos = 0
                    if pos >= chr_length:
                        console.print(f"[bold yellow]警告: 位置 {pos+1} 超出染色体 {chr_name} 长度，跳过[/bold yellow]")
                        continue
                    start = pos
                    end = pos + 1  # 单点转换为1bp区间
                except ValueError:
                    console.print(f"[bold yellow]警告: 无法解析位置 {region_part}，跳过[/bold yellow]")
                    continue
        else:
            # 单染色体格式: chr1
            chr_name = region_part.strip()
            if chr_name not in ref_info:
                console.print(f"[bold yellow]警告: 参考序列 '{chr_name}' 不在FASTA文件中，跳过[/bold yellow]")
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

def fanse2sam(fanse_file, fasta_path, output_sam: Optional[str] = None, region: Optional[str] = None, console=None):
    """
    将FANSe3文件转换为SAM格式，支持区域过滤

    参数:
        fanse_file: 输入FANSe3文件路径
        fasta_path: 参考基因组FASTA文件路径
        output_sam: 输出SAM文件路径(如果为None则打印到标准输出)
        region: 区域过滤字符串
        console: rich Console 对象，用于日志输出
    """
    if console is None:
        console = Console(stderr=True)

    # 解析参考序列信息
    ref_info = parse_fasta(fasta_path)
    
    # 解析区域过滤条件
    regions = {}
    if region:
        regions = parse_region_string(region, ref_info, console)
        if regions:
            console.print(f"区域过滤: 将只输出 {len(regions)} 个染色体的指定区域")
        else:
            console.print("警告: 未解析到有效区域，将输出所有记录")
    
    # 生成SAM头部
    header = generate_sam_header_from_ref_info(ref_info)
    
    if output_sam:
        with open(output_sam, 'w') as out_f:
            # 写入SAM头
            out_f.write(header)
            console.print('Write SAM header down.')
            
            # 处理记录
            batch_size = 100000
            batch_count = 0
            batch_lines = []
            filtered_count = 0
            total_count = 0
            
            file_read_size = os.path.getsize(fanse_file) / 450     #粗略估计平均 450字节一个fanse记录
            with tqdm(total=file_read_size, unit='reads', mininterval=5, unit_scale=True) as pbar:
                for record in fanse_parser_high_performance(fanse_file):
                    total_count += 1
                    
                    # 区域过滤，没有过滤信息则直接跳过（regions为None）
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
            console.print(f"处理完成: 总共 {total_count} 条记录，过滤 {filtered_count} 条，输出 {total_count - filtered_count} 条")
                    
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
            console.print(f"处理完成: 总共 {total_count} 条记录，过滤 {filtered_count} 条，输出 {total_count - filtered_count} 条")
            
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
    console = Console(force_terminal=True, stderr=True)
    processor = PathProcessor()

    # 解析输入文件
    try:
        input_files = processor.parse_input_paths(args.fanse_file, ['.fanse3', '.fanse', '.fanse3.gz', '.fanse.gz'])
    except Exception as e:
        console.print(f"[bold red]Error parsing inputs: {e}[/bold red]")
        return

    if not input_files:
        console.print(f"[bold red]Input file not found: {args.fanse_file}[/bold red]")
        return

    # 确定输出目录（如果批量处理）
    output_dir = None
    if args.output_sam and len(input_files) > 1:
        if not os.path.exists(args.output_sam):
            try:
                os.makedirs(args.output_sam, exist_ok=True)
            except OSError:
                console.print(f"[bold red]Error: Output path '{args.output_sam}' must be a directory when processing multiple files.[/bold red]")
                return
        elif not os.path.isdir(args.output_sam):
            console.print(f"[bold red]Error: Output path '{args.output_sam}' must be a directory when processing multiple files.[/bold red]")
            return
        output_dir = args.output_sam

    # 批量处理
    for i, input_file in enumerate(input_files):
        input_path = str(input_file)
        
        # 确定输出路径
        if output_dir:
            fname = input_file.stem
            output_path = os.path.join(output_dir, fname + '.sam')
        elif args.output_sam:
            output_path = args.output_sam
        else:
            output_path = None # stdout

        if len(input_files) > 1:
            console.print(f"[dim]Processing ({i+1}/{len(input_files)}): {input_file.name}[/dim]")

        try:
            fanse2sam(input_path, 
                      args.fasta_path, 
                      output_path,
                      region=args.region,
                      console=console
                      )
        except Exception as e:
            console.print(f"[bold red]Error processing {input_path}: {e}[/bold red]")


def add_sam_subparser(subparsers):
    sam_parser = subparsers.add_parser(
        'sam',
        help='转换为 SAM 格式',
        description='将 FANSe3 文件转换为标准 SAM 格式, 在linux中不加-o参数可接 samtools 管道处理直接保存为bam格式，支持区域过滤',
        formatter_class=CustomHelpFormatter
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
    