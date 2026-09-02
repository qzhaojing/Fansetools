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
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Pool
from tqdm import tqdm
from typing import Generator, Optional, Dict, Tuple, Iterator, Set, List
from .parser import FANSeRecord, fanse_parser, fanse_parser_high_performance, fanse_line_reader, parse_records_from_lines, unmapped_parser
from .utils.rich_help import CustomHelpFormatter, add_rich_epilog
from .utils.path_utils import PathProcessor
from rich.console import Console
import pathlib
import subprocess # 导入subprocess模块

def _sam_ref_name(name: str) -> str:
    """
    新增：将完整 FASTA header 规范化为 accession（第一个空格前的部分）。

    修正意图：fanse3 的 ref 名是完整 FASTA header（如
    'NC_000962.3 Mycobacterium tuberculosis H37Rv, complete genome'），
    直接写入 SAM 会导致 RNAME / @SQ SN / SA:Z 里携带冗长描述。
    且 SAM 规范要求 SN: 不能含空白字符，带空格的名称不合规，
    IGV 等下游工具可能解析异常。统一截取 accession：
      - RNAME 与 @SQ SN 完全一致（samtools 校验的前提）
      - SA:Z 中的参考名同步规范化，保持与 RNAME 一致
    名称不含空格时原样返回（天然兼容无描述的 header）。
    """
    if not name:
        return name
    sp = name.find(' ')
    return name if sp == -1 else name[:sp]

def _merge_fanse_and_unmapped_records(fanse_file: str, unmapped_file: str) -> Generator[FANSeRecord, None, None]:
    """
    合并FANSe3文件和unmapped文件中的记录，生成统一的FANSeRecord流。

    参数:
        fanse_file: FANSe3文件路径。
        unmapped_file: unmapped文件路径。

    返回:
        生成器，每次yield一个FANSeRecord对象。
    """
    # 从FANSe3文件解析记录
    for record in fanse_parser(fanse_file):
        yield record
    
    # 从unmapped文件解析记录
    for record in unmapped_parser(unmapped_file):
        yield record

# 预编译转换表（全局变量）
_COMPLEMENT_TABLE = str.maketrans('ATCGNatcgn', 'TAGCNtagcn')

def _worker_process_batch(records: List[FANSeRecord], regions: Optional[Dict] = None) -> List[str]:
    """
    Worker function for parallel processing of FANSe records in batches.
    """
    results = []
    for record in records:
        if regions and not is_record_in_region(record, regions):
            continue
        results.extend(fanse_to_sam_type(record))
    return results

def _worker_process_records(records: List[FANSeRecord], regions: Optional[Dict] = None) -> List[str]:
    """
    兼容并行分支调用名称，实际复用批处理逻辑。
    修正意图：避免 threads>1 时出现 NameError: _worker_process_records 未定义。
    """
    return _worker_process_batch(records, regions=regions)

def _iter_record_batches(record_generator, batch_size: int):
    """
    按批次惰性切分记录，避免对超大输入一次性 list() 带来的内存压力。
    修正意图：替代原先 list(record_generator) 的重复构造。
    """
    batch = []
    for record in record_generator:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch



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
    if not alignment:
        return ""
    
    # 预编译操作符映射
    op_map = {'.': 'M', 'x': 'X', '-': 'I', '+': 'D'} # 修正：根据FANSe规范调整I和D的映射
    
    cigar_parts = []
    count = 1
    prev_char = alignment[0]
    
    for char in alignment[1:]:
        if char == prev_char:
            count += 1
        else:
            # 确定操作符
            op = op_map.get(prev_char, 'M') # 修正：如果遇到未知字符，默认按M处理，避免生成S
            cigar_parts.append(f"{count}{op}")
            count = 1
            prev_char = char
    
    # 处理最后一个字符
    op = op_map.get(prev_char, 'M') # 修正：如果遇到未知字符，默认按M处理，避免生成S
    cigar_parts.append(f"{count}{op}")
    
    return ''.join(cigar_parts)

def calculate_flag(record: FANSeRecord, is_reverse_strand: bool, is_primary_alignment: bool) -> int:
    """
    根据FANSeRecord信息计算SAM FLAG。
    现在FANSeRecord中已经包含了is_mapped, is_first_in_pair, is_second_in_pair等信息。
    """
    flag = 0

    # 0x1: read is paired in sequencing
    if record.is_first_in_pair is not None or record.is_second_in_pair is not None:
        flag |= 0x1

    # 0x2: read is mapped in a proper pair (由samtools fixmate设置)
    # 0x4: read is unmapped
    if not record.is_mapped:
        flag |= 0x4

    # 0x8: mate is unmapped (由samtools fixmate设置)

    # 0x10: read reverse strand
    if is_reverse_strand:
        flag |= 0x10

    # 0x20: mate reverse strand (由samtools fixmate设置)

    # 0x40: first in pair
    if record.is_first_in_pair:
        flag |= 0x40

    # 0x80: second in pair
    if record.is_second_in_pair:
        flag |= 0x80

    # 0x100: not primary alignment
    if not is_primary_alignment:
        flag |= 0x100

    # 0x200: read fails platform/vendor quality checks (FANSe3无此信息，默认为通过)
    # 0x400: read is PCR or optical duplicate (FANSe3无此信息)
    # 0x800: supplementary alignment (由fanse_to_sam_type处理)

    return flag

def calculate_nm(alignment: str) -> int:
    """计算编辑距离（不匹配+插入+缺失）"""
    nm = 0
    for char in alignment:
        if char == 'x':  # 错配
            nm += 1
        elif char == '-':  # read 插入（参考序列缺失）
            nm += 1
        elif char == '+':  # read 缺失（参考序列插入）
            nm += 1
    return nm





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
    mismatches = record.mismatches[alignment_index] # 从record中获取
    multi_count = record.multi_count # 从record中获取
    
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
    raw_mapq_val = -10 * math.log10(1 - score_ratio) if score_ratio < 1 else scoring_system['max_mapq'] # 使用Phred-scaled概率
    
    # 多重比对惩罚,太多了就罚60分了，没分了
    if multi_count > 1:
        multi_penalty = min(60, math.log2(multi_count) * 5)
        raw_mapq_val -= multi_penalty
    
    # 次要比对惩罚
    if not is_primary:
        raw_mapq_val *= 0.6
    
    # 边界检查
    raw_mapq_val = max(scoring_system['min_mapq'], min(scoring_system['max_mapq'], raw_mapq_val))
    
    return discretize_mapq(raw_mapq_val)

def generate_sa_tag(record: FANSeRecord, primary_idx: int) -> str:
    """生成SA标签字符串"""
    sa_parts = []
    for i in range(len(record.ref_names)):
        if i == primary_idx:
            continue
        
        # 获取辅助比对信息
        # 修正：SA:Z 中参考名规范化为 accession，与 RNAME 一致
        supp_ref_name = _sam_ref_name(record.ref_names[i])
        supp_position = record.positions[i]
        supp_strand = record.strands[i] if record.strands else 'F'
        is_supp_reverse = (supp_strand == 'R')
        
        supp_cigar = generate_cigar(record.alignment[i], is_supp_reverse)
        supp_mapq = calculate_mapq_advanced(record, i, is_primary=False) # 计算辅助比对的MAPQ
        supp_nm = calculate_nm(record.alignment[i]) # 计算辅助比对的NM

        strand_char = '+' if not is_supp_reverse else '-'
        sa_parts.append(f"{supp_ref_name},{supp_position+1},{strand_char}," +
                        f"{supp_cigar},{supp_mapq},{supp_nm}") # 修正：使用正确的MAPQ和NM
    return f"SA:Z:{';'.join(sa_parts)}" if sa_parts else ""

def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:  #20251024第二次优化
    """
    将FANSeRecord对象转换为SAM格式的字符串。
    现在支持FANSeRecord中包含的双端信息和未比对信息。
    """
    # 质量值（FANSe3不提供，默认为所有位置'I'）
    qual = 'I' * len(record.seq)

    # 预先计算所有辅助比对的SA标签，如果存在的话
    all_sa_tag_str = ""
    if record.is_multi and len(record.ref_names) > 1:
        sa_parts = []
        primary_idx = 0 # 假设第一个是主比对
        for i in range(len(record.ref_names)):
            # 获取比对信息
            # 修正：SA:Z 中参考名规范化为 accession，与 RNAME 一致
            ref_name = _sam_ref_name(record.ref_names[i])
            position = record.positions[i]
            strand = record.strands[i] if record.strands else 'F'
            is_reverse = (strand == 'R')
            
            cigar = generate_cigar(record.alignment[i], is_reverse)
            mapq = calculate_mapq_advanced(record, i, is_primary=(i == primary_idx)) # 计算MAPQ
            nm = calculate_nm(record.alignment[i]) # 计算NM

            strand_char = '+' if not is_reverse else '-'
            sa_parts.append(f"{ref_name},{position+1},{strand_char},{cigar},{mapq},{nm}")
        all_sa_tag_str = f"SA:Z:{';'.join(sa_parts)};"

    # 如果read未比对
    if not record.is_mapped:
        # 计算FLAG
        flag = calculate_flag(record, is_reverse_strand=False, is_primary_alignment=True) # 未比对read，不考虑链方向和辅助比对

        # RNEXT, PNEXT, TLEN 占位符，由samtools fixmate修正
        rnext = '*'
        pnext = 0
        tlen = 0

        # 生成SAM行
        sam_line = '\t'.join([
            record.header,
            str(flag),
            '*',  # RNAME
            '0',  # POS
            '0',  # MAPQ
            '*',  # CIGAR
            rnext,
            str(pnext),
            str(tlen),
            record.seq,
            qual
        ])
        yield sam_line
        return # 未比对read只有一行SAM输出

    # 处理比对上的reads
    # 确定主要比对
    primary_idx = 0
    if record.is_multi:
        # 查找最佳比对作为主要比对
        # 假设第一个比对是主要比对，或者根据错配数选择
        # 这里简化为第一个比对作为主要比对
        pass 
    
    # 确保索引在范围内
    if not record.ref_names or primary_idx >= len(record.ref_names):
        # 这应该不会发生，因为is_mapped为True时ref_names不应为空
        # 但为了健壮性，如果出现问题，可以作为未比对处理或跳过
        yield from fanse_to_sam_type(FANSeRecord(header=record.header, seq=record.seq, is_mapped=False))
        return

    # 获取主要比对信息
    # 修正：RNAME 只取 accession（第一个空格前），与 @SQ SN 一致且符合 SAM 规范
    primary_ref_name = _sam_ref_name(record.ref_names[primary_idx])
    primary_position = record.positions[primary_idx]
    primary_mismatches = record.mismatches[primary_idx] if record.mismatches else 0
    primary_strand = record.strands[primary_idx] if record.strands else 'F'
    
    is_reverse_strand = (primary_strand == 'R')
    is_primary_alignment = True # 当前处理的是主要比对

    # 计算CIGAR和MAPQ
    cigar = generate_cigar(record.alignment[primary_idx], is_reverse_strand) # 修正：第一个参数应为比对字符串
    mapq = calculate_mapq_advanced(record, primary_idx, is_primary=True) # 传递record和primary_idx

    # 计算FLAG
    flag = calculate_flag(record, is_reverse_strand, is_primary_alignment)

    # RNEXT, PNEXT, TLEN 占位符，由samtools fixmate修正
    rnext = '=' if (record.is_first_in_pair or record.is_second_in_pair) else '*'
    pnext = primary_position + 1 if (record.is_first_in_pair or record.is_second_in_pair) else 0
    tlen = 0

    # 修正：SAM 规范要求 FLAG 含 0x10（反向链）时，SEQ 必须为原始测序序列的
    # 反向互补。fanse3 存储的是原始测序序列（按参考正链方向），若直接输出，
    # IGV 会将正链方向的碱基与反链参考比对，导致反链 reads 显示大量错配
    # （与 bam_linux.py 旧实现的 reverse_complement 处理保持一致）
    out_seq = reverse_complement(record.seq) if is_reverse_strand else record.seq

    # 生成主要比对的SAM行
    sam_line_parts = [
        record.header,
        str(flag),
        primary_ref_name,
        str(primary_position + 1),  # SAM是1-based
        str(mapq),
        cigar,
        rnext,
        str(pnext),
        str(tlen),
        out_seq,
        qual,
        f"XM:i:{primary_mismatches}", # 添加XM标签
        f"XN:i:{record.multi_count}", # 添加XN标签
        f"NM:i:{calculate_nm(record.alignment[primary_idx])}", # 添加NM标签
        f"XS:i:{mapq}" # 添加XS标签
    ]
    if all_sa_tag_str:
        sam_line_parts.append(all_sa_tag_str)
    yield '\t'.join(sam_line_parts)

    # 处理辅助比对（如果存在）
    if record.is_multi and len(record.ref_names) > 1:
        for i in range(len(record.ref_names)):
            if i == primary_idx:
                continue # 跳过主要比对

            # 修正：辅助比对行 RNAME 同样只取 accession，与主比对/SA标签一致
            supp_ref_name = _sam_ref_name(record.ref_names[i])
            supp_position = record.positions[i]
            supp_mismatches = record.mismatches[i] if record.mismatches else 0
            supp_strand = record.strands[i] if record.strands else 'F'
            
            is_supp_reverse_strand = (supp_strand == 'R')
            is_supp_primary_alignment = False # 辅助比对

            # 辅助比对的FLAG需要设置0x100 (not primary alignment) 和 0x800 (supplementary alignment)
            supp_flag = calculate_flag(record, is_supp_reverse_strand, is_supp_primary_alignment)
            supp_flag |= 0x800 # 设置辅助比对标志

            supp_cigar = generate_cigar(record.alignment[i], is_supp_reverse_strand) # 修正：第一个参数应为比对字符串
            supp_mapq = calculate_mapq_advanced(record, i, is_primary=False) # 传递record和索引

            # 修正：辅助比对为反向链时 SEQ 同样需要反向互补（同主比对，SAM 0x10 规范）
            supp_out_seq = reverse_complement(record.seq) if is_supp_reverse_strand else record.seq

            supp_sam_line_parts = [
                record.header,
                str(supp_flag),
                supp_ref_name,
                str(supp_position + 1),
                str(supp_mapq),
                supp_cigar,
                rnext, # 辅助比对的RNEXT, PNEXT, TLEN与主要比对相同
                str(pnext),
                str(tlen),
                supp_out_seq,
                qual,
                f"XM:i:{supp_mismatches}", # 添加XM标签
                f"XN:i:{record.multi_count}", # 添加XN标签
                f"NM:i:{calculate_nm(record.alignment[i])}", # 添加NM标签
                f"XS:i:{supp_mapq}" # 添加XS标签
            ]
            if all_sa_tag_str:
                supp_sam_line_parts.append(all_sa_tag_str)
            yield '\t'.join(supp_sam_line_parts)

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
                current_length += len(line.strip()) # 修正：去除换行符再计算长度

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
        # 修正：@SQ SN 只取 accession（第一个空格前），与 RNAME 输出保持一致；
        # SAM 规范要求 SN 不含空白字符
        header_lines.append(f"@SQ\tSN:{_sam_ref_name(ref_name)}\tLN:{length}")

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
                    end = int(end_str.strip())  # 保持1-based，但实际使用时需要注意是包含还是不包含
                    
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

def fanse2sam(fanse_file: str, fasta_path: str, output_sam: Optional[str] = None,
              region: Optional[str] = None, console=None, threads: int = 4,
              unmapped_file: Optional[str] = None, is_paired_end: bool = False,
              ref_info_json: Optional[str] = None,
              _pipe_writer=None): # 新增_pipe_writer参数
    """
    将FANSe3文件转换为SAM格式，支持区域过滤和双端模式。

    参数:
        fanse_file: 输入FANSe3文件路径。
        fasta_path: 参考基因组FASTA文件路径。
        output_sam: 输出SAM文件路径(如果为None则打印到标准输出)。
        region: 区域过滤字符串。
        console: rich Console 对象，用于日志输出。
        threads: 并行处理的线程数。
        unmapped_file: 未比对reads文件路径（仅在is_paired_end为True时有效）。
        is_paired_end: 是否启用双端模式，将合并fanse_file和unmapped_file的记录。
        ref_info_json: 参考序列信息缓存JSON路径（新增，{序列名: 长度}）。
            提供时跳过逐行解析FASTA（数GB网络文件可节省数分钟），直接毫秒级读取缓存。
            缓存不可用（缺失/损坏）时自动回退为解析FASTA，不影响正确性。
        _pipe_writer: 内部参数，用于将输出写入到subprocess管道的stdin。
    """
    if console is None:
        console = Console(stderr=True)

    # 解析参考序列信息
    # 修正：支持 ref_info_json 缓存——bam 批量转换时同一 -r 的 header 只解析一次 FASTA，
    # 其余转换直接读缓存，避免每文件重复读取数 GB 网络文件
    ref_info = None
    if ref_info_json:
        try:
            import json
            with open(ref_info_json, 'r', encoding='utf-8') as jf:
                ref_info = json.load(jf)
            if not isinstance(ref_info, dict) or not ref_info:
                raise ValueError("缓存内容为空或格式错误")
            console.print(f"从缓存加载参考序列信息: {ref_info_json} ({len(ref_info)} 条序列)")
        except Exception as e:
            # 修正：缓存失效时回退为解析FASTA，保证正确性
            console.print(f"[bold yellow]警告: 参考序列缓存不可用({e})，回退为解析FASTA[/bold yellow]")
            ref_info = None
    if ref_info is None:
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
    
    # 统一输出处理逻辑
    writer = None
    should_close = False
    is_binary = False
    
    try:
        if _pipe_writer: # 如果提供了管道写入器，则使用它
            writer = _pipe_writer
            is_binary = True # 管道通常处理字节流
            writer.write(header.encode('utf-8'))
            writer.flush()
        elif output_sam:
            writer = open(output_sam, 'w', encoding='utf-8')
            should_close = True
            console.print('Write SAM header down.')
            writer.write(header)
        else:
            # 标准输出模式：使用buffer直接写入bytes，避免编码问题
            writer = sys.stdout.buffer
            is_binary = True
            writer.write(header.encode('utf-8'))
            writer.flush()  # 确保头部立即写入，防止samtools等待或超时

        # 核心处理逻辑（并行/串行）
        if threads > 1:
            if output_sam:
                console.print(f"启用并行处理: 使用 {threads} 个线程")
            
            from functools import partial
            
            # 调整批次大小
            batch_size = 20000 
            
            # 根据是否双端模式选择记录解析器
            if is_paired_end and unmapped_file:
                # 双端模式下，合并fanse和unmapped记录
                record_generator = _merge_fanse_and_unmapped_records(fanse_file, unmapped_file)
                # 估算文件大小，用于进度条
                file_read_size = (os.path.getsize(fanse_file) + os.path.getsize(unmapped_file)) / 450
            else:
                # 单端模式，只解析fanse文件
                record_generator = fanse_parser_high_performance(fanse_file)
                file_read_size = os.path.getsize(fanse_file) / 450
            
            reader = _iter_record_batches(record_generator, batch_size)
            worker_func = partial(_worker_process_records, regions=regions)
            
            # 进度条仅在输出到文件时显示，避免干扰stdout
            disable_pbar = (output_sam is None)
            
            with tqdm(total=file_read_size, unit='reads', mininterval=5, unit_scale=True, disable=disable_pbar) as pbar:
                with Pool(processes=threads) as pool:
                    for sam_lines_batch in pool.imap(worker_func, reader, chunksize=1):
                        if sam_lines_batch:
                            text = '\n'.join(sam_lines_batch) + '\n'
                            if is_binary:
                                writer.write(text.encode('utf-8'))
                            else:
                                writer.write(text)
                        # 修正意图：并行模式下按批次近似推进进度，避免固定错误步长
                        pbar.update(batch_size)
                            
            if output_sam:
                console.print(f"处理完成")
        
        else:
            # 单线程处理
            # 修正：写批次从 200_000 降到 20_000。multi-mapping 重的文件单条可产出
            # ~23KB SAM 文本（如 48 个比对位置全展开），200k 条/批内存峰值可达数 GB
            # 直接 MemoryError；20k 条/批峰值仅数百 MB，写盘频率增加对性能影响可忽略
            batch_size = 20_000
            batch_count = 0
            batch_lines = []
            filtered_count = 0
            total_count = 0
            
            # 根据是否双端模式选择记录解析器
            if is_paired_end and unmapped_file:
                record_generator = _merge_fanse_and_unmapped_records(fanse_file, unmapped_file)
                file_read_size = (os.path.getsize(fanse_file) + os.path.getsize(unmapped_file)) / 450
            else:
                record_generator = fanse_parser_high_performance(fanse_file)
                file_read_size = os.path.getsize(fanse_file) / 450
            
            # 进度条仅在输出到文件时显示
            disable_pbar = (output_sam is None)
            
            with tqdm(total=file_read_size, unit='reads', mininterval=5, unit_scale=True, disable=disable_pbar) as pbar:
                for record in record_generator:
                    total_count += 1
                    
                    if regions and not is_record_in_region(record, regions):
                        filtered_count += 1
                        pbar.update(1)
                        continue
                    
                    for sam_line in fanse_to_sam_type(record):
                        batch_lines.append(sam_line)
                        batch_count += 1
                    
                    if batch_count >= batch_size:
                        text = '\n'.join(batch_lines) + '\n'
                        if is_binary:
                            writer.write(text.encode('utf-8'))
                        else:
                            writer.write(text)
                        batch_lines = []
                        batch_count = 0
                    
                    pbar.update(1)
                
                # 写入剩余批次
                if batch_lines:
                    text = '\n'.join(batch_lines) + '\n'
                    if is_binary:
                        writer.write(text.encode('utf-8'))
                    else:
                        writer.write(text)
            
            if output_sam:
                console.print(f"处理完成: 总共 {total_count} 条记录，过滤 {filtered_count} 条，输出 {total_count - filtered_count} 条")

    except (BrokenPipeError, OSError) as e:
        # 处理管道断开错误（例如 | head 或 samtools 提前退出）
        # 此时应静默退出，不需要打印错误堆栈
        # errno 32 是 Broken pipe
        if isinstance(e, BrokenPipeError) or (hasattr(e, 'errno') and e.errno == 32):
            try:
                sys.stderr.close()
            except:
                pass
            sys.exit(0)
        else:
            raise e
            
    finally:
        if should_close and writer: # 只有当writer是我们自己打开的文件时才关闭
            writer.close()

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
        # 如果启用了 --fixmate，则强制输出到标准输出，以便管道传递给samtools
        if args.fixmate:
            output_path = None
        elif output_dir:
            fname = input_file.stem
            output_path = os.path.join(output_dir, fname + '.sam')
        elif args.output_sam:
            output_path = args.output_sam
        else:
            # 修正意图：双端模式默认落盘到输入文件同目录，避免大量SAM内容直接刷到终端
            if args.is_paired_end:
                output_path = str(input_file.with_suffix('.sam'))
            else:
                output_path = None # stdout

        if len(input_files) > 1:
            console.print(f"[dim]Processing ({i+1}/{len(input_files)}): {input_file.name}[/dim]")

        try:
            # 在这里处理unmapped_file的自动推断
            current_unmapped_file = None
            if args.is_paired_end:
                # 尝试推断unmapped文件路径
                # 假设unmapped文件和fanse文件在同一目录，且文件名只有后缀不同
                unmapped_candidate = input_file.with_suffix('.unmapped')
                if unmapped_candidate.exists():
                    current_unmapped_file = str(unmapped_candidate)
                else:
                    console.print(f"[bold yellow]警告: 双端模式下未找到 {input_file.name} 对应的 .unmapped 文件，将只处理 .fanse3 文件[/bold yellow]")

            # 如果启用了 --fixmate，则通过管道将fanse2sam的输出传递给samtools
            if args.fixmate:
                # 获取samtools路径
                samtools_path = processor.get_samtools_path(console=console)
                if not samtools_path:
                    # 错误信息已在get_samtools_path中打印
                    return

                # 构建samtools sort -n 命令
                sort_cmd = [samtools_path, 'sort', '-n', '-@', str(args.threads), '-o', '-', '-']
                # 构建samtools fixmate 命令
                fixmate_cmd = [samtools_path, 'fixmate', '-@', str(args.threads), '-', output_path if output_path else '-']

                console.print(f"[dim]执行管道命令: fanse sam ... | {' '.join(sort_cmd)} | {' '.join(fixmate_cmd)}[/dim]")

                # 启动samtools sort -n 进程
                sort_process = subprocess.Popen(sort_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # 启动samtools fixmate 进程
                fixmate_process = subprocess.Popen(fixmate_cmd, stdin=sort_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # 关闭sort_process的stdout，确保它不会阻塞
                sort_process.stdout.close()

                # 调用fanse2sam，将输出写入sort_process的stdin
                fanse2sam(input_path,
                          args.fasta_path,
                          output_sam=None, # 强制输出到stdout，由管道捕获
                          region=args.region,
                          console=console,
                          threads=args.threads,
                          unmapped_file=current_unmapped_file,
                          is_paired_end=args.is_paired_end,
                          ref_info_json=getattr(args, 'ref_info_json', None), # 新增：header缓存透传
                          _pipe_writer=sort_process.stdin # 将stdin传递给fanse2sam
                          )
                sort_process.stdin.close() # 关闭stdin，通知sort进程输入结束

                # 读取fixmate的输出并写入最终目的地
                if output_path:
                    with open(output_path, 'wb') as out_f:
                        for line in fixmate_process.stdout:
                            out_f.write(line)
                else:
                    # 如果output_path为None，则fixmate的输出直接到stdout
                    for line in fixmate_process.stdout:
                        sys.stdout.buffer.write(line)
                
                # 检查samtools进程的返回码
                sort_stderr = sort_process.communicate()[1].decode('utf-8')
                fixmate_stderr = fixmate_process.communicate()[1].decode('utf-8')

                if sort_process.returncode != 0:
                    console.print(f"[bold red]错误: samtools sort -n 进程异常退出，返回码 {sort_process.returncode}[/bold red]")
                    console.print(f"[dim]Stderr: {sort_stderr}[/dim]")
                    return
                if fixmate_process.returncode != 0:
                    console.print(f"[bold red]错误: samtools fixmate 进程异常退出，返回码 {fixmate_process.returncode}[/bold red]")
                    console.print(f"[dim]Stderr: {fixmate_stderr}[/dim]")
                    return
                
                console.print("[bold green]samtools sort -n | fixmate 管道处理完成。[/bold green]")

            else:
                # 正常调用fanse2sam
                fanse2sam(input_path,
                          args.fasta_path,
                          output_path,
                          region=args.region,
                          console=console,
                          threads=args.threads,
                          unmapped_file=current_unmapped_file,
                          is_paired_end=args.is_paired_end,
                          ref_info_json=getattr(args, 'ref_info_json', None) # 新增：header缓存透传
                          )
        except Exception as e:
            console.print(f"[bold red]Error processing {input_path}: {e}[/bold red]")


def add_sam_subparser(subparsers):
    sam_parser = subparsers.add_parser(
        'sam',
        help='转换为 SAM 格式',
        description='将FANSe3比对结果转换为SAM格式，支持区域过滤和双端模式。',
        formatter_class=CustomHelpFormatter
    )
    sam_parser.add_argument('-i', '--fanse-file', required=True,
                            help='输入FANSe3文件路径 (支持通配符和目录)')
    sam_parser.add_argument('-r', '--fasta-path', required=True,
                            help='参考基因组FASTA文件路径')
    sam_parser.add_argument('-o', '--output-sam',
                            help='输出SAM文件路径 (如果为None则打印到标准输出)')
    sam_parser.add_argument('-R', '--region',
                            help='区域过滤，例如 "chr1:1000-2000,chr2"')
    sam_parser.add_argument('-t', '--threads', type=int, default=4,
                            help='并行处理的线程数 (仅在输出到文件时有效)')
    sam_parser.add_argument('--pe', '--paired-end', action='store_true', dest='is_paired_end',
                            help='启用双端模式。将自动搜索同目录下的 .unmapped 文件进行合并处理。')
    sam_parser.add_argument('--fixmate', action='store_true',
                            help='在输出SAM后，自动通过管道执行 samtools sort -n | samtools fixmate。此选项会强制输出到标准输出。')
    # 新增：参考序列信息缓存JSON（{序列名: 长度}），跳过逐行解析FASTA生成header
    sam_parser.add_argument('--ref-info-json',
                            help='（可选）参考序列信息缓存JSON路径，格式为 {"序列名": 长度}。'
                                 '提供时跳过逐行解析FASTA（数GB文件可节省数分钟），直接毫秒级读取缓存生成@SQ header；'
                                 '缓存缺失或损坏时自动回退为解析FASTA。可由 fanse bam 批量转换自动生成，'
                                 '也可用 fansetools.sam.parse_fasta 手动生成')
    sam_parser.set_defaults(func=run_sam_command)

    add_rich_epilog(sam_parser, """
[bold]功能说明:[/bold]
  本工具将 FANSe3 比对结果转换为标准 SAM 格式。
  支持多线程并行处理，支持区域过滤。
  在 Linux 环境下，如果不指定 [green]-o[/green] 参数，结果将输出到标准输出 (stdout)，
  可以直接通过管道传递给 samtools 进行处理 (如转换为 BAM)。

[bold]区域格式 (-R/--region):[/bold]
  [green]chr1[/green]             整个染色体
  [green]chr1:1000-[/green]       从 1000bp 开始到末尾
  [green]chr1:1000-2000[/green]   指定区间
  [green]chr1,chr2[/green]        多个染色体

[bold]示例:[/bold]
  1. 基本转换:
     [green]fanse sam -i sample.fanse3 -r ref.fa -o sample.sam[/green]

  2. 管道操作 (Linux):
     [green]fanse sam -i sample.fanse3 -r ref.fa | samtools view -bS - > sample.bam[/green]

  3. 区域过滤:
     [green]fanse sam -i sample.fanse3 -r ref.fa -R chr1:1000-2000 -o filtered.sam[/green]
""")




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
    
