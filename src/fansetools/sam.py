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
import tempfile
import shutil
import time  # 修正：网络盘预加载计时用，从内层函数 import 提到顶层
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
    # 加速：避免 split() 创建列表，用 find + slice 直接取第一个空格前的 accession
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

# 修正：双端模式需要发现 R2 对应的 fanse3 和 unmapped 文件，
# 然后四文件合并才能让 fixmate 找到配对

def _discover_paired_fanse_files(input_path):
    """
    修正：从 R1 输入文件自动推断 R2 的 fanse3 和 unmapped 路径。

    典型命名约定（Illumina/BGI）：
      R1: CQ304891_S16_L001_R1_001.fanse3
      R2: CQ304891_S16_L001_R2_001.fanse3
    或简写：
      R1: sample_R1.fanse3 / sample_R1.unmapped
      R2: sample_R2.fanse3 / sample_R2.unmapped
      R1: sample_1.fanse3 / sample_1.unmapped
      R2: sample_2.fanse3 / sample_2.unmapped

    参数:
        input_path: 输入的 R1 fanse3 文件路径（Path 对象或字符串）

    返回:
        (r1_fanse3, r1_unmapped, r2_fanse3, r2_unmapped) 四个 Path 对象；
        R2 文件不存在时对应位置为 None。
    """
    import pathlib
    p = pathlib.Path(input_path)
    parent = p.parent
    stem = p.stem  # e.g. "CQ304891_S16_L001_R1_001"
    suffix = p.suffix  # e.g. ".fanse3"

    # 替换 R1 → R2 的多种模式（按优先级尝试）
    pair_swap_patterns = [
        ('_R1_', '_R2_'),  # e.g. _R1_001 → _R2_001
        ('_R1.', '_R2.'),
        ('-R1-', '-R2-'),
        ('-R1.', '-R2.'),
        ('_R1', '_R2'),
        ('-R1', '-R2'),
        ('_1.', '_2.'),
        ('_1_', '_2_'),
        ('-1.', '-2.'),
        ('_1', '_2'),
    ]

    r2_fanse3 = None
    for old, new in pair_swap_patterns:
        if old in stem:
            candidate_stem = stem.replace(old, new)
            candidate = parent / (candidate_stem + suffix)
            if candidate.exists():
                r2_fanse3 = candidate
                break

    # 如果 stem 本身含 "R1" 且上面没匹配到（如 "sample_R1"），再试一次
    if r2_fanse3 is None and 'R1' in stem:
        candidate = parent / (stem.replace('R1', 'R2') + suffix)
        if candidate.exists():
            r2_fanse3 = candidate

    # 发现 unmapped 文件
    r1_unmapped_candidate = parent / (stem + '.unmapped')
    r1_unmapped = r1_unmapped_candidate if r1_unmapped_candidate.exists() else None

    r2_unmapped = None
    if r2_fanse3:
        r2_unmapped_candidate = parent / (r2_fanse3.stem + '.unmapped')
        r2_unmapped = r2_unmapped_candidate if r2_unmapped_candidate.exists() else None

    return p, r1_unmapped, r2_fanse3, r2_unmapped


def _merge_four_streams(r1_fanse3, r1_unmapped, r2_fanse3, r2_unmapped):
    """
    修正：四文件合并生成统一的 FANSeRecord 流（双端模式核心）。

    顺序：R1.mapped → R1.unmapped → R2.mapped → R2.unmapped
    每个 record 的 read name 通过 _standardize_read_name 自动标准化，
    R1 会标记 is_first_in_pair=True，R2 标记 is_second_in_pair=True，
    为下游 samtools fixmate 提供正确的配对依据。

    不使用 list() 全量加载，保持流式内存效率。

    加速：使用 fanse_parser_high_performance 替代普通 fanse_parser，
    减少字符串操作开销。
    """
    # R1 比对上的
    if r1_fanse3 and os.path.exists(r1_fanse3):
        # 修正：用高性能 parser 替代普通 parser，减少 CPU 开销
        for record in fanse_parser_high_performance(str(r1_fanse3)):
            yield record
    # R1 未比对上的
    if r1_unmapped and os.path.exists(r1_unmapped):
        for record in unmapped_parser(str(r1_unmapped)):
            yield record
    # R2 比对上的
    if r2_fanse3 and os.path.exists(r2_fanse3):
        # 修正：用高性能 parser 替代普通 parser
        for record in fanse_parser_high_performance(str(r2_fanse3)):
            yield record
    # R2 未比对上的
    if r2_unmapped and os.path.exists(r2_unmapped):
        for record in unmapped_parser(str(r2_unmapped)):
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

    修正（基因组证据验证，CQ1-1 实测）:
        1. FANSe 对齐串操作符语义: '.'=匹配(M), 'x'=错配(X),
           '-'=参考有而read缺失 → 缺失D(仅消耗参考),
           字母ACGT=read有而参考缺失 → 插入I(仅消耗查询)。
           旧实现把 '-' 映射为 I，导致 CIGAR 查询长度 ≠ SEQ 长度，
           samtools 报 "CIGAR and query sequence are of different length"。
        2. 反向链(-R) alignment 串按 read 5'→3' 方向记录，与写出 SEQ
           (revcomp，参考正向) 方向相反，必须先反转串再生成 CIGAR，
           否则 X/I/D 位置全部镜像错位（与 bam_linux.py 旧实现一致）。
    """
    if not alignment:
        return ""

    # 修正2: 反向链比对串按read方向记录, 先翻转为参考正向
    if is_reverse:
        alignment = alignment[::-1]

    def _op(ch: str) -> str:
        # 修正1: '-'=缺失D(仅消耗参考), 字母=插入I(仅消耗查询)
        # 注意: 'x' 也是字母, 必须在 isalpha 之前判断
        if ch == '.':
            return 'M'
        if ch == 'x':
            return 'X'
        if ch == '-':
            return 'D'
        if ch.isalpha():
            return 'I'
        return 'M'  # 未知符号兜底按M处理

    cigar_parts = []
    count = 0
    prev_op = None
    # 修正: 按操作符(而非字符)分组, 使相邻不同字母(如AC)正确合并为连续I
    for char in alignment:
        op = _op(char)
        if op == prev_op:
            count += 1
        else:
            if prev_op is not None:
                cigar_parts.append(f"{count}{prev_op}")
            prev_op = op
            count = 1
    if prev_op is not None:
        cigar_parts.append(f"{count}{prev_op}")

    return ''.join(cigar_parts)

def calculate_flag(record: FANSeRecord, is_reverse_strand: bool, is_primary_alignment: bool) -> int:
    """
    根据FANSeRecord信息计算SAM FLAG。

    设计要点：
    - 单端场景：record.is_first_in_pair / is_second_in_pair 均为 None → 不加任何配对位
    - 双端合并场景（方案 A）：parser 自动 strip read name 并标记 R1/R2 → 本函数设置
      0x1(1)/0x40(64)/0x80(128)；0x2(2)/0x8(8)/0x20(32)/RNEXT/PNEXT/TLEN 留给下游 samtools fixmate 补全
    - 与 fixmate 协同：本函数设好"是 paired + 是 R1 还是 R2"，fixmate 负责算精确的
      mate 位置和 properly paired 判断

    修正历史：
    - v1: 全注释 0x1(1)/0x40(64)/0x80(128)（单端输出时代）
    - v2: 启用配对位（双端合并时代，parser 已自动标记）
    """
    flag = 0

    # 0x1(1): read is paired in sequencing
    # 修正：双端合并时 parser 已标记 is_first / is_second → 这里设 paired 位
    # 单端时两个字段都是 None → 不设，保持原单端输出行为
    if record.is_first_in_pair is not None or record.is_second_in_pair is not None:
        flag |= 0x1

    # 0x40(64): read is the first in a pair
    if record.is_first_in_pair:
        flag |= 0x40

    # 0x80(128): read is the second in a pair
    if record.is_second_in_pair:
        flag |= 0x80

    # 0x2(2): read mapped in proper pair — 保持 0，fixmate 会根据 R1/R2 比对位置判断
    # 0x8(8): mate unmapped — 保持 0，fixmate 会根据 mate 是否比对上补
    # 0x20(32): mate reverse strand — 保持 0，fixmate 会根据 mate 方向补

    # 0x4(4): read is unmapped
    if not record.is_mapped:
        flag |= 0x4

    # 0x10(16): read reverse strand
    if is_reverse_strand:
        flag |= 0x10

    # 0x100(256): not primary alignment
    if not is_primary_alignment:
        flag |= 0x100

    # 0x200(512): read fails platform/vendor quality checks (FANSe3无此信息，默认为通过)
    # 0x400(1024): read is PCR or optical duplicate (FANSe3无此信息)
    # 0x800(2048): supplementary alignment (由fanse_to_sam_type处理)

    return flag
 

def calculate_nm(alignment: str) -> int:
    """计算编辑距离（错配+插入+缺失）"""
    nm = 0
    for char in alignment:
        if char == 'x':  # 错配
            nm += 1
        elif char == '-':  # 缺失（参考有read无）
            nm += 1
        elif char == '+':  # 兼容旧版fanse标记
            nm += 1
        elif char != '.' and char.isalpha():  # 修正：插入碱基(ACGT)也计入编辑距离
            nm += 1
    return nm


def _calculate_alignment_metrics(alignment: str, is_reverse: bool,
                                 multi_count: int, is_primary: bool,
                                 scoring_system: dict = None) -> Tuple[str, int, int]:
    """单次扫描计算 CIGAR、NM 和 MAPQ，保持旧函数的评分口径。"""
    if scoring_system is None:
        scoring_system = {
            'match_score': 2, 'mismatch_penalty': -4,
            'gap_open_penalty': -6, 'gap_extend_penalty': -1,
            'min_mapq': 0, 'max_mapq': 60
        }

    def operation(char: str) -> str:
        if char == '.':
            return 'M'
        if char == 'x':
            return 'X'
        if char == '-':
            return 'D'
        if char.isalpha():
            return 'I'
        return 'M'

    # 保持旧实现语义：MAPQ/NM 按原始 alignment 方向计算，CIGAR 反向链才翻转。
    score = 0
    nm = 0
    non_gap_length = 0
    gap_open = False
    cigar_parts = []
    previous_op = None
    op_count = 0
    cigar_source = reversed(alignment) if is_reverse else iter(alignment)

    for char in alignment:
        if char == 'x' or char == '-' or char == '+' or (char != '.' and char.isalpha()):
            nm += 1
        if char != '-':
            non_gap_length += 1
        if char == '.':
            score += scoring_system['match_score']
            gap_open = False
        elif char == 'x':
            score += scoring_system['mismatch_penalty']
            gap_open = False
        elif char == '-':
            if not gap_open:
                score += scoring_system['gap_open_penalty']
                gap_open = True
            else:
                score += scoring_system['gap_extend_penalty']
        else:
            if not gap_open:
                score += scoring_system['gap_open_penalty']
                gap_open = True
            else:
                score += scoring_system['gap_extend_penalty']

    for char in cigar_source:
        op = operation(char)
        if op == previous_op:
            op_count += 1
        else:
            if previous_op is not None:
                cigar_parts.append(f'{op_count}{previous_op}')
            previous_op = op
            op_count = 1

    if previous_op is not None:
        cigar_parts.append(f'{op_count}{previous_op}')

    if non_gap_length == 0:
        mapq = scoring_system['min_mapq']
    else:
        score_ratio = score / (non_gap_length * scoring_system['match_score'])
        raw_mapq = (-10 * math.log10(1 - score_ratio)
                    if score_ratio < 1 else scoring_system['max_mapq'])
        if multi_count > 1:
            raw_mapq -= min(60, math.log2(multi_count) * 5)
        if not is_primary:
            raw_mapq *= 0.6
        raw_mapq = max(scoring_system['min_mapq'],
                       min(scoring_system['max_mapq'], raw_mapq))
        mapq = discretize_mapq(raw_mapq)

    return ''.join(cigar_parts), nm, mapq


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

def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:  #20251024第二次优化
    """
    将FANSeRecord对象转换为SAM格式的字符串。
    现在支持FANSeRecord中包含的双端信息和未比对信息。

    优化说明 (v3):
      - P0 SA:Z 预计算：先算好每个比对位置的 SA entry 字符串（ref,pos,strand,cigar,mapq,nm），
        主比对行拼 SA=排除 primary 的所有 entry；辅助行拼 SA=排除自身的所有 entry。
        旧实现每个 SAM 行都重算 O(N) 次属性 → N 行 SAM × N 比对 = O(N²)。现在 O(N) 预计算 + O(N) join。
      - P1 multi 属性缓存：主比对和辅助比对共用一个预计算表（ref_name/pos/strand/is_reverse/cigar/mapq/nm），
        避免 generate_cigar / calculate_mapq / calculate_nm 对同一个比对串重复算 N 次。
      - 预期提速 2-4x（multi-mapping 重的样本效果更显著）
    """
    # 质量值（FANSe3不提供，默认为所有位置'I'）
    qual = 'I' * len(record.seq)

    # QNAME 标准化：去除 " 1:N:0:" Illumina 后缀
    qname = getattr(record, 'read_pair_id', None) or (
        record.header.split()[0] if record.header.split() else record.header)

    # RNEXT, PNEXT, TLEN 占位符，由 samtools fixmate 修正
    rnext = '*'
    pnext = 0
    tlen = 0

    # ── 未比对 read：只有一行 SAM，不需要多位置展开 ──
    if not record.is_mapped:
        flag = calculate_flag(record, is_reverse_strand=False, is_primary_alignment=True)
        sam_line = '\t'.join([qname, str(flag), '*', '0', '0', '*', rnext, str(pnext), str(tlen), record.seq, qual])
        yield sam_line
        return

    # ── 比对上的 read：预计算所有比对位置属性 ──
    primary_idx = 0
    if not record.ref_names or primary_idx >= len(record.ref_names):
        yield from fanse_to_sam_type(FANSeRecord(header=record.header, seq=record.seq, is_mapped=False))
        return

    n_align = len(record.ref_names)

    # ═══ P0+P1 核心优化：一次性预计算所有比对位置的属性 ═══
    # 避免在主比对 + N-1 辅助比对的 SAM 行里重复 generate_cigar / calculate_mapq / calculate_nm / _sam_ref_name
    # 以及重复构造 SA:Z 标签的 O(N²) 开销
    sa_entries = []      # 每个比对位置的 SA 子串（不含前缀 "SA:Z:"），长度 = n_align
    ref_names_sam = []   # 规范化后的参考名（accession）
    positions_0based = []
    cigars = []
    mapqs = []
    nms = []
    flags = []           # 各位置的 FLAG（不含 0x100/0x800 辅助修正）
    out_seqs = []        # 各位置的 SEQ（反向链已 revcomp）

    # 修正：同一条 read 的多个反向比对共享一次反向互补结果。
    reverse_seq = None
    if any(strand == 'R' for strand in (record.strands or [])):
        reverse_seq = reverse_complement(record.seq)

    for i in range(n_align):
        ref_nm = record.mismatches[i] if record.mismatches else 0
        strand_i = record.strands[i] if record.strands else 'F'
        rev_i = (strand_i == 'R')

        cig, nm_i, mapq_i = _calculate_alignment_metrics(
            record.alignment[i],
            rev_i,
            record.multi_count,
            is_primary=(i == primary_idx),
        )

        # SA entry 格式: ref,pos1based,+|-,cigar,mapq,nm
        sa_entries.append(
            f"{_sam_ref_name(record.ref_names[i])},{record.positions[i]+1},{('-' if rev_i else '+')},{cig},{mapq_i},{nm_i}"
        )

        ref_names_sam.append(_sam_ref_name(record.ref_names[i]))
        positions_0based.append(record.positions[i])
        cigars.append(cig)
        mapqs.append(mapq_i)
        nms.append(nm_i)
        flags.append(calculate_flag(record, rev_i, is_primary_alignment=(i == primary_idx)))
        out_seqs.append(reverse_seq if rev_i else record.seq)

    # SA:Z 完整标签（排除指定 index 的 entry）
    def _sa_tag_excluding(exclude_idx: int) -> str:
        """快速生成：跳过 exclude_idx，join 剩余 sa_entries"""
        if n_align <= 1:
            return ""
        parts = [sa_entries[i] for i in range(n_align) if i != exclude_idx]
        return f"SA:Z:{';'.join(parts)};"

    # ═══ 预计算结束，开始生成 SAM 行 ═══

    # ── 主比对行 ──
    out_seq_primary = out_seqs[primary_idx]
    sam_line_parts = [
        qname,
        str(flags[primary_idx]),          # 预计算好的 primary FLAG
        ref_names_sam[primary_idx],
        str(positions_0based[primary_idx] + 1),
        str(mapqs[primary_idx]),
        cigars[primary_idx],
        rnext,
        str(pnext),
        str(tlen),
        out_seq_primary,
        qual,
        f"XM:i:{record.mismatches[primary_idx] if record.mismatches else 0}",
        f"XN:i:{record.multi_count}",
        f"NM:i:{nms[primary_idx]}",
        f"XS:i:{mapqs[primary_idx]}"
    ]
    sa_tag = _sa_tag_excluding(primary_idx)
    if sa_tag:
        sam_line_parts.append(sa_tag)
    yield '\t'.join(sam_line_parts)

    # ── 辅助比对行（跳过 primary_idx）──
    if n_align > 1:
        for i in range(n_align):
            if i == primary_idx:
                continue
            supp_flag = flags[i] | 0x100 | 0x800  # not_primary + supplementary
            supp_sam_line_parts = [
                qname,
                str(supp_flag),
                ref_names_sam[i],
                str(positions_0based[i] + 1),
                str(mapqs[i]),
                cigars[i],
                rnext,
                str(pnext),
                str(tlen),
                out_seqs[i],
                qual,
                f"XM:i:{record.mismatches[i] if record.mismatches else 0}",
                f"XN:i:{record.multi_count}",
                f"NM:i:{nms[i]}",
                f"XS:i:{mapqs[i]}"
            ]
            supp_sa = _sa_tag_excluding(i)
            if supp_sa:
                supp_sam_line_parts.append(supp_sa)
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

def generate_sam_header_from_ref_info(ref_info: Dict[str, int], is_paired_end: bool = False) -> str:
    """从参考信息生成SAM头部，单端/双端模式在 @PG CL 字段中做区分标识"""
    pe_tag = "_PE" if is_paired_end else "_SE"
    header_lines = [
        "@HD\tVN:1.6\tSO:unsorted",
        f"@PG\tID:fanse3{pe_tag}\tPN:fanse3\tVN:3.0\tCL:fanse3 -pe {is_paired_end}"
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


def _normalize_output_sam_name(input_file: pathlib.Path, is_paired_end: bool) -> str:
    """规范化SAM输出文件名；双端模式统一去掉R1/R2后缀并追加_PE。"""
    stem = input_file.stem
    if stem.endswith('.fanse3') or stem.endswith('.fanse'):
        stem = pathlib.Path(stem).stem
    if is_paired_end:
        for suffix in ['_R1_001', '_R2_001', '_R1', '_R2', '.R1', '.R2', '-R1', '-R2', '_1', '_2']:
            if stem.endswith(suffix):
                stem = stem[:-len(suffix)]
                break
        stem = stem.rstrip('_-.') + '_PE'
    return stem + '.sam'

def fanse2sam(fanse_file: str, fasta_path: str, output_sam: Optional[str] = None,
              region: Optional[str] = None, console=None, threads: int = 4,
              unmapped_file: Optional[str] = None, is_paired_end: bool = False,
              ref_info_json: Optional[str] = None,
              r2_fanse3: Optional[str] = None, r2_unmapped: Optional[str] = None,
              preload_network: bool = False,  # 修正：网络盘预加载从默认启用改为显式选项
              _pipe_writer=None): # 内部参数，用于将输出写入到subprocess管道的stdin
    """
    将FANSe3文件转换为SAM格式，支持区域过滤和双端模式。

    参数:
        fanse_file: 输入FANSe3文件路径（通常是 R1）。
        fasta_path: 参考基因组FASTA文件路径。
        output_sam: 输出SAM文件路径(如果为None则打印到标准输出)。
        region: 区域过滤字符串。
        console: rich Console 对象，用于日志输出。
        threads: 并行处理的线程数。
        unmapped_file: R1 未比对 reads 文件路径。
        is_paired_end: 是否启用双端模式。
        ref_info_json: 参考序列信息缓存JSON路径。
        r2_fanse3: 修正：R2 fanse3 文件路径（双端模式必需）。
            传入后四文件合并（R1.mapped + R1.unmapped + R2.mapped + R2.unmapped），
            为下游 samtools fixmate 提供正确的配对数据源。
            若为 None 则回退为原"假双端"行为（仅合并 R1 fanse3 + R1 unmapped）。
        r2_unmapped: 修正：R2 unmapped 文件路径（双端模式可选）。
        _pipe_writer: 内部参数，用于将输出写入到subprocess管道的stdin。
    """
    if console is None:
        console = Console(stderr=True)

    # 解析参考序列信息
    # 修正：支持 ref_info_json 缓存；若未显式传入，则自动探测参考基因组同目录的 <fasta>.ref_info.json
    ref_info = None
    auto_ref_info_json = None
    if not ref_info_json:
        fasta_p = pathlib.Path(fasta_path)
        auto_ref_info_json = str(fasta_p.with_name(fasta_p.name + '.ref_info.json'))
        if os.path.exists(auto_ref_info_json) and os.path.getsize(auto_ref_info_json) > 0:
            ref_info_json = auto_ref_info_json
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
        # 修正：local_temp_dir 必须在 try 开头初始化——finally 里无条件引用
        # 如果走并行分支（threads>1），原代码没定义这个变量会导致 NameError
        local_temp_dir = None

        # 修正：网络盘预加载从默认启用改为 --preload 显式选项
        # 移到并行/串行分支之前，让两条路径都能受益
        preload_map = {}
        if preload_network:
            def _is_network_path(p):
                """检测 UNC 网络盘路径或 NFS 挂载路径"""
                if not p:
                    return False
                sp = str(p)
                if sp.startswith('\\\\') or sp.startswith('//'):
                    return True
                if sp.startswith('/mnt/') or sp.startswith('/nfs/'):
                    return True
                return False

            data_files = [fanse_file, unmapped_file, r2_fanse3, r2_unmapped]
            unc_files = [p for p in data_files if p and _is_network_path(p)]
            if unc_files:
                local_temp_dir = tempfile.mkdtemp(prefix='fanse3_preload_')
                console.print(f"[bold yellow]--preload: {len(unc_files)} 个 UNC 文件预加载到本地 {local_temp_dir}[/bold yellow]")
                t0 = time.time()
                for src in unc_files:
                    dst = os.path.join(local_temp_dir, os.path.basename(str(src)))
                    shutil.copy2(str(src), dst)
                    preload_map[str(src)] = dst
                    console.print(f"  预加载: {os.path.basename(str(src))} ({os.path.getsize(dst)/1024/1024:.1f} MB)")
                console.print(f"[bold green]预加载完成: {time.time()-t0:.1f}s[/bold green]")

            def _localize(p):
                """把路径解析为预加载后的本地副本（如果存在）"""
                if not p:
                    return p
                return preload_map.get(str(p), str(p))
            fanse_file = _localize(fanse_file)
            unmapped_file = _localize(unmapped_file)
            r2_fanse3 = _localize(r2_fanse3)
            r2_unmapped = _localize(r2_unmapped)

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
        # 修正：双端模式默认走单线程流式输出，避免 Pool + 大批次在合并 unmapped 时显著降速
        # 单端且 threads>1 时才保留并行批处理；双端/配对修复场景以稳定优先
        if threads > 1 and not (is_paired_end and unmapped_file):
            if output_sam:
                console.print(f"启用并行处理: 使用 {threads} 个线程")
            
            from functools import partial
            
            # 调整批次大小
            batch_size = 20_000 
            
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
            # ═══════════════════════════════════════════════════════
            # 串行分支：两遍独立 fanse_to_sam_type
            # 改动意图：
            #  1. 双端：R1 全处理完 → 追加写 R2，用同一个 writer
            #     fixmate 按 read name 配对——R1/R2 同名 + 同 writer 输出 = sort -n 后紧挨 = fixmate 能配对
            #  2. 单端：默认 fanse3 + unmapped；没有 unmapped 就提示一下继续跑
            #  3. 干掉 _merge_four_streams 这种"四文件交错 generator"——没必要，两遍独立更直白
            #  4. 进度条在所有流启动前算好 total，全程跑一个条
            # ═══════════════════════════════════════════════════════
            batch_size = 20_000
            batch_count = 0
            batch_lines = []
            filtered_count = 0
            total_count = 0

            # ── 1. 计算进度条 total（先算好再开进度条）
            file_read_size = 0
            if is_paired_end and r2_fanse3 and os.path.exists(str(r2_fanse3)):
                # 真双端：R1 + R2 fanse3 + unmapped 全部加起来
                console.print(f"[bold blue]双端合并: R1 + R2 fanse3 + unmapped（两遍独立处理）[/bold blue]")
                console.print(f"  R1 fanse3 : {fanse_file}")
                console.print(f"  R1 unmapped: {unmapped_file or '(无)'}")
                console.print(f"  R2 fanse3 : {r2_fanse3}")
                console.print(f"  R2 unmapped: {r2_unmapped or '(无)'}")
                for p in [fanse_file, unmapped_file, r2_fanse3, r2_unmapped]:
                    if p and os.path.exists(str(p)):
                        file_read_size += os.path.getsize(str(p)) / 450
            elif is_paired_end and r2_fanse3:
                # 双端但 R2 文件不存在——假双端，警告
                console.print(f"[bold yellow]警告: 双端模式但未发现 R2 fanse3 文件 ({r2_fanse3})，仅处理 R1[/bold yellow]")
                unmapped_path = unmapped_file if unmapped_file and os.path.exists(str(unmapped_file)) else None
                if unmapped_path:
                    file_read_size = (os.path.getsize(str(fanse_file)) + os.path.getsize(str(unmapped_path))) / 450
                else:
                    file_read_size = os.path.getsize(str(fanse_file)) / 450
            else:
                # 单端：默认 fanse3 + unmapped
                unmapped_path = unmapped_file if unmapped_file and os.path.exists(str(unmapped_file)) else None
                if unmapped_path:
                    file_read_size = (os.path.getsize(str(fanse_file)) + os.path.getsize(str(unmapped_path))) / 450
                else:
                    console.print(f"[dim]未检测到 unmapped 文件，仅处理 mapped reads[/dim]")
                    file_read_size = os.path.getsize(str(fanse_file)) / 450

            # ── 2. 定义闭包：读 record generator → 转 SAM → 批次写
            # 共享外层的 batch_count/batch_lines/total_count/filtered_count 变量
            def _consume_stream(record_gen):
                """消费一个 FANSeRecord generator，转换为 SAM 行并写入 writer"""
                nonlocal total_count, filtered_count, batch_count, batch_lines
                for record in record_gen:
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

            # ── 3. 进度条 + 两遍独立消费
            disable_pbar = (output_sam is None)
            with tqdm(total=file_read_size, unit='reads', mininterval=5, unit_scale=True, disable=disable_pbar) as pbar:

                if is_paired_end and r2_fanse3 and os.path.exists(str(r2_fanse3)):
                    # 真双端：先 R1，后 R2，用同一个 writer 追加写
                    console.print(f"[dim]遍 1/2: 处理 R1 (fanse3 + unmapped)[/dim]")
                    _consume_stream(_merge_fanse_and_unmapped_records(fanse_file, unmapped_file))
                    console.print(f"[dim]遍 2/2: 处理 R2 (fanse3 + unmapped)[/dim]")
                    _consume_stream(_merge_fanse_and_unmapped_records(r2_fanse3, r2_unmapped))

                elif is_paired_end:
                    # 假双端：R2 不存在，只处理 R1
                    unmapped_path = unmapped_file if unmapped_file and os.path.exists(str(unmapped_file)) else None
                    if unmapped_path:
                        _consume_stream(_merge_fanse_and_unmapped_records(fanse_file, unmapped_path))
                    else:
                        _consume_stream(fanse_parser_high_performance(fanse_file))

                else:
                    # 单端：默认 fanse3 + unmapped
                    unmapped_path = unmapped_file if unmapped_file and os.path.exists(str(unmapped_file)) else None
                    if unmapped_path:
                        _consume_stream(_merge_fanse_and_unmapped_records(fanse_file, unmapped_path))
                    else:
                        _consume_stream(fanse_parser_high_performance(fanse_file))

                # 写完剩余批次
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
        # 修正：清理网络盘预加载的本地 temp 目录
        if local_temp_dir and os.path.isdir(local_temp_dir):
            try:
                shutil.rmtree(local_temp_dir)
                console.print(f"[dim]清理预加载 temp: {local_temp_dir}[/dim]")
            except OSError as e:
                console.print(f"[dim]清理 temp 失败（可忽略）: {e}[/dim]")

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
            output_path = os.path.join(output_dir, _normalize_output_sam_name(input_file, args.is_paired_end))
        elif args.output_sam:
            output_candidate = pathlib.Path(args.output_sam)
            # 修正：如果 -o 指向的是目录（已存在或无后缀路径），则自动拼接输出文件名
            if (output_candidate.exists() and output_candidate.is_dir()) or (not output_candidate.suffix and not output_candidate.exists()):
                output_path = str(output_candidate / _normalize_output_sam_name(input_file, args.is_paired_end))
            else:
                output_path = args.output_sam
        else:
            # 修正意图：双端模式默认落盘到输入文件同目录，避免大量SAM内容直接刷到终端
            if args.is_paired_end:
                output_path = str(input_file.parent / _normalize_output_sam_name(input_file, True))
            else:
                output_path = None # stdout

        if len(input_files) > 1:
            console.print(f"[dim]Processing ({i+1}/{len(input_files)}): {input_file.name}[/dim]")

        try:
            # 修正：双端模式自动发现 R1/R2 fanse3 + unmapped 四文件
            # 用 _discover_paired_fanse_files 替代旧的单 .unmapped 查找
            current_unmapped_file = None
            current_r2_fanse3 = None
            current_r2_unmapped = None
            if args.is_paired_end:
                # 尝试推断unmapped文件路径
                # 假设unmapped文件和fanse文件在同一目录，且文件名只有后缀不同
                r1_path, r1_unm, r2_f, r2_u = _discover_paired_fanse_files(input_file)
                current_unmapped_file = str(r1_unm) if r1_unm else None
                current_r2_fanse3 = str(r2_f) if r2_f else None
                current_r2_unmapped = str(r2_u) if r2_u else None

                if current_r2_fanse3:
                    console.print(f"[bold green]发现 R2 fanse3: {pathlib.Path(current_r2_fanse3).name}[/bold green]")
                else:
                    console.print(f"[bold yellow]警告: 未发现 R2 fanse3，fixmate 将无法配对（仅处理 R1）[/bold yellow]")

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
                          ref_info_json=getattr(args, 'ref_info_json', None),
                          r2_fanse3=current_r2_fanse3,      # 修正：R2 fanse3 传入
                          r2_unmapped=current_r2_unmapped,  # 修正：R2 unmapped 传入
                          preload_network=getattr(args, 'preload', False),  # 修正：网络盘预加载选项
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
                          ref_info_json=getattr(args, 'ref_info_json', None),
                          r2_fanse3=current_r2_fanse3,      # 修正：R2 fanse3 传入
                          r2_unmapped=current_r2_unmapped,   # 修正：R2 unmapped 传入
                          preload_network=getattr(args, 'preload', False)  # 修正：网络盘预加载选项
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
    sam_parser.add_argument('--preload', action='store_true',
                            help='网络盘 UNC 路径文件预加载到本地 temp（加速 5-10x）。默认不预加载，'
                                 '在本地 SSD + 千兆网络环境下效果显著；本地文件会自动跳过。')
    sam_parser.add_argument('--pe', '--paired-end', action='store_true', dest='is_paired_end',
                            help='启用双端模式。自动搜索同目录下的 .unmapped 文件进行合并处理。')
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
    
