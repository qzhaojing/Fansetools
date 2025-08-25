# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 11:56:35 2025
v0.1

@author: P.h.D., ZhaoJing, 

Jinan University
"""

import os
from typing import Generator,  Optional, Dict  # ,# Tuple, Iterator, Set
from .parser import FANSeRecord, fanse_parser
import gzip
import sys


def generate_cigar(alignment: str) -> str:
    """
    参数:
        alignment: FANSe比对字符串
        seq_len: 序列实际长度

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
    # if not alignment or seq_len <= 0:
    #     return f"{seq_len}M"

    cigar = []
    current_op = None
    count = 0
    consumed_query = 0  # 已消耗的查询序列长度

    for char in alignment:
        # 确定操作类型
        if char == '.':
            op = 'M'
            consumed_query += 1
        elif char == 'x':
            op = 'X'
            consumed_query += 1
        elif char == '-':
            op = 'D'  # 不消耗查询序列
        elif char.isalpha():
            op = 'I'
            consumed_query += 1
        else:
            op = 'S'
            consumed_query += 1

        # 统计连续操作
        if op == current_op:
            count += 1
        else:
            if current_op is not None:
                cigar.append(f"{count}{current_op}")
            current_op = op
            count = 1

    # 添加最后一个操作
    if current_op is not None:
        cigar.append(f"{count}{current_op}")

    return "".join(cigar)


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
        sa_parts.append(f"{record.ref_names[i]},{record.positions[i]+1},{strand}," +
                        f"{generate_cigar(record.alignment[i])},255,{record.mismatches[i]}")
    return f"SA:Z:{';'.join(sa_parts)}" if sa_parts else ""


def fanse_to_sam_type(record: FANSeRecord) -> Generator[str, None, None]:
    """将FANSeRecord转换为SAM格式行"""
    if not record.ref_names:
        return

    # 找出主记录(错配最少)
    primary_idx = min(range(len(record.mismatches)),
                      key=lambda i: record.mismatches[i])

    # 处理主记录
    flag = calculate_flag(record.strands[primary_idx])
    cigar = generate_cigar(record.alignment[primary_idx])
    seq = reverse_complement(
        record.seq) if 'R' in record.strands[primary_idx] else record.seq
    sa_tag = generate_sa_tag(record, primary_idx)

    sam_fields = [
        record.header,
        str(flag),
        record.ref_names[primary_idx],
        str(record.positions[primary_idx] + 1),  # 1-based
        "255",  # MAPQ
        cigar,
        "*",    # RNEXT
        "0",    # PNEXT
        "0",    # TLEN
        seq,
        "*",    # QUAL
        f"XM:i:{record.mismatches[primary_idx]}",
        f"XN:i:{record.multi_count}"
    ]

    if sa_tag:
        sam_fields.append(sa_tag)

    yield "\t".join(sam_fields)

    # 处理辅助记录
    for i in range(len(record.ref_names)):
        if i == primary_idx:
            continue
        flag = calculate_flag(record.strands[i], is_secondary=True)
        cigar = generate_cigar(record.alignment[i])
        seq = reverse_complement(
            record.seq) if 'R' in record.strands[i] else record.seq

        sam_fields = [
            record.header,
            str(flag),
            record.ref_names[i],
            str(record.positions[i] + 1),
            "255",
            cigar,
            "*",
            "0",
            "0",
            seq,
            "*",
            f"XM:i:{record.mismatches[i]}",
            f"XN:i:{record.multi_count}"
        ]
        yield "\t".join(sam_fields)


def reverse_complement(seq: str) -> str:
    """生成反向互补序列"""
    complement = {'A': 'T', 'T': 'A', 'C': 'G', 'G': 'C', 'N': 'N'}
    return ''.join([complement.get(base, 'N') for base in reversed(seq)])


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


def generate_sam_header_from_fasta(fasta_path: str) -> str:
    """
    从FASTA文件生成完整的SAM头部

    参数:
        fasta_path: FASTA文件路径

    返回:
        完整的SAM头部字符串
    """
    ref_info = parse_fasta(fasta_path)

    header_lines = [
        "@HD\tVN:1.6\tSO:unsorted",
        "@PG\tID:fanse3\tPN:fanse3\tVN:3.0\tCL:fanse3"
    ]

    # 添加参考序列信息
    for ref_name, length in ref_info.items():
        header_lines.append(f"@SQ\tSN:{ref_name}\tLN:{length}")

    return '\n'.join(header_lines) + '\n'


def fanse2sam(fanse_file, fasta_path, output_sam: Optional[str] = None):
    """
    将FANSe3文件转换为SAM格式

    参数:
        fanse_file: 输入FANSe3文件路径
        output_sam: 输出SAM文件路径(如果为None则打印到标准输出)
    """
    # print('Start fanse2sam: {}'.format(fanse_file))
    # 先读取所有记录以生成头部
    # records = list(fanse_parser(fanse_file))
    header = generate_sam_header_from_fasta(fasta_path)
    # 组合两者
    if output_sam:
        with open(output_sam, 'w') as out_f:
            # 写入SAM头
            # out_f.write("@HD\tVN:1.6\tSO:unsorted\n")
            out_f.write(header)
            print('Header write done.')
            # 处理记录
            for record in fanse_parser(fanse_file):
                for sam_line in fanse_to_sam_type(record):
                    out_f.write(sam_line + "\n")
    else:
        # 修复管道输出兼容性
        try:
            # 尝试直接写入标准输出缓冲区
            sys.stdout.buffer.write(header.encode())
            for record in fanse_parser(fanse_file):
                for sam_line in fanse_to_sam_type(record):
                    sys.stdout.buffer.write((sam_line + "\n").encode())
        except AttributeError:
            # 回退方案：使用原始标准输出
            sys.__stdout__.write(header)
            for record in fanse_parser(fanse_file):
                for sam_line in fanse_to_sam_type(record):
                    sys.__stdout__.write(sam_line + "\n")


def run_sam_command(args):
    """Handle sam subcommand"""
# def run_fanse2sam(args):
    fanse2sam(args.fanse_file, args.fasta_path, args.output_sam)


def add_sam_subparser(subparsers):
    sam_parser = subparsers.add_parser(
        'sam',
        help='转换为 SAM 格式',
        description='将 FANSe3 文件转换为标准 SAM 格式, 在linux中不加-o参数可接 samtools 管道处理直接保存为bam格式'
    )
    sam_parser.add_argument(
        '-i', '--input', dest='fanse_file', required=True, help='输入文件路径（FANSe3 格式）')
    sam_parser.add_argument(
        '-r', '--fasta', dest='fasta_path', required=True, help='参考基因组 FASTA 文件路径')
    sam_parser.add_argument(
        '-o', '--output', dest='output_sam', help='输出文件路径（不指定输出位置，默认打印到终端）')
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
    fasta_path = r'\\fs2\D\DATA\Zhaojing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\genomic16_merge.fasta'
    fanse_file = r'\\fs2\D\DATA\Zhaojing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\4.1.merge-polya.fanse3'
    output_sam = r'\\fs2\D\DATA\Zhaojing\20250722-kbseq\PSM-ZM202507310003-0001\out_no_trimming\have_remain_files\fanse3_align\4.1.merge-polya.sam'
    # fanse2sam(fanse_file, fasta_path, output_sam)
