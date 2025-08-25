# fansetools/fastx.py
'''
covert multi styles between them
Support Fanse/unmapped/fasta/fastq to FASTA/FASTQ.
'''
import os
# import argparse
from tqdm import tqdm
from typing import Optional, Generator, NamedTuple
# from collections import namedtuple
from .parser import fanse_parser, unmapped_parser, FANSeRecord, UnmappedRecord


class FastxRecord(NamedTuple):
    header: str
    seq: str


def simple_fasta_parser(fasta_file: str) -> Generator[FastxRecord, None, None]:
    """简单高效的FASTA解析器"""
    header, seq = None, []
    with open(fasta_file) as f:
        for line in f:
            if line.startswith('>'):
                if header is not None:
                    yield FastxRecord(header, ''.join(seq))
                header = line[1:].strip()
                seq = []
            else:
                seq.append(line.strip())
        if header is not None:
            yield FastxRecord(header, ''.join(seq))


def simple_fastq_parser(fastq_file: str) -> Generator[FastxRecord, None, None]:
    """简单高效的FASTQ解析器（忽略质量值）"""
    with open(fastq_file) as f:
        while True:
            header_line = f.readline().strip()
            if not header_line:
                break
            if header_line.startswith('@'):
                seq = f.readline().strip()
                f.readline()  # 跳过+
                f.readline()  # 跳过质量行
                yield FastxRecord(header_line[1:], seq)


def fanse2fasta(input_file: str, output_file: Optional[str] = None) -> str:
    """Convert Fanse format to FASTA format"""
    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + '.fasta'

    with open(output_file, 'w') as f_out:
        for record in fanse_parser(input_file):
            f_out.write(f">{record.header}\n{record.seq}\n")

    return output_file


def fanse2fastq(input_file: str, output_file: Optional[str] = None) -> str:
    """Convert Fanse format to FASTQ format"""
    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + '.fastq'

    with open(output_file, 'w') as f_out:
        for record in fanse_parser(input_file):
            qual = 'I' * len(record.seq)  # Default quality score
            f_out.write(f"@{record.header}\n{record.seq}\n+\n{qual}\n")

    return output_file


def unmap2fasta(input_file: str, output_file: Optional[str] = None) -> str:
    """Convert unmapped reads to FASTA format"""
    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + '.fasta'

    # 获取记录数用于进度条
    with open(input_file) as f:
        total = sum(1 for _ in f)

    with open(output_file, 'w') as f_out:
        pbar = tqdm(total=total, desc="Converting unmapped to FASTA")
        for record in unmapped_parser(input_file):
            f_out.write(f">{record.read_id}\n{record.sequence}\n")
            pbar.update(1)
        pbar.close()

    return output_file


def unmap2fastq(input_file: str, output_file: Optional[str] = None) -> str:
    """Convert unmapped reads to FASTQ format"""
    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + '.fastq'

    # 获取记录数用于进度条
    with open(input_file) as f:
        total = sum(1 for _ in f)

    with open(output_file, 'w') as f_out:
        pbar = tqdm(total=total, desc="Converting unmapped to FASTQ")
        for record in unmapped_parser(input_file):
            qual = 'I' * len(record.sequence)  # Default quality score
            f_out.write(f"@{record.read_id}\n{record.sequence}\n+\n{qual}\n")
            pbar.update(1)
        pbar.close()

    return output_file


def fasta2fastq(input_file: str, output_file: Optional[str] = None) -> str:
    """高效转换FASTA到FASTQ"""
    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + '.fastq'

    with open(output_file, 'w') as f_out:
        for record in simple_fasta_parser(input_file):
            qual = 'I' * len(record.seq)  # 默认质量分数
            f_out.write(f"@{record.header}\n{record.seq}\n+\n{qual}\n")

    return output_file


def fastq2fasta(input_file: str, output_file: Optional[str] = None) -> str:
    """高效转换FASTQ到FASTA"""
    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + '.fasta'

    # 预先计算记录数用于进度条
    with open(input_file) as f:
        total_records = sum(1 for _ in f) // 4

    with open(output_file, 'w') as f_out:
        pbar = tqdm(total=total_records, desc="Converting FASTQ to FASTA")
        for record in simple_fastq_parser(input_file):
            f_out.write(f">{record.header}\n{record.seq}\n")
            pbar.update(1)
        pbar.close()

    return output_file


def fastx_command(args):
    """Handle fastx subcommand"""
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}")

    if args.mode == 'fanse':
        if args.fasta:
            output = fanse2fasta(args.input, args.output)
            print(f"FASTA conversion complete: {output}")
        elif args.fastq:
            output = fanse2fastq(args.input, args.output)
            print(f"FASTQ conversion complete: {output}")
    elif args.mode == 'unmapped':
        if args.fasta:
            output = unmap2fasta(args.input, args.output)
            print(f"FASTA conversion complete: {output}")
        elif args.fastq:
            output = unmap2fastq(args.input, args.output)
            print(f"FASTQ conversion complete: {output}")
    elif args.mode == 'fasta2fastq':
        output = fasta2fastq(args.input, args.output)
        print(f"FASTA→FASTQ conversion complete: {output}")
    elif args.mode == 'fastq2fasta':
        output = fastq2fasta(args.input, args.output)
        print(f"FASTQ→FASTA conversion complete: {output}")


def add_fastx_subparser(subparsers):
    """Add fastx subcommand to the main parser"""
    parser = subparsers.add_parser('fastx',
                                   help='Convert between Fanse/unmapped/fasta/fastq to FASTA/FASTQ')

    # 输入文件参数
    parser.add_argument('-i', '--input', required=True,
                        help='Input file path')
    # 输出文件参数
    parser.add_argument('-o', '--output',
                        help='Output file path (default: input file with new extension)')
    # 模式选择
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--fanse', action='store_const', dest='mode',
                            const='fanse', help='Process Fanse format file')
    mode_group.add_argument('--unmapped', action='store_const', dest='mode',
                            const='unmapped', help='Process unmapped reads file')
    mode_group.add_argument('--fasta2fastq', action='store_const', dest='mode',
                            const='fasta2fastq', help='Convert FASTA to FASTQ')
    mode_group.add_argument('--fastq2fasta', action='store_const', dest='mode',
                            const='fastq2fasta', help='Convert FASTQ to FASTA')

    # 输出格式选择
    format_group = parser.add_mutually_exclusive_group(required=False)
    format_group.add_argument('--fasta', action='store_true',
                              help='Convert to FASTA format (for fanse/unmapped)')
    format_group.add_argument('--fastq', action='store_true',
                              help='Convert to FASTQ format (for fanse/unmapped)')

    parser.set_defaults(func=fastx_command)
