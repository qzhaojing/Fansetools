# fansetools/fastx.py
'''
covert multi styles between them
Support Fanse/unmapped/fasta/fastq to FASTA/FASTQ.
'''
import os
import argparse
from .utils.rich_help import CustomHelpFormatter
from tqdm import tqdm
from typing import Optional, Generator, NamedTuple
# from collections import namedtuple
from .parser import fanse_parser, unmapped_parser, FANSeRecord, UnmappedRecord
from .utils.path_utils import PathProcessor
from rich.console import Console
import pathlib


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
    console = Console(force_terminal=True)
    processor = PathProcessor()

    # 解析输入文件（支持通配符）
    try:
        input_files = processor.parse_input_paths(args.input, valid_extensions=None)
    except Exception as e:
        console.print(f"[bold red]Error parsing inputs: {e}[/bold red]")
        return

    if not input_files:
        console.print(f"[bold red]Input file not found: {args.input}[/bold red]")
        return

    # 如果指定了输出路径且有多个输入文件，确保输出路径是目录
    output_dir = None
    if args.output and len(input_files) > 1:
        if not os.path.exists(args.output):
            try:
                os.makedirs(args.output, exist_ok=True)
            except OSError:
                console.print(f"[bold red]Error: Output path '{args.output}' must be a directory when processing multiple files.[/bold red]")
                return
        elif not os.path.isdir(args.output):
            console.print(f"[bold red]Error: Output path '{args.output}' must be a directory when processing multiple files.[/bold red]")
            return
        output_dir = args.output

    # 处理每个文件
    for i, input_file in enumerate(input_files):
        input_path = str(input_file)
        
        # 确定输出文件路径
        if output_dir:
            # 批量处理，输出到指定目录
            fname = input_file.stem
            if args.mode == 'fanse' or args.mode == 'unmapped':
                ext = '.fasta' if args.fasta else '.fastq'
            elif args.mode == 'fasta2fastq':
                ext = '.fastq'
            elif args.mode == 'fastq2fasta':
                ext = '.fasta'
            else:
                ext = '.out' # fallback
            
            output_path = os.path.join(output_dir, fname + ext)
        elif args.output:
            # 单个文件，直接使用指定输出路径
            output_path = args.output
        else:
            # 未指定输出，使用默认（同目录改后缀）
            output_path = None

        try:
            if len(input_files) > 1:
                console.print(f"[dim]Processing ({i+1}/{len(input_files)}): {input_file.name}[/dim]")

            if args.mode == 'fanse':
                if args.fasta:
                    output = fanse2fasta(input_path, output_path)
                    console.print(f"[green]FASTA conversion complete: {output}[/green]")
                elif args.fastq:
                    output = fanse2fastq(input_path, output_path)
                    console.print(f"[green]FASTQ conversion complete: {output}[/green]")
            elif args.mode == 'unmapped':
                if args.fasta:
                    output = unmap2fasta(input_path, output_path)
                    console.print(f"[green]FASTA conversion complete: {output}[/green]")
                elif args.fastq:
                    output = unmap2fastq(input_path, output_path)
                    console.print(f"[green]FASTQ conversion complete: {output}[/green]")
            elif args.mode == 'fasta2fastq':
                output = fasta2fastq(input_path, output_path)
                console.print(f"[green]FASTA→FASTQ conversion complete: {output}[/green]")
            elif args.mode == 'fastq2fasta':
                output = fastq2fasta(input_path, output_path)
                console.print(f"[green]FASTQ→FASTA conversion complete: {output}[/green]")
        except Exception as e:
            console.print(f"[bold red]Error processing {input_path}: {e}[/bold red]")


def add_fastx_subparser(subparsers):
    """Add fastx subcommand to the main parser"""
    parser = subparsers.add_parser(
        'fastx',
        help='Convert between Fanse/unmapped/fasta/fastq to FASTA/FASTQ',
        formatter_class=CustomHelpFormatter
    )

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
