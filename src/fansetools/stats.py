# -*- coding: utf-8 -*-
"""
fansetools/stats.py

fanse stats 子命令：批量解析 fanse 比对运行生成的 .log 文件，
提取运行参数与关键结果（总 reads、mapped reads、速度、耗时等），汇总为 CSV。

修正意图：
  fanse 运行时每个 fastq 生成一个同名 .log（注意排除转换产生的 *.bam_conv.log），
  手工逐个打开抄录参数与结果易错且耗时；本模块按固定日志格式批量解析。

日志格式要点（fanse 输出为固定模板）：
  Analysis datetime: 26-09-02 05:16
  5 CPU logical cores detected/set.
  -------------- Parameters -----------------
      Reference sequence (-R): ...
      Max read length (-L): 1000
      Errors allowed (-E): 3
      Seed length (-S): 12
      ...
  Output filenmae: ...fanse3        （注意 fanse 原文拼写就是 filenmae）
  Batch #1 读.mapping.写. reads=337740 mapped=22010 (6.5%) 00:00:31.6721398 Speed=633262 reads/min
  Finish mapping. Time=00:00:32.4377713

用法:
  fanse stats -i <目录|通配符|单个log> [-r] -o out.csv
  fanse stats -i "\\\\fs2\\d\\data\\...\\2.自测数据比对结果" -o stats.csv
"""
import os
import re
import csv
import glob
from pathlib import Path
from typing import Optional, Dict, List

from rich.console import Console
from rich.table import Table

from .utils.rich_help import CustomHelpFormatter, add_rich_epilog

# 修正：fanse 由 .NET 输出日志，中文段落（如"读.mapping.写."）为 GBK 编码，
# UTF-8 直接读会抛解码错误；先按 UTF-8 尝试，失败回退 GBK，坏字节替换不中断
def _read_log_text(path: str) -> str:
    with open(path, 'rb') as f:
        raw = f.read()
    for enc in ('utf-8', 'gbk'):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode('utf-8', errors='replace')

# 预编译解析模式
_RE_DATETIME = re.compile(r'Analysis datetime:\s*(.+)')
_RE_CPU_CORES = re.compile(r'(\d+)\s+CPU logical cores')
_RE_PARAM = re.compile(r'^\s*(.+?)\s*\((-\w+|--\w+(?:/\w+)?)\):\s*(.*?)\s*$')
_RE_FILE_COUNT = re.compile(r'file count:\s*(\d+)')
_RE_FORMAT = re.compile(r'Format:(\w+)')
_RE_OUTPUT = re.compile(r'Output filenmae:\s*(.+)')  # fanse 原文拼写错误，保持一致
_RE_BATCH = re.compile(
    r'Batch\s*#(\d+).*?reads=(\d+)\s+mapped=(\d+)\s*\(([\d.]+)%\)\s*'
    r'([\d:.]+)\s+Speed=(\d+)\s+reads/min')
_RE_FINISH = re.compile(r'Finish mapping\. Time=([\d:.]+)')

def _hms_to_seconds(hms: str) -> float:
    """'00:00:31.6721398' → 31.672 秒；解析失败返回 0.0"""
    try:
        parts = hms.split(':')
        while len(parts) < 3:
            parts.insert(0, '0')
        h, m, s = float(parts[0]), float(parts[1]), float(parts[2])
        return h * 3600 + m * 60 + s
    except (ValueError, IndexError):
        return 0.0

def parse_fanse_log(log_path: str) -> Optional[Dict]:
    """
    解析单个 fanse .log 文件。

    返回字典（键即 CSV 列名），无法识别为 fanse 日志时返回 None。
    多 Batch 时 total_reads/total_mapped 为各 Batch 之和，速度取
    总 reads / 总耗时（比末 Batch 的瞬时速度更真实）。
    """
    text = _read_log_text(log_path)
    # 快速判定：非 fanse 日志（如 bam_conv.log 已被排除，这里再兜底）
    if 'Parameters' not in text and 'Batch #' not in text:
        return None

    info: Dict = {
        'sample': Path(log_path).name[:-4] if Path(log_path).name.lower().endswith('.log') else Path(log_path).name,
        'log_file': log_path,
        'analysis_datetime': '',
        'cpu_cores': '',
        'reference': '',
        'batch_mode': '',
        'dataset_file': '',
        'file_count': '',
        'max_read_length': '',
        'errors_allowed': '',
        'seed_length': '',
        'batch_size': '',
        'use_cpu': '',
        'trim_reads': '',
        'unidirectional': '',
        'rename_reads': '',
        'unique_only': '',
        'indel_detection': '',
        'masked_genome': '',
        'input_format': '',
        'output_fanse3': '',
        'n_batches': 0,
        'total_reads': 0,
        'total_mapped': 0,
        'mapped_pct': '',
        'total_mapping_time_s': 0.0,
        'overall_speed_reads_per_min': '',
        'last_batch_speed_reads_per_min': '',
        'finish_time_s': '',
    }

    # ---- 参数段：只在 Parameters 区块内逐行匹配（-k: v 形式）----
    in_param_block = False
    for line in text.splitlines():
        m = _RE_DATETIME.search(line)
        if m:
            info['analysis_datetime'] = m.group(1).strip()
            continue
        m = _RE_CPU_CORES.search(line)
        if m:
            info['cpu_cores'] = m.group(1)
            continue
        if '-------------- Parameters' in line:
            in_param_block = True
            continue
        if in_param_block and line.strip().startswith('---'):
            in_param_block = False
            continue
        if in_param_block:
            pm = _RE_PARAM.match(line)
            if pm:
                label, flag, value = pm.group(1), pm.group(2), pm.group(3)
                if flag == '-R':
                    info['reference'] = value
                elif flag == '--batchmode':
                    info['batch_mode'] = value
                elif flag == '-D':
                    info['dataset_file'] = value
                elif flag == '-L':
                    info['max_read_length'] = value
                elif flag == '-E':
                    info['errors_allowed'] = value
                elif flag == '-S':
                    info['seed_length'] = value
                elif flag == '-H':
                    info['batch_size'] = value
                elif flag == '-C':
                    info['use_cpu'] = value
                elif flag == '-T':
                    info['trim_reads'] = value
                elif flag == '-U':
                    info['unidirectional'] = value
                elif flag == '--rename':
                    info['rename_reads'] = value
                elif flag == '--unique':
                    info['unique_only'] = value
                elif flag == '--indel/-I1':
                    info['indel_detection'] = value
                elif flag == '--mask':
                    info['masked_genome'] = value
                continue
            fm = _RE_FILE_COUNT.search(line)
            if fm:
                info['file_count'] = fm.group(1)

    # ---- 结果段 ----
    m = _RE_FORMAT.search(text)
    if m:
        info['input_format'] = m.group(1)
    m = _RE_OUTPUT.search(text)
    if m:
        info['output_fanse3'] = m.group(1).strip()

    total_time_s = 0.0
    last_speed = ''
    for bm in _RE_BATCH.finditer(text):
        info['n_batches'] += 1
        reads, mapped = int(bm.group(2)), int(bm.group(3))
        info['total_reads'] += reads
        info['total_mapped'] += mapped
        total_time_s += _hms_to_seconds(bm.group(5))
        last_speed = bm.group(6)
    fm = _RE_FINISH.search(text)
    if fm:
        info['finish_time_s'] = round(_hms_to_seconds(fm.group(1)), 1)

    if info['n_batches']:
        info['mapped_pct'] = round(info['total_mapped'] / info['total_reads'] * 100, 2) if info['total_reads'] else 0.0
        info['total_mapping_time_s'] = round(total_time_s, 1)
        # 修正：整体速度 = 总 reads / 总纯映射耗时（跨 Batch 汇总，优于仅取末 Batch 瞬时值）
        if total_time_s > 0:
            info['overall_speed_reads_per_min'] = round(info['total_reads'] / (total_time_s / 60.0))
        info['last_batch_speed_reads_per_min'] = last_speed

    return info

# 修正：排除转换流程产生的日志（.bam_conv.log），只统计 fanse 比对自身日志
_EXCLUDE_PATTERN = re.compile(r'\.bam_conv\.log$', re.IGNORECASE)

def _collect_log_files(input_path: str, recursive: bool = False) -> List[str]:
    """收集待解析的 log 文件：支持目录 / 通配符 / 单文件"""
    p = Path(input_path)
    if p.is_dir():
        pattern = '**/*.log' if recursive else '*.log'
        files = [str(x) for x in p.glob(pattern)]
    elif any(ch in input_path for ch in '*?'):
        files = glob.glob(input_path, recursive=recursive)
    elif p.is_file():
        files = [str(p)]
    else:
        files = []
    return sorted(f for f in files if not _EXCLUDE_PATTERN.search(f))

_CSV_COLUMNS = [
    'sample', 'analysis_datetime', 'cpu_cores',
    'total_reads', 'total_mapped', 'mapped_pct',
    'overall_speed_reads_per_min', 'last_batch_speed_reads_per_min',
    'n_batches', 'total_mapping_time_s', 'finish_time_s',
    'input_format', 'output_fanse3', 'log_file',
    'reference', 'dataset_file', 'file_count',
    'max_read_length', 'errors_allowed', 'seed_length', 'batch_size',
    'use_cpu', 'trim_reads', 'unidirectional',
    'rename_reads', 'unique_only', 'indel_detection', 'masked_genome', 'batch_mode',
]

def stats_command(args):
    """fanse stats 主入口：收集 log → 解析 → 打印摘要表 → 写 CSV"""
    console = Console(force_terminal=True)

    log_files = _collect_log_files(args.input, recursive=args.recursive)
    if not log_files:
        console.print(f"[bold red]错误: 未找到 .log 文件: {args.input}[/bold red]（提示: 目录下应有 fanse 运行生成的 .log，*.bam_conv.log 已自动排除）")
        sys_exit = __import__('sys')
        sys_exit.exit(1)

    console.print(f"找到 {len(log_files)} 个 fanse 日志，开始解析...")
    rows = []
    skipped = 0
    for lf in log_files:
        try:
            info = parse_fanse_log(lf)
        except Exception as e:
            console.print(f"[bold yellow]警告: 解析失败 {lf}: {e}[/bold yellow]")
            info = None
        if info is None:
            skipped += 1
            continue
        rows.append(info)

    if not rows:
        console.print("[bold red]错误: 所有日志均无法解析为 fanse 比对日志[/bold red]")
        __import__('sys').exit(1)

    # 摘要表（按总 reads 降序）
    rows.sort(key=lambda r: r['total_reads'], reverse=True)
    table = Table(title=f"fanse 运行统计 ({len(rows)} 个日志, 跳过 {skipped} 个)", box=None)
    for col, header in [('sample', 'Sample'), ('total_reads', 'Total Reads'),
                        ('total_mapped', 'Mapped'), ('mapped_pct', 'Mapped%'),
                        ('overall_speed_reads_per_min', 'Speed(reads/min)')]:
        table.add_column(header, overflow='fold')
    for r in rows:
        table.add_row(str(r['sample']), f"{r['total_reads']:,}", f"{r['total_mapped']:,}",
                      str(r['mapped_pct']), str(r['overall_speed_reads_per_min']))
    console.print(table)

    # 写 CSV（utf-8-sig：Excel 直接打开中文不乱码）
    out_path = args.output
    with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    console.print(f"[bold green]已写入: {out_path}[/bold green]")


def add_stats_subparser(subparsers):
    """添加 stats 子命令解析器"""
    stats_parser = subparsers.add_parser(
        'stats',
        help='汇总 fanse 运行日志(.log)为 CSV 统计表',
        description='批量解析 fanse 比对生成的 .log 文件（自动排除 *.bam_conv.log），'
                    '提取参数与关键结果（总reads、mapped、speed、耗时等）输出 CSV。',
        formatter_class=CustomHelpFormatter
    )
    stats_parser.add_argument(
        '-i', '--input', required=True,
        help='输入：目录 / 通配符（如 "CQ1*.log"）/ 单个 .log 文件路径')
    stats_parser.add_argument(
        '-o', '--output', default='fanse_stats.csv',
        help='输出 CSV 路径（默认: ./fanse_stats.csv）')
    stats_parser.add_argument(
        '-r', '--recursive', action='store_true',
        help='递归搜索子目录（仅 -i 为目录时生效）')

    stats_parser.set_defaults(func=stats_command)
    add_rich_epilog(stats_parser, """
[bold]功能说明:[/bold]
  扫描文件夹中 fanse 比对运行生成的 .log 文件，汇总每个样本的
  运行参数（-R/-L/-E/-S/-H/-C 等）与关键结果：
  总 reads 数、mapped reads（数量与百分比）、整体速度（reads/min）、
  纯映射总耗时、Batch 数等，输出为 CSV（utf-8-sig，Excel 可直接打开）。

[bold]示例:[/bold]
  1. 统计整个文件夹（不递归）:
     [green]fanse stats -i "\\\\fs2\\d\\data\\...\\2.自测数据比对结果" -o stats.csv[/green]

  2. 通配符统计 R1 测序:
     [green]fanse stats -i "\\\\fs2\\...\\CQ1*_R1_001.log" -o r1_stats.csv[/green]

  3. 递归搜索子目录:
     [green]fanse stats -i "\\\\fs2\\...\\20260804-fanse3_quanti_bed" -r -o all_stats.csv[/green]

[bold]注意:[/bold]
  - *.bam_conv.log（bam 转换日志）会自动排除，只统计 fanse 比对日志
  - mapped_pct / overall_speed 由多 Batch 汇总计算，非单 Batch 瞬时值
""")
