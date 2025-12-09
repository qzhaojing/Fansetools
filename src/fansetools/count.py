#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fansetools count 组件 - 用于处理fanse3文件的read计数

-count gene level
-count transcript level
-count exon level
-count cds level
-count 5utr
-count 3utr
---如果想实现这个，可能得转换坐标，将基因组坐标转换为转录组坐标，refflat文件中对应的坐标都进行更改，全部都减去第一个start来转换。
长度需要有多个，
"""

import argparse
from .utils.rich_help import CustomHelpFormatter
from collections import Counter, defaultdict, deque
from itertools import chain  # 修正：用于生成器级展开multi2all，避免构建巨大的中间列表
from fansetools.quant import add_quant_columns, build_length_maps  # 新增：引入统一的定量计算函数
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, SpinnerColumn
from rich.console import Console, Group
from rich.layout import Layout
from rich.columns import Columns
from rich.live import Live
import os
import math
from pathlib import Path
import sys
import time
import warnings

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from fansetools.gxf2refflat_plus import (
    convert_gxf_to_refflat,
    load_annotation_to_dataframe,
)
from fansetools.parser import FANSeRecord, fanse_parser, fanse_parser_high_performance
# 新增：Rust 加速解析适配层（存在时优先使用）
try:
    from fansetools.fastcount_py import rust_fastcount_available, parse_and_count_rust
except Exception:
    rust_fastcount_available = lambda: False
    parse_and_count_rust = None
# 修正：新增定量模块引入，用于在唯一文件中追加 TPM/RPKM 列
try:
    from fansetools.quant import add_quant_columns, build_length_maps
except Exception:
    # 若独立模块不可用，保持兼容运行（不追加定量列）
    add_quant_columns = None
    build_length_maps = None
from fansetools.utils.path_utils import PathProcessor
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except Exception:
    pass

# %% ParallelFanseCounter

# Global variable for worker process annotation cache
_worker_annotation_df = None

def process_single_file_task(task):
    """处理单个文件（独立函数，避免pickling问题）"""
    global _worker_annotation_df
    
    try:
        # Load annotation data if needed and not cached
        if task['gxf_file'] and _worker_annotation_df is None:
            # Reconstruct minimal args for load_annotation_data
            args = argparse.Namespace(gxf=task['gxf_file'], verbose=task.get('verbose', False))
            # load_annotation_data is defined in this module
            _worker_annotation_df = load_annotation_data(args)
            
        counter = FanseCounter(
            input_file=task['input_file'],
            output_dir=task['output_dir'],
            gxf_file=task['gxf_file'],
            level=task['level'],
            annotation_df=_worker_annotation_df,
            verbose=task.get('verbose', False),
            export_format=task.get('format', 'rsem'),
            export_count_type=task.get('count_type','Final_EM'),
            length_mode_gene=task.get('length_mode_gene', 'genelongesttxLength'),
            length_mode_isoform=task.get('length_mode_isoform', 'txLength'),
            batch_size=task.get('batch_size', None),
            quant=task.get('quant', 'none'),
            engine=task.get('engine', 'auto')
        )

        result = counter.run()
        return f"成功处理 {task['file_stem']}"

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        raise Exception(f"处理文件 {task['input_file']} 失败: {str(e)}\nTraceback: {tb}")

class ParallelFanseCounter:
    """并行处理多个fanse3文件的计数器"""

    def __init__(self, max_workers=None, verbose=False):
        self.max_workers = max_workers or min(mp.cpu_count(), 8)
        self.verbose = verbose
        self.console = Console(force_terminal=True)
        if self.verbose:
            self.console.print(f"初始化并行处理器: {self.max_workers} 个进程")

    def process_files_parallel(self, file_list, output_base_dir, gxf_file=None, level='gene', paired_end=None, annotation_df=None, length_mode_gene=None, length_mode_isoform='txLength', verbose=False, batch_size=None, quant='none', engine='auto', export_format='rsem', export_count_type='Final_EM'):
        """并行处理多个文件（仅显示正在运行的任务）"""
        if verbose:
            self.console.print(f" 开始并行处理 {len(file_list)} 个文件，使用 {self.max_workers} 个进程")

        tasks = []
        for input_file in file_list:
            file_stem = input_file.stem
            output_dir = Path(output_base_dir) / file_stem
            output_dir.mkdir(parents=True, exist_ok=True)

            task = {
                'input_file': str(input_file),
                'output_dir': str(output_dir),
                'gxf_file': gxf_file,
                'level': level,
                'length_mode_gene': length_mode_gene,
                'length_mode_isoform': length_mode_isoform,
                'file_stem': file_stem,
                'verbose': verbose,
                'batch_size': batch_size,
                'quant': quant,
                'engine': engine,
                'format': export_format,
                'count_type': export_count_type
            }
            tasks.append(task)

        pending = deque(tasks)
        results = []
        slots = min(self.max_workers, len(tasks))

        bars_per_row = 5
        overall = Progress(TextColumn("[bold]总体进度"), BarColumn(), TimeElapsedColumn())
        overall_task = overall.add_task("总体进度", total=len(tasks))

        prog_list = []
        task_id_list = []
        for s in range(slots):
            p = Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TimeElapsedColumn())
            tid = p.add_task("等待", total=None)
            prog_list.append(p)
            task_id_list.append(tid)

        rows = []
        num_rows = max(1, math.ceil(slots / bars_per_row))
        for r in range(num_rows):
            start = r * bars_per_row
            end = min(start + bars_per_row, slots)
            if start < end:
                rows.append(Columns(prog_list[start:end], equal=True))

        render_group = Group(overall, *rows)

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_slot = {}
            current_task_by_slot = {}

            def submit_next(slot_idx):
                if pending:
                    t = pending.popleft()
                    current_task_by_slot[slot_idx] = t
                    # 使用独立函数 process_single_file_task 替代实例方法，避免 pickle 问题
                    f = executor.submit(process_single_file_task, t)
                    future_to_slot[f] = slot_idx
                    prog_list[slot_idx].update(task_id_list[slot_idx], description=f"[cyan]{t['file_stem']}", total=None, completed=0)
                    return True
                return False

            with Live(render_group, refresh_per_second=10):
                for s in range(slots):
                    submit_next(s)

                while future_to_slot:
                    for future in as_completed(list(future_to_slot.keys())):
                        slot_idx = future_to_slot.pop(future)
                        t = current_task_by_slot.get(slot_idx)
                        try:
                            result = future.result()
                            results.append((t['input_file'], True, result))
                            prog_list[slot_idx].update(task_id_list[slot_idx], total=1, completed=1, description=f"[green]完成: {t['file_stem']}")
                        except Exception as e:
                            results.append((t['input_file'], False, str(e)))
                            prog_list[slot_idx].update(task_id_list[slot_idx], total=1, completed=1, description=f"[red]失败: {t['file_stem']}")
                        finally:
                            overall.update(overall_task, advance=1)

                        submit_next(slot_idx)

        return results

    def _process_single_file(self, task, annotation_df=None):
        """处理单个文件（工作进程函数）"""
        try:
            # 在工作进程中重新加载注释数据（如果需要）
            if task['gxf_file'] and annotation_df is None:
                # 这里可以添加在工作进程中加载注释的逻辑
                pass

            counter = FanseCounter(
                input_file=task['input_file'],
                output_dir=task['output_dir'],
                gxf_file=task['gxf_file'],
                level=task['level'],
                annotation_df=annotation_df,
                verbose=task.get('verbose', False),
                export_format=task.get('format', 'rsem'),
                export_count_type=task.get('count_type','Final_EM'),
                length_mode_gene=task.get('length_mode_gene', 'genelongesttxLength'),
                length_mode_isoform=task.get('length_mode_isoform', 'txLength'),
                batch_size=task.get('batch_size', None),
                # 修正：并行路径中追加定量参数
                quant=task.get('quant', 'none'),
                engine=task.get('engine', 'auto')
            )

            # 运行计数处理
            result = counter.run()
            return f"成功处理 {task['file_stem']}"

        except Exception as e:
            # 捕获详细的堆栈信息
            import traceback
            tb = traceback.format_exc()
            raise Exception(f"处理文件 {task['input_file']} 失败: {str(e)}\nTraceback: {tb}")


def count_main_parallel(args):
    """支持并行的主函数"""
    console = Console(force_terminal=True)
    if getattr(args, 'verbose', False):
        print_mini_fansetools()
    processor = PathProcessor()

    try:
        # 1. 解析输入文件
        input_files = processor.parse_input_paths(
            args.input, ['.fanse', '.fanse3', '.fanse3.gz', '.fanse.gz', '.fanse3.zip'])
        if not input_files:
            console.print("[bold red]错误: 未找到有效的输入文件[/bold red]")
            return

        if getattr(args, 'verbose', False):
            console.print(f"找到 {len(input_files)} 个输入文件")

        # 2. 加载注释文件（主进程加载，然后传递给工作进程）
        annotation_df = None
        if args.gxf:
            annotation_df = load_annotation_data(args)
            if annotation_df is None:
                console.print("[bold red]错误: 无法加载注释数据[/bold red]")
                return
            if getattr(args, 'verbose', False):
                console.print(f"已加载注释数据: {len(annotation_df)} 个转录本")
        else:
            if getattr(args, 'verbose', False):
                console.print("[yellow]未提供注释文件，将只生成isoform水平计数[/yellow]")

        # 3. 设置输出目录
        output_dir = Path(
            args.output) if args.output else Path.cwd() / "fansetools_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        if getattr(args, 'verbose', False):
            console.print(f"输出目录: {output_dir}")

        # 4. 断点续传检查
        files_to_process = []
        skipped_files = 0

        for input_file in input_files:
            file_stem = input_file.stem
            individual_output_dir = output_dir / file_stem

            # 检查输出文件是否存在
            output_files_to_check = []
            if args.level in ['isoform', 'both']:
                output_files_to_check.append(
                    individual_output_dir / f"{file_stem}.isoform_level_unique.csv")
                output_files_to_check.append(
                    individual_output_dir / f"{file_stem}.isoform_level_multi.csv")

            if args.level in ['gene', 'both'] and args.gxf:
                output_files_to_check.append(
                    individual_output_dir / f"{file_stem}.gene_level_unique.csv")
                output_files_to_check.append(
                    individual_output_dir / f"{file_stem}.gene_level_multi.csv")

            # 检查文件是否存在，都存在才算运行完。有一个不再都算没运行完，然后重新跑一次
            all_files_exist = all(f.exists() for f in output_files_to_check)

            if args.resume and all_files_exist:
                if getattr(args, 'verbose', False):
                    console.print(f"  [dim]跳过: {input_file.name} - 输出文件已存在[/dim]")
                skipped_files += 1
            else:
                files_to_process.append(input_file)

        if not files_to_process:
            if getattr(args, 'verbose', False):
                console.print("[green]所有文件均已处理完成[/green]")
            return

        if getattr(args, 'resume', False) and getattr(args, 'verbose', False):
            console.print(
                f"断点续传: 跳过 {skipped_files} 个文件，剩余 {len(files_to_process)} 个文件待处理")

        # 5. 并行处理
        max_workers = args.processes if hasattr(
            args, 'processes') and args.processes > 1 else min(mp.cpu_count(), len(files_to_process))

        if max_workers == 1:
            if getattr(args, 'verbose', False):
                console.print("使用串行处理模式")
            return count_main_serial(args)  # 回退到串行处理

        parallel_counter = ParallelFanseCounter(max_workers=max_workers, verbose=getattr(args, 'verbose', False))

        # if getattr(args, 'verbose', False):
        console.print(f"启动多任务并行处理 ==>> 共 {len(files_to_process)} 个文件")
        console.print("=" * 60)

        start_time = time.time()
        results = parallel_counter.process_files_parallel(
            file_list=files_to_process,
            output_base_dir=output_dir,
            gxf_file=args.gxf,
            level=args.level,
            # paired_end=args.paired_end,
            annotation_df=annotation_df,
            length_mode_gene=getattr(args, 'len_gene', getattr(args, 'len', 'genelongesttxLength')),
            length_mode_isoform=getattr(args, 'len_isoform', 'txLength'),
            verbose=getattr(args, 'verbose', False),
            batch_size=getattr(args, 'batch_size', None),
            # 修正：并行路径中传递定量方法选择
            quant=getattr(args, 'quant', 'none'),
            engine=getattr(args, 'engine', 'auto'),
            export_format=getattr(args, 'format', 'salmon'),
            export_count_type=getattr(args, 'count_type', 'Final_EM')
        )

        duration = time.time() - start_time

        # 6. 输出结果摘要
        if getattr(args, 'verbose', False):
            console.print("\n" + "=" * 60)
            console.print(" 处理结果摘要")
            console.print("=" * 60)

        success_count = sum(1 for _, success, _ in results if success)
        failed_count = len(results) - success_count

        if getattr(args, 'verbose', False):
            console.print(f" 成功: {success_count} 个文件")
            console.print(f" 失败: {failed_count} 个文件")
            console.print(f" 总耗时: {duration:.2f} 秒")

        if failed_count > 0:
            console.print("\n[bold red]失败详情:[/bold red]")
            printed_errors = 0
            for input_file, success, result in results:
                if not success:
                    console.print(f"  - {Path(input_file).name}: {result}")
                    printed_errors += 1
                    if printed_errors >= 10 and not getattr(args, 'verbose', False):
                        console.print(f"  ... 以及其他 {failed_count - 10} 个错误 (使用 -v 查看全部)")
                        break

        if getattr(args, 'verbose', False):
            console.print(f"\n [bold green]处理完成! 结果保存在: {output_dir}[/bold green]")

    except Exception as e:
        console = Console(force_terminal=True)
        console.print(f"[bold red]错误: {str(e)}[/bold red]")
        import traceback
        traceback.print_exc()


def count_main_serial(args, progress=None, task_id=None):
    """串行处理版本（原有的count_main函数）"""
    console = Console(force_terminal=True)
    if getattr(args, 'verbose', False):
        console.print("使用单任务处理模式...")
    processor = PathProcessor()

    try:
        # 原有的串行处理逻辑...
        input_files = processor.parse_input_paths(
            args.input, ['.fanse', '.fanse3', '.fanse3.gz', '.fanse.gz', '.fanse3.zip'])
        if not input_files:
            console.print("[bold red]错误: 未找到有效的输入文件[/bold red]")
            return

        # 加载注释文件
        annotation_df = None
        if args.gxf:
            annotation_df = load_annotation_data(args)
            if annotation_df is None:
                console.print("[bold red]错误: 无法加载注释数据[/bold red]")
                return
        else:
            if getattr(args, 'verbose', False):
                console.print("[yellow]未提供注释文件，将只生成isoform水平计数[/yellow]")

        # 生成输出映射
        output_map = processor.generate_output_mapping(
            input_files, args.output, '.counts.csv')

        # 断点续传检查
        skipped_files = 0
        if args.resume:
            if getattr(args, 'verbose', False):
                print("启用断点续传模式，检查已存在的输出文件...")
            files_to_process = {}

            for input_file, output_file in output_map.items():
                output_dir = Path(output_file).parent
                input_stem = input_file.stem

                output_files_to_check = []
                if args.level in ['isoform', 'both']:
                    output_files_to_check.append(
                        output_dir / f"{input_stem}.counts_isoform_level_unique.csv")
                if args.level in ['gene', 'both']:
                    output_files_to_check.append(
                        output_dir / f"{input_stem}.counts_gene_level_unique.csv")
                    output_files_to_check.append(
                        output_dir / f"{input_stem}.counts_gene_level_multi.csv")

                all_files_exist = all(f.exists()
                                      for f in output_files_to_check)
                if all_files_exist:
                    if getattr(args, 'verbose', False):
                        console.print(f"  [dim]跳过: {input_file.name} - 输出文件已存在[/dim]")
                    skipped_files += 1
                else:
                    files_to_process[input_file] = output_file

            output_map = files_to_process
            if getattr(args, 'verbose', False):
                console.print(f"断点续传: 跳过 {skipped_files} 个文件，剩余 {len(output_map)} 个文件待处理")

            if not output_map:
                if getattr(args, 'verbose', False):
                    console.print("[green]所有文件均已处理完成[/green]")
                return

        # 串行处理每个文件
        for i, (input_file, output_file) in enumerate(output_map.items(), 1):
            # 确保输出目录存在
            if not output_file.parent.exists():
                output_file.parent.mkdir(parents=True, exist_ok=True)

            if getattr(args, 'verbose', False):
                console.print(
                    f"\n[{i + skipped_files}/{len(input_files)}] 处理: {input_file.name}")
                console.print(f"  输出: {output_file}")

            try:
                counter = FanseCounter(
                    input_file=str(input_file),
                    output_dir=str(output_file.parent),
                    output_filename=output_file.name,
                    gxf_file=args.gxf,
                    level=args.level if annotation_df is not None else 'isoform',
                    # paired_end=args.paired_end,
                    annotation_df=annotation_df,
                    verbose=getattr(args, 'verbose', False),
                    export_format=getattr(args, 'format', 'rsem'),
                    export_count_type=getattr(args, 'count_type', 'Final_EM'),
                    progress=progress,  # Pass progress object
                    task_id=task_id,     # Pass task_id
                    length_mode_gene=getattr(args, 'len_gene', getattr(args, 'len', 'genelongesttxLength')),
                    length_mode_isoform=getattr(args, 'len_isoform', 'txLength'),
                    batch_size=getattr(args, 'batch_size', None),
                    quant=getattr(args, 'quant', 'none'),
                    engine=getattr(args, 'engine', 'auto')
                )
                count_files = counter.run()
                if getattr(args, 'verbose', False):
                    console.print(" [green]完成[/green]")
            except Exception as e:
                console.print(f" [bold red]处理失败: {str(e)}[/bold red]")
                import traceback
                traceback.print_exc()

        if getattr(args, 'verbose', False):
            console.print(f"\n[bold green]处理完成: 总共 {len(input_files)} 个文件[/bold green]")

    except Exception as e:
        console = Console(force_terminal=True)
        console.print(f"[bold red]错误: {str(e)}[/bold red]")


def count_main(args, progress=None, task_id=None):
    """主入口函数，根据参数选择并行或串行"""
    if not getattr(args, 'input', None) and getattr(args, 'read1', None):
        args.input = args.read1
    if getattr(args, 'read2', None) and not getattr(args, 'paired_end', None):
        args.paired_end = args.read2
    if hasattr(args, 'processes') and args.processes != 1:
        return count_main_parallel(args)
    else:
        return count_main_serial(args, progress, task_id)


class FanseCounter:
    """fanse3文件计数处理器"""

    def __init__(self, input_file, output_dir, level='isoform',
                 # minreads=0,
                 rpkm=0,
                 gxf_file=None,
                #  paired_end=None,
                 output_filename=None,
                 annotation_df=None,
                 verbose=False,
                 export_format='rsem',
                 export_count_type='Final_EM',
                 progress=None,  # New: rich progress object
                 task_id=None,   # New: rich task ID
                 length_mode='genelongesttxLength',  # 修正：默认改为基因层最长转录本长度
                 length_mode_gene=None,              # 新增：基因层长度模式（优先于 length_mode）
                 length_mode_isoform='txLength',     # 新增：转录本层长度模式
                 batch_size=None,  # 新增：批处理大小参数，允许用户通过CLI控制
                 quant='none',     # 新增：定量方法选择（none/tpm/rpkm/both），用于在唯一文件中追加TPM/RPKM
                 engine='auto'     # 新增：解析引擎选择（auto/python/rust），auto优先使用Rust
                 ):

        # 添加计数类型前缀
        self.isoform_prefix = 'isoform_'
        self.gene_prefix = 'gene_'

        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.level = level
        # self.minreads = minreads
        # self.rpkm = rpkm
        self.gxf_file = gxf_file
        self.paired_end = None
        self.output_filename = output_filename  # 新增：支持自定义输出文件名
        self.annotation_df = annotation_df  # 新增：注释数据框
        self.verbose = verbose
        self.export_format = export_format
        self.export_count_type = export_count_type
        self.progress = progress  # Store progress object
        self.task_id = task_id    # Store task ID
        # 修正：分别存储 gene 与 isoform 层的长度选择；保持 length_mode 兼容
        self.length_mode_gene = length_mode_gene if length_mode_gene else (length_mode or 'genelongesttxLength')
        self.length_mode_isoform = length_mode_isoform or 'txLength'
        self.length_mode = self.length_mode_gene
        # 新增：批处理大小（用于解析阶段批量更新计数），None表示使用默认策略
        self.batch_size = batch_size
        # 新增：定量方法（none/tpm/rpkm/both）
        self.quant = quant if quant in ('none','tpm','rpkm','both') else 'none'
        # 新增：解析引擎选择
        self.engine = engine if engine in ('auto','python','rust') else 'auto'
        self.console = Console(force_terminal=True)

        # # 存储计数结果
        # self.counts_data = {}
        # self.summary_stats = {}
        # self.multi_mapping_info = defaultdict(list)  # 存储多映射信息
        # 存储计数结果
        self.counts_data = {
            # isoform水平计数
            f'{self.isoform_prefix}raw': Counter(),
            f'{self.isoform_prefix}unique_to_isoform': Counter(),
            # f'{self.isoform_prefix}unique': Counter(),
            f'{self.isoform_prefix}multi_to_isoform': Counter(),
            f'{self.isoform_prefix}firstID': Counter(),

            f'{self.isoform_prefix}multi2all': Counter(),
            f'{self.isoform_prefix}multi_equal': Counter(),
            f'{self.isoform_prefix}multi_EM': Counter(),

            f'{self.isoform_prefix}Final_EM': Counter(),
            f'{self.isoform_prefix}Final_EQ': Counter(),
            f'{self.isoform_prefix}Final_MA': Counter(),
            f'{self.isoform_prefix}multi_EQ_ratio': Counter(),
            f'{self.isoform_prefix}multi_EM_ratio': Counter(),

            # gene水平计数
            f'{self.gene_prefix}raw': Counter(),
            f'{self.gene_prefix}multi_to_isoform': Counter(),
            # f'{self.gene_prefix}multi': Counter(),
            f'{self.gene_prefix}unique_to_isoform': Counter(),
            f'{self.gene_prefix}unique_to_gene': Counter(),
            f'{self.gene_prefix}firstID': Counter(),

            f'{self.gene_prefix}multi2all': Counter(),
            f'{self.gene_prefix}multi_equal': Counter(),
            f'{self.gene_prefix}multi_EM': Counter(),

            f'{self.gene_prefix}Final_EM': Counter(),
            f'{self.gene_prefix}Final_EQ': Counter(),
            f'{self.gene_prefix}Final_MA': Counter(),
            f'{self.gene_prefix}multi_EQ_ratio': Counter(),
            f'{self.gene_prefix}multi_EM_ratio': Counter(),
        }
        # 兼容旧键名：确保 isoform_multi 指向 isoform_multi_to_isoform
        # self.counts_data[f'{self.isoform_prefix}multi'] = self.counts_data[f'{self.isoform_prefix}multi_to_isoform']
        self.summary_stats = {}
        self.multi_mapping_info = defaultdict(list)

    def judge_sequence_mode(self):
        """判断测序模式（单端/双端）"""
        if self.paired_end and os.path.isfile(self.paired_end):
            if self.verbose:
                self.console.print('Pair-End mode detected.')
            return True
        else:
            if self.verbose:
                self.console.print('Single-End mode detected.')
            return False


# %% parser
    def parse_fanse_file_optimized_final(self, position=0):
        """综合优化版本"""
        # 选择解析器：默认强制使用高性能 Python 解析器；如启用 Rust 则优先走 Rust 路径
        fanse_parser_selected = fanse_parser_high_performance

        if self.verbose:
            self.console.print(f'Parsing {self.input_file.name}')
            # 修正：输出所选解析器与批处理大小，便于定位性能问题
            self.console.print(f"Parser: {'high_performance' if fanse_parser_selected is fanse_parser_high_performance else 'standard'}")
        start_time = time.time()

        # 预初始化数据结构，针对isoform。默认所有reads都比对到isoform,后续再根据这个，multi_to_isoform判断是否属于gene
        counts_data = {
            f'{self.isoform_prefix}raw': Counter(),
            f'{self.isoform_prefix}unique_to_isoform': Counter(),
            f'{self.isoform_prefix}multi_to_isoform': Counter(),
            f'{self.isoform_prefix}firstID': Counter(),

            f'{self.isoform_prefix}multi2all': Counter(),
            f'{self.isoform_prefix}multi_equal': Counter(),
            f'{self.isoform_prefix}multi_EM': Counter(),
            f'{self.isoform_prefix}multi_EM_cannot_allocate_tpm': Counter(),
            f'{self.isoform_prefix}Final_EM': Counter(),
            f'{self.isoform_prefix}Final_EQ': Counter(),
            f'{self.isoform_prefix}Final_MA': Counter(),
        }

        # 兼容旧键名，确保 isoform_multi 可用
        # counts_data[f'{self.isoform_prefix}multi'] = counts_data[f'{self.isoform_prefix}multi_to_isoform']

        total_count = 0
        # 修正：移除重复的 batch_size 赋值，统一使用实例属性 self.batch_size
        # 调整默认批大小至 2_000_000，减少函数调用与哈希开销，提高吞吐
        # 可通过 CLI --batch-size 覆盖以适配不同机器与数据规模
        batch_size = int(self.batch_size) if self.batch_size else 2_000_000
        if self.verbose:
            self.console.print(f"Using batch size: {batch_size}")
        # 修正：根据批大小动态设置进度更新步长，减少频繁刷新带来的开销
        # 大批量时放宽刷新频率，小批量保持默认
        update_interval = max(10_000, batch_size // 4)

        # 3. 本地变量缓存，消除属性查找
        raw      = counts_data[f'{self.isoform_prefix}raw']
        multi    = counts_data[f'{self.isoform_prefix}multi_to_isoform']
        unique   = counts_data[f'{self.isoform_prefix}unique_to_isoform']
        firstID  = counts_data[f'{self.isoform_prefix}firstID']
        multi2all= counts_data[f'{self.isoform_prefix}multi2all']

        FANSE_EXTS = {'.fanse3','.fanse','.fanse3.zip', '.fanse3.gz'}
        candidate_files = [self.input_file]
        
        #判断是否是双端fanse文件，如果是，判断是否有对应的双端文件，如果有，加入到candidate_files中
        if self.paired_end:
            pe_path = Path(self.paired_end)
            if pe_path.suffix in FANSE_EXTS:
                candidate_files.append(pe_path)
        # 遍历所有候选文件
        for position, fanse_file in enumerate(candidate_files):
            if self.progress and self.task_id:
                self.progress.update(self.task_id, description=f"[cyan]Parsing {fanse_file.name}...")
            
            # 获取文件大小用于统计进度条总数
            file_size = fanse_file.stat().st_size
            processed_bytes = 0
            
            # 如果是第一个文件，或者不是断点续传，则重置进度条
            if self.progress and self.task_id:
                self.progress.update(self.task_id, total=file_size)
                self.progress.update(self.task_id, completed=0)

            if self.progress and self.task_id:
                self.progress.update(self.task_id, description=f"Parsing {fanse_file.name}")
            # 如果有进度条，则更新任务描述
            if self.progress and self.task_id is not None:
                self.progress.update(self.task_id, description=f"[cyan]Processing {fanse_file.name}[/cyan]")
            if not fanse_file.exists():
                continue

            # 优先尝试 Rust 引擎（若启用且可用），一次性解析并返回计数；失败则回退Python
            if self.engine in ('rust','auto') and rust_fastcount_available() and parse_and_count_rust:
                try:
                    if self.verbose:
                        print("Using Rust fastcount engine")
                    res = parse_and_count_rust([str(fanse_file)])
                    # 将 Rust 返回的字典写入预初始化的计数器
                    raw.update(res.get('raw', {}))
                    firstID.update(res.get('firstID', {}))
                    unique.update(res.get('unique_to_isoform', {}))
                    multi.update(res.get('multi_to_isoform', {}))
                    multi2all.update(res.get('multi2all', {}))
                    total_count += sum(res.get('raw', {}).values())
                    continue  # 当前文件已完成，处理下一个候选文件
                except Exception as e:
                    if self.verbose:
                        print(f"Rust engine error, fallback to Python: {e}")

            try:
                batch = []
                # last_update = 0

                # file_size = fanse_file.stat().st_size
                # estimated_records = max(1, file_size // 527)
                # 智能估算记录数
                sample_size = 10_000  # 从fanse文件开头提取的fanse记录数目，用来估算总reads数
                estimated_records = self.calculate_file_record_estimate(
                    fanse_file, sample_size)
                if self.progress and self.task_id:
                    self.progress.update(self.task_id, total=estimated_records, completed=0)

                use_tqdm = not (self.progress and self.task_id)
                pbar = None
                if use_tqdm:
                    pbar = tqdm(total=estimated_records, unit='reads', mininterval=5, unit_scale=True, position=position, leave=False)

                # 保持与批大小一致的刷新步长，避免过于频繁刷新导致渲染开销
                update_counter = 0

                for i, record in enumerate(fanse_parser_selected(str(fanse_file))):
                    if record.ref_names:
                        total_count += 1

                        batch.append(record)
                        if len(batch) >= batch_size:
                            self._fast_batch_process(batch, raw, multi, unique, firstID, multi2all)
                            batch = []

                        update_counter += 1
                        if update_counter >= update_interval:
                            if pbar:
                                pbar.update(update_counter)
                            if self.progress and self.task_id:
                                self.progress.update(self.task_id, advance=update_counter)
                            update_counter = 0
                    else:
                        update_counter += 1

                if update_counter > 0:
                    if pbar:
                        pbar.update(update_counter)
                    if self.progress and self.task_id:
                        self.progress.update(self.task_id, advance=update_counter)

                if pbar:
                    pbar.close()

                if batch:
                    self._fast_batch_process(batch, raw, multi, unique, firstID, multi2all)

            except Exception as e:
                print(f"Error: {e}")
                continue

        duration = time.time() - start_time
        if self.verbose:
            print(
                f" Completed: {total_count} records in {duration:.2f}s ({total_count/duration:.0f} rec/sec)")

        return counts_data, total_count

    def _fast_batch_process(self, batch, raw, multi, unique, firstID, multi2all):
        """
        批量处理FANSe记录，按比对状态更新各类计数器。
        
        参数
        ----
        batch : list[FANSeRecord]
            待处理的一批记录。
        raw : collections.Counter
            原始计数器：记录所有reads（无论唯一/多重比对）的首次比对ID或拼接ID。
        multi : collections.Counter
            多重比对计数器：仅记录多重比对reads的拼接ID。
        unique : collections.Counter
            唯一比对计数器：仅记录唯一比对reads的ID。
        firstID : collections.Counter
            首ID计数器：记录所有reads（无论唯一/多重）的首个比对ID。
        multi2all : collections.Counter
            多重比对展开计数器：将多重比对reads的拼接ID拆开后，分别累加到每个转录本。
        """
        # 优化：批量聚合更新，减少每条记录多次 Counter.update 的方法调用开销
        # 将原“每记录5次update”改为“每批每类型1次update”，显著降低Python调用与哈希查找开销
        raw_ids_to_add = []
        first_ids_to_add = []
        unique_ids_to_add = []
        multi_joined_to_add = []
        multi2all_ids_to_add = []

        for record in batch:
            ids = record.ref_names          # 当前read比对到的所有转录本ID列表
            is_multi = record.is_multi      # 是否为多重比对（True=多重，False=唯一）
            first_id = ids[0]               # 取首个比对ID作为代表

            if not is_multi:
                # 唯一比对分支
                raw_ids_to_add.append(first_id)
                first_ids_to_add.append(first_id)
                unique_ids_to_add.append(first_id)
            else:
                # 多重比对分支
                # 修正：统一组合键类型为 tuple，内部统一使用元组以减少重复字符串拼接与哈希重分配；导出阶段再格式化为逗号分隔字符串
                joined_tuple = tuple(ids)
                raw_ids_to_add.append(joined_tuple)
                first_ids_to_add.append(first_id)
                multi_joined_to_add.append(joined_tuple)
                # 展开到每个转录本
                # 注意：这里不去重，保留重复以准确累计每条reads对应的每个转录本
                multi2all_ids_to_add.extend(ids)

        if raw_ids_to_add:
            raw.update(raw_ids_to_add)
        if first_ids_to_add:
            firstID.update(first_ids_to_add)
        if unique_ids_to_add:
            unique.update(unique_ids_to_add)
        if multi_joined_to_add:
            multi.update(multi_joined_to_add)
        if multi2all_ids_to_add:
            multi2all.update(multi2all_ids_to_add)

        # # 修正：使用生成器进行Counter.update，避免构建巨大中间列表导致的内存压力与GC抖动
        # # 说明：保持“每批每类型1次update”的策略，同时用生成器表达式与chain展开multi2all
        # raw.update(
        #     (
        #         (record.ref_names[0] if not record.is_multi else tuple(record.ref_names))
        #         for record in batch
        #     )
        # )
        # firstID.update((record.ref_names[0] for record in batch))
        # unique.update((record.ref_names[0] for record in batch if not record.is_multi))
        # multi.update((tuple(record.ref_names) for record in batch if record.is_multi))
        # # 将多重比对展开到每个转录本ID：使用chain避免一次性构建巨大的列表
        # multi2all.update(
        #     chain.from_iterable((record.ref_names for record in batch if record.is_multi))
        # )


    def calculate_average_record_size(self, file_path, sample_size=100_000):
        """
        通过采样计算fanse3文件的平均记录大小

        参数:
            file_path: 文件路径
            sample_size: 采样记录数（默认100,000条）

        返回:
            平均每条记录的字节数
        """
        if self.verbose:
            print(f"采样计算平均记录大小，采样数: {sample_size}")

        try:
            total_bytes = 0
            record_count = 0

            # 使用fanse_parser进行采样
            for i, record in enumerate(fanse_parser(str(file_path))):
                if i >= sample_size:
                    break

                # 估算当前记录的大小（基于记录内容的字符串长度）
                record_size = len(str(record))  # 基本估算
                total_bytes += record_size
                record_count += 1

            if record_count > 0:
                avg_size = total_bytes / record_count
                if self.verbose:
                    print(f"采样完成: {record_count} 条记录，平均大小: {avg_size:.1f} 字节")
                return avg_size
            else:
                if self.verbose:
                    self.console.print("警告: 无法采样记录，使用默认值500")
                return 500

        except Exception as e:
            if self.verbose:
                self.console.print(f"采样失败: {e}，使用默认值500")
            return 500

    def calculate_file_record_estimate(self, file_path, sample_size=100_000):
        """
        综合估算文件中的记录数量

        参数:
            file_path: 文件路径
            sample_size: 采样大小

        返回:
            估计的记录数量
        """
        if not file_path.exists():
            return 0

        # 获取文件大小
        file_size = file_path.stat().st_size

        # 如果是小文件，直接解析计数
        if file_size < 100 * 1024 * 1024:
            if self.verbose:
                self.console.print("小文件，直接计数...")
            try:
                record_count = sum(
                    1 for _ in fanse_parser(str(file_path)))
                if self.verbose:
                    self.console.print(f"直接计数完成: {record_count} 条记录")
                return record_count
            except:
                pass
        else:
            # 对于大文件，使用采样估算
            avg_size = self.calculate_average_record_size(
                file_path, sample_size)*0.8  # 经验乘以0.8，人为增大一点估算的reads总数，反而比较符合实际，也是估算
            estimated_records = max(1, int(file_size / avg_size))

        if self.verbose:
            self.console.print(f"文件大小: {file_size/1_000_000_000:.2f} GB ")
            self.console.print(f"平均记录大小: {avg_size:.0f} 字节")
            self.console.print(f"估计Fanse记录数: {estimated_records/1_000_000:.2f} M")

        return estimated_records


# %% generate counts

    def _rescue_multi_mappings_by_tpm(self, counts_data, prefix=None, length_dict=None, annotation_df=None):
        """
        通用多映射处理 - 
        1.支持isoform和gene level前缀
        2.支持记录multi mapped reads的分配比例
        参数:
            counts_data: 计数数据字典
            prefix: 前缀类型 ('isoform_' 或 'gene_')，如果为None则使用isoform_prefix
            length_dict: ID到长度的映射字典，如果为None则从annotation_df获取
            annotation_df: 注释数据框，如果为None则使用实例的annotation_df

        """
        if prefix is None:
            prefix = self.isoform_prefix  # 默认使用isoform前缀
        if self.verbose:
            self.console.print(f"开始高级多映射分析 (前缀: {prefix})...")

        # 检查是否有multi数据
        multi_key = f'{prefix}multi_to_isoform'
        if multi_key not in counts_data or not counts_data[multi_key]:
            if self.verbose:
                self.console.print(f"没有{prefix}多映射数据，跳过高级分析")
            return

        # 获取长度信息（统一入口）
        if length_dict is None:
            length_dict = self._build_length_dict(prefix, annotation_df)
            if self.verbose:
                self.console.print(f"加载了 {len(length_dict)} 个{prefix}ID的长度信息（模式: {self.length_mode}）")

        # 通过unique部分计算TPM：
        # 为gene水平添加特殊处理,用unique_to_gene的reads，而不用unique_to_isoform.重要。因为很多reads映射到多个isoform
        #一些来自同一个基因的reads，会被分配到多个isoform而导致每个isoform没有unique reads，而unique_to_gene可以让gene具有unique reads，而被挽救
        #这种数目有多少，可以尝试统计一下，显示到摘要里面。
        if prefix == self.gene_prefix :  #and hasattr(self, 'gene_level_counts_unique_genes')
            unique_key = f'{prefix}unique_to_gene'
            # 如果找不到对应的计数器，否则就新建一个空的 Counter，避免后续代码因 KeyError 崩溃。实际是可以直接找到的，因此不必担心是空的
            unique_source = counts_data.get(unique_key, Counter())
            tpm_values = self._calculate_tpm(unique_source, length_dict)
       
        # isoform水平，直接用unique_to_isoform即可，无需考虑
        elif prefix == self.isoform_prefix: 
            unique_key = f'{prefix}unique_to_isoform'
            unique_source = counts_data.get(unique_key, Counter())
            tpm_values = self._calculate_tpm(unique_source, length_dict)

        if self.verbose:
            self.console.print(f"计算了 {len(tpm_values)} 个具有unique reads {prefix}ID的TPM值")

        # 初始化计数器
        multi_equal_counter = Counter()
        multi_em_counter = Counter()
        multi_em_cannot_allocate_tpm_counter = Counter()

        processed_events = 0
        total_events = len(counts_data[multi_key])

        if self.verbose:
            self.console.print(f"开始处理 {total_events} 个{prefix}多映射事件...")

        for ids_key, event_count in counts_data[multi_key].items():
            try:
                ids = ids_key.split(',') if isinstance(ids_key, str) else list(ids_key)

                # multi_equal: 平均分配
                equal_alloc = self._distribute_equal(ids, event_count)
                for id_val, cnt in equal_alloc.items():
                    multi_equal_counter[id_val] += cnt

                # multi_EM: 按具有unique reads 的  isoform  或者 gene  的 TPM比例分配 multi-reads
                allocation = self._allocate_multi_reads_by_tpm_rescued(
                    ids, tpm_values)
                
                if allocation:
                    # 如果可以分配TPM，按比例分配
                    for id_val, share_ratio in allocation.items():
                        multi_em_counter[id_val] += event_count * share_ratio
                else:
                    # 无法分配的情况，我们不采取按照相等比例分配的办法，而是采取不分配的方案。确保没有UNIQUE 的reads 不参与继续分配，以保持严谨性
                    # 这些reads会被分配到multi_EM_cannot_allocate_tpm_counter中
                    multi_em_cannot_allocate_tpm_counter.update({ids_key: event_count})
                if self.verbose:
                    processed_events += 1
                    if processed_events % 10000 == 0:
                        self.console.print(f"已处理 {processed_events}/{total_events} 个{prefix}多映射事件")

            except Exception as e:
                self.console.print(f"[bold red]处理{prefix}多映射事件 {ids_key} 时出错: {str(e)}[/bold red]")
                continue

        # 更新计数器
        counts_data[f'{prefix}multi_equal'] = multi_equal_counter
        counts_data[f'{prefix}multi_EM'] = multi_em_counter
        counts_data[f'{prefix}multi_EM_cannot_allocate_tpm'] = multi_em_cannot_allocate_tpm_counter
        if self.verbose:
            self.console.print(f"{prefix}高级多映射分析完成：")
            self.console.print(f"  - {prefix}multi_equal: {len(multi_equal_counter)} 个ID")
            self.console.print(f"  - {prefix}multi_EM: {len(multi_em_counter)} 个ID")
            self.console.print(f"  - 无法分配TPM的事件: {len(multi_em_cannot_allocate_tpm_counter)} 个")

        # return None

    def _rescue_multi_mappings_by_tpm_isoform(self, counts_data):
        """isoform水平的多映射处理（向后兼容）"""
        return self._rescue_multi_mappings_by_tpm(counts_data, prefix=self.isoform_prefix)

    def _rescue_multi_mappings_by_tpm_gene(self, counts_data):
        """gene水平的多映射处理"""
        return self._rescue_multi_mappings_by_tpm(counts_data, prefix=self.gene_prefix)

    def _calculate_tpm(self, unique_counts, lengths):
        '''
        """计算每个基因的TPM值"""
        TPM是一种常用的基因表达标准化方法，能够消除基因长度和测序深度的影响。
        正确的计算步骤分为两步：
        - 第一步是RPK标准化，用基因的原始reads数除以基因长度(以千碱基为单位)；
        - 第二步是总和标准化，将所有基因的RPK值相加，然后用每个基因的RPK值除以这个总和再乘以一百万。
        '''
        if not unique_counts or not lengths:
            return {}

        # 计算RPK (Reads Per Kilobase)
        rpk_values = {}
        total_rpk = 0

        for transcript, count in unique_counts.items():
            if transcript in lengths and lengths[transcript] > 0:
                length_kb = lengths[transcript] / 1000
                rpk = count / length_kb
                rpk_values[transcript] = rpk
                total_rpk += rpk  # 计算总rpk

        # 计算TPM (Transcripts Per Million)
        tpm_values = {}
        if total_rpk > 0:
            scaling_factor = 1e6 / total_rpk
            for transcript, rpk in rpk_values.items():
                tpm_values[transcript] = rpk * scaling_factor

        return tpm_values

    def _allocate_multi_reads_by_tpm_rescued(self, transcript_ids, tpm_values):
        """根据unique 计算的  TPM值分配多映射reads"""
        allocation = {}
         #这个分配情况是否合适，有待后续再检查一下。还没想明白的感觉。为什么直接分配1
        if len(transcript_ids) == 1:
            tid = transcript_ids[0]
            return {tid: 1.0}

        # 过滤掉没有TPM值的转录本
        valid_transcripts = [tid for tid in transcript_ids if tid in tpm_values and tpm_values[tid] > 0]

        if not valid_transcripts:
            # 回退到平均分配，，，这个有点不太合适，放在另一个列表multi_EM_cannot_allocate_tpm里，不参与分配  20251111
            return None
            # share = 1.0 / len(transcript_ids)
            # return  {tid: share for tid in transcript_ids}

        # 计算总TPM
        total_tpm = sum(tpm_values[tid] for tid in valid_transcripts)

        # 按TPM比例分配total_tpm
        for tid in valid_transcripts:
            allocation[tid] = tpm_values[tid] / total_tpm

        # 处理不在valid_transcripts中的转录本,其实什么都不做最好了，先注释掉20251125
        # invalid_transcripts = [
        #     tid for tid in transcript_ids if tid not in valid_transcripts]
        # if invalid_transcripts and total_tpm > 0:
        #     remaining_share = 1.0 - sum(allocation.values())
        #     if remaining_share > 0:
        #         share_per_invalid = remaining_share / len(invalid_transcripts)
        #         for tid in invalid_transcripts:
        #             allocation[tid] = share_per_invalid

        return allocation

    def _distribute_equal(self, ids, event_count):
        """平均分配多映射reads到各ID，返回{id: count}映射"""
        if not ids:
            return {}
        share = event_count / float(len(ids))
        return {id_val: share for id_val in ids}

    def _build_length_dict(self, prefix, annotation_df=None):
        """构建用于TPM/EM分配的长度字典
        - prefix 为 'isoform_' 或 'gene_'
        - 根据分别设置的长度模式选择具体列
        - 兼容缺失列，自动回退到合适的列
        """
        df = annotation_df if annotation_df is not None else self.annotation_df
        if df is None or df.empty:
            return {}

        if prefix == self.isoform_prefix:
            # 修正：支持 isoform 层长度选择（txLength/cdsLength/isoformEffectiveLength）
            id_col = 'txname' if 'txname' in df.columns else None
            if not id_col:
                return {}
            mode_iso = self.length_mode_isoform or 'txLength'
            candidates = [mode_iso, 'txLength', 'isoformEffectiveLength', 'cdsLength']
            selected = None
            for col in candidates:
                if col in df.columns:
                    selected = col
                    break
            if not selected:
                return {}
            return dict(zip(df[id_col], df[selected]))

        # gene level
        id_col = 'geneName' if 'geneName' in df.columns else None
        if not id_col:
            return {}

        # 修正：支持 gene 层长度选择（geneEffectiveLength/genelongesttxLength/txLength/genelongestcdsLength）
        mode_gene = self.length_mode_gene or 'genelongesttxLength'
        candidates = [mode_gene, 'geneEffectiveLength', 'geneNonOverlapLength', 'geneReadCoveredLength', 'genelongesttxLength', 'genelongestcdsLength', 'txLength']
        # 选择第一个存在的列
        selected = None
        for col in candidates:
            if col in df.columns:
                selected = col
                break
        if selected is None:
            # 回退：使用每个基因最长转录本长度（从 txLength 聚合）
            if 'txLength' in df.columns:
                if self.verbose:
                    self.console.print(f"[len] 未找到列 {mode_gene} ，回退为按 txLength 聚合的基因最长转录本长度")
                return df.groupby(id_col)['txLength'].max().to_dict()
            return {}

        # gene 层面长度字典：若 selected 是转录本列需聚合
        if selected == 'txLength':
            if self.verbose:
                self.console.print(f"[len] 选择 txLength，按基因聚合为最长转录本长度用于归一化")
            return df.groupby(id_col)['txLength'].max().to_dict()
        if selected == 'genelongestcdsLength':
            return df.groupby(id_col)['genelongestcdsLength'].max().to_dict()
        if self.verbose:
            self.console.print(f"[len] 使用列 {selected} 作为 {prefix} 层面的长度指标")
        return df.groupby(id_col)[selected].max().to_dict()

    def generate_isoform_level_counts(self, counts_data, total_count):
        """
        根据解析的计数数据生成isoform水平的各种计数
        """
        if self.verbose:
            print("Generating isoform level counts...")
        start_time = time.time()
        # 第一阶段：已经在parse_fanse_file_optimized_final中计算了raw_counts, firstID_counts, \
        #                                   unique_counts, multi2all_counts, multi_equal_counts, multi_EM_counts.
        # 第二阶段： **重要** 针对多映射计数进行重新分配rescue multi mapped reads
        if counts_data[f'{self.isoform_prefix}multi_to_isoform']:
            if self.verbose:
                print("Starting advanced multi-mapping analysis...")
            # self._rescue_multi_mappings_by_tpm(counts_data)
            self._rescue_multi_mappings_by_tpm(
                counts_data, prefix=self.isoform_prefix)
            if self.verbose:
                print("Advanced multi-mapping analysis completed.")

        # 第三阶段:计算正确的counts，合并raw和multi_EM，以及multi_equal 的counts
        if self.verbose:
            print("Starting third stage: merging counts...")

        # 初始化合并计数器
        counts_data[f'{self.isoform_prefix}Final_EM'] = Counter()
        counts_data[f'{self.isoform_prefix}Final_EQ'] = Counter()
        counts_data[f'{self.isoform_prefix}Final_MA'] = Counter()

        # 1. 合并 unique 和 multi_EM 计数 (Final_EM)
        counts_data[f'{self.isoform_prefix}Final_EM'].update(counts_data[f'{self.isoform_prefix}unique_to_isoform'])
        counts_data[f'{self.isoform_prefix}Final_EM'].update(counts_data[f'{self.isoform_prefix}multi_EM'])

        # 2. 合并 unique 和 multi_equal 计数 (Final_EQ)
        counts_data[f'{self.isoform_prefix}Final_EQ'].update(counts_data[f'{self.isoform_prefix}unique_to_isoform'])
        counts_data[f'{self.isoform_prefix}Final_EQ'].update(counts_data[f'{self.isoform_prefix}multi_equal'])

        # 3. 合并 unique 和 multi2all 计数 (Final_MA)
        counts_data[f'{self.isoform_prefix}Final_MA'].update(counts_data[f'{self.isoform_prefix}unique_to_isoform'])
        counts_data[f'{self.isoform_prefix}Final_MA'].update(counts_data[f'{self.isoform_prefix}multi2all'])

        # 验证合并结果
        total_em = sum(counts_data[f'{self.isoform_prefix}Final_EM'].values())
        total_eq = sum(counts_data[f'{self.isoform_prefix}Final_EQ'].values())
        total_unique_to_isoform = sum( counts_data[f'{self.isoform_prefix}unique_to_isoform'].values())
        total_multi_em = sum(counts_data[f'{self.isoform_prefix}multi_EM'].values())
        total_multi_eq = sum(counts_data[f'{self.isoform_prefix}multi_equal'].values())

        if self.verbose:
            print("合并验证:")
            print(f"  - unique_to_isoform计数总计: {total_unique_to_isoform}")
            print(f"  - multi_EM计数总计: {round(total_multi_em)}")
            print(f"  - multi_equal计数总计: {round(total_multi_eq)}")
            print(f"  - Final_em总计: {round(total_em)} ")
            print(f"  - Final_eq总计: {round(total_eq)} ")

        # 更新实例变量
        self.counts_data = counts_data
        self.summary_stats = {
            'total_reads': total_count,
            'unique_mapped': total_unique_to_isoform,
            'multi_mapped': sum(counts_data[f'{self.isoform_prefix}multi_to_isoform'].values()),
            'raw': sum(counts_data[f'{self.isoform_prefix}raw'].values()),
            'firstID': sum(counts_data[f'{self.isoform_prefix}firstID'].values()),
            'multi_equal': total_multi_eq,
            'multi_EM': total_multi_em,
            'multi_EM_cannot_allocate_tpm': sum(counts_data[f'{self.isoform_prefix}multi_EM_cannot_allocate_tpm'].values()),
            'Final_em': total_em,
            'Final_eq': total_eq,
            'processing_time': time.time() - start_time
        }

        if self.verbose:
            print(
                f"Count generation completed in {self.summary_stats['processing_time']:.2f} seconds")
            print("最终计数统计:")
            print(
                f"  - Final_EM: {len(counts_data[f'{self.isoform_prefix}Final_EM'])} 个转录本, {round(total_em)} 条reads")
            print(
                f"  - Final_EQ: {len(counts_data[f'{self.isoform_prefix}Final_EQ'])} 个转录本, {round(total_eq)} 条reads")


    def aggregate_gene_level_counts(self):
        """
        基因水平计数聚合
        """
        if self.annotation_df is None:
            if self.verbose:
                self.console.print("Warning: Can not aggregate gene level counts without annotation data")
            return {}, {}

        if self.verbose:
            self.console.print("Aggregating gene level counts...")
        start_time = time.time()

        # 创建转录本到基因的映射列表
        # 修正：对 geneName 应用 sys.intern，稳定键匹配并降低内存占用（与解析器中对 ref_names 的驻留一致）
        import sys
        transcript_to_gene = {
            str(tx): sys.intern(str(gn))
            for tx, gn in zip(self.annotation_df['txname'], self.annotation_df['geneName'])
        }

        # 初始化基因水平计数器
        # gene_level_counts_unique_genes：存放“唯一比对到某个基因”的各类计数结果
        # gene_level_counts_multi_genes：存放“同时比对到多个基因”的各类计数结果
        gene_level_counts_unique_genes = {}
        gene_level_counts_multi_genes  = {}


        # 初始化所有基因计数类型
        for count_type in self.counts_data.keys():
            if count_type.startswith(self.isoform_prefix):
                # 将isoform替换为gene的计数类型，unique和multi两类都重新定义好
                base_type = count_type.replace(self.isoform_prefix, '')
                gene_level_counts_unique_genes[f'{self.gene_prefix}{base_type}'] = Counter()
                gene_level_counts_multi_genes[f'{self.gene_prefix}{base_type}']  = Counter()

        # 额外补充：基因层直接记录“跨基因的多重组合原始计数”
        gene_level_counts_multi_genes[f'{self.gene_prefix}multi_to_gene']    = Counter()
        # 明确定义两类基因唯一计数,前面有定义过，这里重新定义为了解读方便省心
        # 修正：补充并规范基因唯一细分计数器的键名，避免后续更新时报 KeyError
        # - unique_to_gene_and_unique_isoform：同时在 isoform 与 gene 层唯一
        # - unique_to_gene_but_not_unique_isoform：在 gene 层唯一但 isoform 层不唯一
        gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene_and_isoform'] = Counter()
        gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene_but_not_isoform'] = Counter()
        gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene']   = Counter()

        # gene_level_counts_unique_genes[f'{self.gene_prefix}multi_to_isoform'] = self.counts_data.get(f'{self.isoform_prefix}multi_to_isoform', Counter())
        # gene_level_counts_unique_genes[f'{self.gene_prefix}multi_to_gene']    = Counter()

        count_type = f'{self.isoform_prefix}raw'
        counter_raw = self.counts_data[count_type]  #提取isoform_raw的counter计数器，gene level 的所有count统计 采用这个计数器来计数就够了，避免AI幻觉
        # 去掉 isoform 前缀，得到基础计数类型名
        # 如 'isoform_unique_to_isoform' -> 'unique_to_isoform'
        # base_type = count_type.replace(self.isoform_prefix, '')

        # 遍历当前计数器中的每一条记录：键为转录本ID（或组合），值为对应reads数
        for transcript_ids_key, event_count in counter_raw.items():
            # 判断该键是否为“多转录本组合”——既可能是tuple，也可能是逗号分隔的字符串
            if isinstance(transcript_ids_key, tuple) or (isinstance(transcript_ids_key, str) and ',' in transcript_ids_key):
                # 统一转成列表：tuple直接转；字符串按逗号拆分
                transcript_ids = list(transcript_ids_key) if isinstance(transcript_ids_key, tuple) else transcript_ids_key.split(',')
                # 用set收集这些转录本对应的所有基因名（自动去重）
                genes = set()
                for tid in transcript_ids:
                    g = transcript_to_gene.get(tid)   # 通过预建的映射拿到基因名
                    if g:                             # 仅当映射成功才加入
                        genes.add(g)
               
                #不论如何，firstID都要更新，所以放到if外面，避免受干扰（例如在 len(genes) == 1中，不在第一个）
                #这里还必须用transcript_id,因为这个才是正确的顺序，gene已经set过，顺序变化了，而且唯一，所以不准了
                # 修正：直接获取首转录本所属基因名称（字符串），避免对字符串迭代导致仅取首字符
                first_gene = transcript_to_gene.get(transcript_ids[0])
                if first_gene:
                    gene_level_counts_unique_genes[f'{self.gene_prefix}firstID'].update({first_gene: event_count})
                
                # 1) 多转录本 → 单基因：isoform 不唯一但 gene 唯一
                if len(genes) == 1:
                    gene = genes.pop()          # 唯一基因
                    # 基础计数
                    gene_level_counts_unique_genes[f'{self.gene_prefix}raw'].update({gene: event_count})
                    gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene'].update({gene: event_count})

                    # 细分：区分“是否唯一比对到 isoform”
                    first_tx = transcript_ids[0]
                    if first_tx in self.counts_data.get(f'{self.isoform_prefix}unique_to_isoform', Counter()):
                        # 首转录本在gene层和 isoform 层都唯一 → 同时唯一到 isoform & gene
                        gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene_and_isoform'].update({gene: event_count})
                    else:
                        # 首转录本在 isoform 层不唯一 → 仅 gene 唯一
                        gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene_but_not_isoform'].update({gene: event_count})

                # 2) 多转录本 → 多基因：gene 不唯一
                elif len(genes) > 1:
                    # 修正：统一组合键为 tuple（按字典序排序保证稳定），导出阶段再格式化
                    gene_key = tuple(sorted(genes))
                    # 全部归入“基因-多重”计数器
                    gene_level_counts_multi_genes[f'{self.gene_prefix}raw'].update({gene_key: event_count})
                    gene_level_counts_multi_genes[f'{self.gene_prefix}multi_to_gene'].update({gene_key: event_count})
                    gene_level_counts_multi_genes[f'{self.gene_prefix}multi_to_isoform'].update({gene_key: event_count})

            # 3) 单转录本 → 单基因：isoform & gene 均唯一
            else:
                gene = transcript_to_gene.get(transcript_ids_key)
                if gene:
                    gene_level_counts_unique_genes[f'{self.gene_prefix}raw'].update({gene: event_count})
                    gene_level_counts_unique_genes[f'{self.gene_prefix}firstID'].update({gene: event_count})
                    gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene'].update({gene: event_count})
                    gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_isoform'].update({gene: event_count})
                    gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene_and_isoform'].update({gene: event_count})

        if self.verbose:
            print(f"基因水平 unique reads 计数完成: "
                  f"{len(gene_level_counts_unique_genes[f'{self.gene_prefix}raw'])} 个基因")

        # 第二步：使用基因水平的unique reads计算TPM,暂时用最长转录本长度

        # 用有unique reads的genes为gene水平的基因们构建长度字典（这里目前采用基因最长转录本长度，还可以采用其他种类长度替代）
        
        # 1. 统一构建所有基因的长度字典（依据 --len 选择）
        gene_lengths = self._build_length_dict(self.gene_prefix, self.annotation_df)
        
        # 21. 检查unique和gene水平的东西都存在哎
        if gene_level_counts_multi_genes:
            # 检查是否有gene水平的multi数据，可以参与分配EM reads
            gene_multi_key = f'{self.gene_prefix}multi_to_gene'
            if gene_level_counts_multi_genes.get(gene_multi_key):
                if self.verbose:
                    print("Starting advanced multi-mapping analysis for gene level...")

                # 修正：基因层 EM/Equal 分配需要使用 unique_to_gene 的 TPM 视图
                # 因此构造一个临时 counts_data，包含 multi_to_isoform 与 unique_to_gene 两个来源
                gene_counts_for_rescue = {
                    f'{self.gene_prefix}multi_to_isoform': gene_level_counts_multi_genes.get(f'{self.gene_prefix}multi_to_isoform', Counter()),
                    f'{self.gene_prefix}unique_to_gene': gene_level_counts_unique_genes.get(f'{self.gene_prefix}unique_to_gene', Counter()),
                }

                self._rescue_multi_mappings_by_tpm(
                    counts_data=gene_counts_for_rescue,
                    prefix=self.gene_prefix,
                    length_dict=gene_lengths,
                    annotation_df=self.annotation_df,
                )

                # 将分配结果写回 gene_level_counts_multi_genes
                gene_level_counts_multi_genes[f'{self.gene_prefix}multi_equal'] = gene_counts_for_rescue.get(f'{self.gene_prefix}multi_equal', Counter())
                gene_level_counts_multi_genes[f'{self.gene_prefix}multi_EM'] = gene_counts_for_rescue.get(f'{self.gene_prefix}multi_EM', Counter())
                gene_level_counts_multi_genes[f'{self.gene_prefix}multi_EM_cannot_allocate_tpm'] = gene_counts_for_rescue.get(f'{self.gene_prefix}multi_EM_cannot_allocate_tpm', Counter())
                if self.verbose:
                    print("Gene level advanced multi-mapping analysis completed.")


        # 第四步：合并计数
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EM'] = Counter()
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EQ'] = Counter()
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_MA'] = Counter()

        # 1. 合并 unique_to_gene 和 multi_EM 计数 (最终的Final_EM)
        # Final_EM = unique_to_gene + multi_EM
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EM'].update(
            gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene']
        )
        # 修正：multi_EM 来源应为 gene_level_counts_multi_genes（EM 分配结果），而非 unique 字典
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EM'].update(
            gene_level_counts_multi_genes.get(f'{self.gene_prefix}multi_EM', Counter())
        )

        # 2. 合并 unique_to_gene 和 multi_equal 计数 (Final_EQ)
        # Final_EQ = unique_to_gene + multi_equal
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EQ'].update(
            gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene']
        )
        # 修正：multi_equal 来源应为 gene_level_counts_multi_genes（平均分配结果），而非 unique 字典
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EQ'].update(
            gene_level_counts_multi_genes.get(f'{self.gene_prefix}multi_equal', Counter())
        )

        # 3. 合并 unique_to_gene 和 multi2all 计数 (Final_MA)
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_MA'].update(
            gene_level_counts_unique_genes[f'{self.gene_prefix}unique_to_gene']
        )
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_MA'].update(
            gene_level_counts_unique_genes[f'{self.gene_prefix}multi2all']
        )

        # 计算基因水平的multi_equal_ratio和multi_EM_ratio
        if self.verbose:
            print("Calculating gene level multi-mapping ratios...")
        # if f'{self.gene_prefix}multi_equal_ratio' not in gene_level_counts_unique_genes:
        gene_level_counts_unique_genes[f'{self.gene_prefix}multi_equal_ratio'] = Counter()
        # if f'{self.gene_prefix}multi_EM_ratio' not in gene_level_counts_unique_genes:
        gene_level_counts_unique_genes[f'{self.gene_prefix}multi_EM_ratio'] = Counter()

        for gene_id in gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EQ'].keys():
            # multi_equal_ratio
            total_gene_reads_equal  = gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EQ'].get(gene_id, 0) 
            if total_gene_reads_equal > 0:
                # 修正：multi_equal 计数来自 gene_level_counts_multi_genes
                multi_equal_count = gene_level_counts_multi_genes.get(f'{self.gene_prefix}multi_equal', Counter()).get(gene_id, 0)
                gene_level_counts_unique_genes[f'{self.gene_prefix}multi_equal_ratio'][gene_id] = multi_equal_count / total_gene_reads_equal

            # multi_EM_ratio
            total_gene_reads_em = gene_level_counts_unique_genes[f'{self.gene_prefix}Final_EM'].get(gene_id, 0)   
            if total_gene_reads_em > 0:
                # 修正：multi_EM 计数来自 gene_level_counts_multi_genes
                multi_em_count   = gene_level_counts_multi_genes.get(f'{self.gene_prefix}multi_EM', Counter()).get(gene_id, 0)
                gene_level_counts_unique_genes[f'{self.gene_prefix}multi_EM_ratio'][gene_id] = multi_em_count / total_gene_reads_em
        processing_time = time.time() - start_time
        if self.verbose:
            print(f"基因水平聚合完成，耗时 {processing_time:.2f} 秒")

        return gene_level_counts_unique_genes, gene_level_counts_multi_genes


# %% run and generate files

    def _generate_isoform_level_files(self, base_name):
        """生成转录本水平计数文件 - 修复版"""
        isoform_files = {}

        try:
            # 收集所有转录本水平计数类型
            isoform_count_types = []
            for count_type in self.counts_data.keys():
                if count_type.startswith(self.isoform_prefix):
                    base_type = count_type.replace(self.isoform_prefix, '')
                    isoform_count_types.append(base_type)

            if self.verbose:
                print(f"找到转录本水平计数类型: {isoform_count_types}")

            if not isoform_count_types:
                if self.verbose:
                    print("没有转录本水平计数数据")
                return {}

            # 使用firstID作为基础数据框,因为这个包含所有的reads
            firstID_type = f'{self.isoform_prefix}firstID'
            # if firstID_type not in self.counts_data or not self.counts_data[firstID_type]:
            #     if self.verbose:
            #         print("没有firstID计数数据，无法生成转录本水平文件")
            #     return {}

            base_items = [
                (k if isinstance(k, str) else ','.join(k), v)
                for k, v in self.counts_data[firstID_type].items()
            ]
            combined_df = pd.DataFrame(base_items, columns=['Transcript', 'firstID'])

            # 合并所有计数类型
            for count_type in isoform_count_types:
                if count_type == 'firstID':  # 已经作为基础，跳过
                    continue

                full_type = f'{self.isoform_prefix}{count_type}'
                if full_type in self.counts_data and self.counts_data[full_type]:
                    items = [
                        (k if isinstance(k, str) else ','.join(k), v)
                        for k, v in self.counts_data[full_type].items()
                    ]
                    # 重命名unique_count为unique_to_isoform_count，保留multi_to_isoform_count
                    col_name = f'{count_type}'
                    if count_type == 'unique':
                        col_name = 'unique_to_isoform'
                    temp_df = pd.DataFrame(items, columns=['Transcript', col_name])
                    combined_df = combined_df.merge(
                        temp_df, on='Transcript', how='outer')

            # if 'multi_count' in combined_df.columns:
            #     combined_df['multi_to_isoform_count'] = combined_df['multi_count']
            #     combined_df = combined_df.drop(columns=['multi_count'])

            # 添加注释信息（如果有）
            if self.annotation_df is not None:
                # 检查可用的注释列
                available_columns = self.annotation_df.columns.tolist()
                annotation_cols = ['txname', 'geneName']

                # 添加其他可能有用的列
                optional_cols = ['txLength',
                                 'cdsLength', 'symbol', 'description']
                for col in optional_cols:
                    if col in available_columns:
                        annotation_cols.append(col)

                annotation_subset = self.annotation_df[annotation_cols]
                combined_df = combined_df.merge(
                    annotation_subset,
                    left_on='Transcript',
                    right_on='txname',
                    how='left'
                )

                # 移除重复的txname列（如果存在）
                if 'txname' in combined_df.columns and 'Transcript' in combined_df.columns:
                    combined_df = combined_df.drop('txname', axis=1)

            # 别名与列规范：区分isoform/gene层的TPM不可分配事件
            if 'multi_EM_cannot_allocate_tpm' in combined_df.columns:
                combined_df['multi_EM_cannot_allocate_tpm_isoform'] = combined_df['multi_EM_cannot_allocate_tpm']
                combined_df = combined_df.drop(columns=['multi_EM_cannot_allocate_tpm'])

            # 填充NaN值为0
            count_columns = [
                col for col in combined_df.columns if col.endswith('_count')]
            combined_df[count_columns] = combined_df[count_columns].fillna(0)

            # 若启用定量计算：为目标列追加 TPM/RPKM 列
            try:
                if self.quant and self.quant != 'none' and add_quant_columns is not None and build_length_maps is not None:
                    # 选择长度映射（转录本层）
                    df_anno = self.annotation_df if self.annotation_df is not None else pd.DataFrame()
                    # 修正：唯一文件的主键列为 'Transcript'，不可使用注释中的 'txname'
                    id_col = 'Transcript'
                    # 修正：传递 isoform 层长度选择模式
                    length_map, eff_length_map = build_length_maps(df_anno, level='isoform', mode=self.length_mode_isoform)
                    target_cols = ['Final_EM','Final_EQ','Final_MA','firstID','unique_to_isoform']
                    present_cols = [c for c in target_cols if c in combined_df.columns]
                    if present_cols and length_map is not None:
                        combined_df = add_quant_columns(
                            combined_df,
                            id_col=id_col,
                            count_cols=present_cols,
                            length_map=length_map,
                            eff_length_map=eff_length_map,
                            methods=self.quant
                        )
                elif self.quant and self.quant != 'none' and self.verbose:
                    print("定量模块不可用，跳过TPM/RPKM追加")
            except Exception as e:
                if self.verbose:
                    print(f"添加isoform定量列失败: {e}")

            # 保存文件
            combined_filename = self.output_dir / \
                f'{base_name}.counts_isoform_level_unique.csv'
            try:
                combined_df.to_csv(combined_filename,
                                   index=False, float_format='%.2f')
            except Exception:
                combined_df.to_csv(combined_filename,
                                   index=False)
            isoform_files['isoform'] = combined_filename

            multi_key = f'{self.isoform_prefix}multi_to_isoform'
            if multi_key in self.counts_data and self.counts_data[multi_key]:
                # 注释映射：为多转录本组合提供 geneName/symbol/description 输出
                length_map = {}
                eff_length_map = {}
                gene_name_map = {}
                symbol_map = {}
                desc_map = {}
                if self.annotation_df is not None:
                    # 修正：优先通过注释构建 isoform 层长度与有效长度映射，保证TPM分配的稳健性
                    try:
                        _len_map, _eff_map = build_length_maps(self.annotation_df, level='isoform', mode=self.length_mode_isoform)
                        length_map = _len_map or {}
                        eff_length_map = _eff_map or length_map
                    except Exception:
                        if 'txname' in self.annotation_df.columns and 'txLength' in self.annotation_df.columns:
                            length_map = dict(zip(self.annotation_df['txname'], self.annotation_df['txLength']))
                            eff_length_map = length_map
                    if 'txname' in self.annotation_df.columns and 'geneName' in self.annotation_df.columns:
                        # 修正：对 geneName 应用 sys.intern，减少重复字符串开销
                        import sys
                        gene_name_map = {str(tx): sys.intern(str(gn)) for tx, gn in zip(self.annotation_df['txname'], self.annotation_df['geneName'])}
                    if 'txname' in self.annotation_df.columns and 'symbol' in self.annotation_df.columns:
                        symbol_map = dict(zip(self.annotation_df['txname'], self.annotation_df['symbol']))
                    if 'txname' in self.annotation_df.columns and 'description' in self.annotation_df.columns:
                        desc_map = dict(zip(self.annotation_df['txname'], self.annotation_df['description']))

                # 修正：使用 isoform 层唯一计数键 unique_to_isoform + 有效长度映射计算 TPM
                tpm_values = self._calculate_tpm(
                    self.counts_data.get(f'{self.isoform_prefix}unique_to_isoform', Counter()),
                    eff_length_map
                )

                # 数值格式化：未检测到的空值标记为 0
                def _fmt_val(v):
                    if v is None:
                        return '*'
                    r = round(v)
                    if abs(v - r) < 1e-6:
                        return str(int(r))
                    return f'{v:.1f}'
                rows = []
                for ids_key, event_count in self.counts_data[multi_key].items():
                    ids = ids_key.split(',') if isinstance(ids_key, str) else list(ids_key)
                    
                    raw_vals = [_fmt_val(float(self.counts_data.get(f'{self.isoform_prefix}raw', Counter()).get(t, 0))) for t in ids]
                    firstID_vals = [_fmt_val(float(self.counts_data.get(f'{self.isoform_prefix}firstID', Counter()).get(t, 0))) for t in ids]
                    # 修正：使用 unique_to_isoform 显示 isoform 层的唯一计数
                    uniq_vals = [_fmt_val(float(self.counts_data.get(f'{self.isoform_prefix}unique_to_isoform', Counter()).get(t, 0))) for t in ids]
                    m2a_vals = [_fmt_val(float(self.counts_data.get(f'{self.isoform_prefix}multi2all', Counter()).get(t, 0))) for t in ids]

                    eq_vals = [_fmt_val(event_count / len(ids)) for _ in ids]
                    em_alloc = self._allocate_multi_reads_by_tpm_rescued(ids, tpm_values)
                    if em_alloc:
                        em_vals = [_fmt_val(event_count * em_alloc.get(t, 0.0)) for t in ids]
                    else:
                        em_vals = ['*' for _ in ids]
                    
                    final_em_vals = [_fmt_val(float(self.counts_data.get(f'{self.isoform_prefix}Final_EM', Counter()).get(t, 0))) for t in ids]
                    final_eq_vals = [_fmt_val(float(self.counts_data.get(f'{self.isoform_prefix}Final_EQ', Counter()).get(t, 0))) for t in ids]
                    def _fmt_ratio(numerator, denominator):
                        if denominator is None or denominator == 0:
                            return '*'
                        return f"{numerator/denominator:.2f}"
                    em_ratio_vals = [
                        _fmt_ratio(
                            float(self.counts_data.get(f'{self.isoform_prefix}multi_EM', Counter()).get(t, 0)),
                            float(self.counts_data.get(f'{self.isoform_prefix}Final_EM', Counter()).get(t, 0))
                        ) for t in ids
                    ]
                    eq_ratio_vals = [
                        _fmt_ratio(
                            float(self.counts_data.get(f'{self.isoform_prefix}multi_equal', Counter()).get(t, 0)),
                            float(self.counts_data.get(f'{self.isoform_prefix}Final_EQ', Counter()).get(t, 0))
                        ) for t in ids
                    ]

                    # 注释拼接：为多转录本组合提供 geneName/symbol/description（多基因用逗号分隔）
                    gene_names = ','.join([str(gene_name_map.get(t, '0')) for t in ids]) if gene_name_map else ''
                    gene_symbols = ','.join([str(symbol_map.get(t, '0')) for t in ids]) if symbol_map else ''
                    gene_descs = ','.join([str(desc_map.get(t, '0')) for t in ids]) if desc_map else ''

                    rows.append({
                        'Transcripts': ids_key if isinstance(ids_key, str) else ','.join(ids),
                        'GeneNames': gene_names,
                        'Symbols': gene_symbols if gene_symbols else None,
                        'Descriptions': gene_descs if gene_descs else None,
                        f'{self.isoform_prefix}raw': ';'.join(raw_vals),
                        f'{self.isoform_prefix}firstID': ';'.join(firstID_vals),
                        f'{self.isoform_prefix}unique_to_isoform': ';'.join(uniq_vals),
                        f'{self.isoform_prefix}multi2all': ';'.join(m2a_vals),
                        f'{self.isoform_prefix}multi_equal': ';'.join(eq_vals),
                        f'{self.isoform_prefix}multi_EM': ';'.join(em_vals),
                        f'{self.isoform_prefix}Final_EM': ';'.join(final_em_vals),
                        f'{self.isoform_prefix}Final_EQ': ';'.join(final_eq_vals),
                        f'{self.isoform_prefix}multi_EM_ratio': ';'.join(em_ratio_vals),
                        f'{self.isoform_prefix}multi_equal_ratio': ';'.join(eq_ratio_vals),
                    })
                iso_multi_df = pd.DataFrame(rows)
                iso_multi_filename = self.output_dir / f'{base_name}.counts_isoform_level_multi.csv'
                iso_multi_df.to_csv(iso_multi_filename, index=False)
                isoform_files['isoform_multi_to_isoform'] = iso_multi_filename

            if self.verbose:
                print(f"转录本水平计数文件生成完成: {len(combined_df)} 个转录本")

        except Exception as e:
            if self.verbose:
                print(f"生成转录本水平计数文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()

        return isoform_files

    def _generate_gene_level_files(self, base_name):
        """生成基因水平计数文件 - 完整修复版"""
        if self.annotation_df is None:
            if self.verbose:
                print("没有注释信息，跳过基因水平文件生成")
            return {}

        if self.verbose:
            print("生成基因水平计数文件...")

        # 调试信息：检查基因水平数据
        if self.verbose:
            print(
                f"gene_level_counts_unique_genes 存在: {hasattr(self, 'gene_level_counts_unique_genes')}")
        if hasattr(self, 'gene_level_counts_unique_genes'):
            if self.verbose:
                print(
                    f"gene_level_counts_unique_genes 类型: {type(self.gene_level_counts_unique_genes)}")
            if self.gene_level_counts_unique_genes:
                if self.verbose:
                    print(
                        f"gene_level_counts_unique_genes 键: {list(self.gene_level_counts_unique_genes.keys())}")
                for key, counter in self.gene_level_counts_unique_genes.items():
                    if self.verbose:
                        print(f"  {key}: {len(counter)} 个条目")
            else:
                if self.verbose:
                    print("gene_level_counts_unique_genes 为空")

        if self.verbose:
            print(
                f"gene_level_counts_multi_genes 存在: {hasattr(self, 'gene_level_counts_multi_genes')}")
        if hasattr(self, 'gene_level_counts_multi_genes'):
            if self.verbose:
                print(
                    f"gene_level_counts_multi_genes 类型: {type(self.gene_level_counts_multi_genes)}")
            if self.gene_level_counts_multi_genes:
                if self.verbose:
                    print(
                        f"gene_level_counts_multi_genes 键: {list(self.gene_level_counts_multi_genes.keys())}")
                for key, counter in self.gene_level_counts_multi_genes.items():
                    if self.verbose:
                        print(f"  {key}: {len(counter)} 个条目")
            else:
                if self.verbose:
                    print("gene_level_counts_multi_genes 为空")

        gene_files = {}

        try:
            # 检查是否有基因水平计数数据
            has_unique_data = False
            has_multi_data = False

            # 检查 unique genes 数据
            if (hasattr(self, 'gene_level_counts_unique_genes') and
                self.gene_level_counts_unique_genes and
                    any(len(counter) > 0 for counter in self.gene_level_counts_unique_genes.values())):
                has_unique_data = True
                if self.verbose:
                    print("发现 unique genes 数据")

            # 检查 multi genes 数据
            if (hasattr(self, 'gene_level_counts_multi_genes') and
                self.gene_level_counts_multi_genes and
                    any(len(counter) > 0 for counter in self.gene_level_counts_multi_genes.values())):
                has_multi_data = True
                if self.verbose:
                    print("发现 multi genes 数据")

            if not has_unique_data and not has_multi_data:
                if self.verbose:
                    print("没有基因水平计数数据，跳过文件生成")
                return {}

            # 生成单个基因的计数文件
            if has_unique_data:
                if self.verbose:
                    print("开始生成单个基因计数文件...")
                single_gene_data = []

                # 收集所有唯一的基因
                all_genes = set()
                for counter in self.gene_level_counts_unique_genes.values():
                    if counter:  # 确保计数器非空
                        all_genes.update(counter.keys())

                if self.verbose:
                    print(f"处理 {len(all_genes)} 个唯一基因")

                # 为每个基因构建数据行
                for gene in all_genes:
                    gene_row = {'Gene': gene}

                    # 修正：错误的条件导致计数列未写入；改为无条件追加各计数类型的值
                    for count_type, counter in self.gene_level_counts_unique_genes.items():
                        if not counter:
                            continue
                        base_count_type = count_type.replace(self.gene_prefix, '')
                        count_value = counter.get(gene, 0)
                        gene_row[base_count_type] = count_value

                    single_gene_data.append(gene_row)

                if single_gene_data:
                    # 转换为DataFrame
                    single_gene_df = pd.DataFrame(single_gene_data)
                    if self.verbose:
                        print(f"单个基因数据框形状: {single_gene_df.shape}")

                    # 添加基因注释信息
                    gene_annotation = self._get_gene_annotation_data()
                    if gene_annotation is not None:
                        if self.verbose:
                            print(f"合并基因注释信息，注释数据形状: {gene_annotation.shape}")
                        single_gene_df = single_gene_df.merge(
                            gene_annotation,
                            left_on='Gene',
                            right_on='geneName',
                            how='left'
                        )

                        # 移除重复的geneName列
                        if 'geneName' in single_gene_df.columns and 'Gene' in single_gene_df.columns:
                            single_gene_df = single_gene_df.drop(
                                'geneName', axis=1)

                    existing_cols = list(single_gene_df.columns)
                    preferred_order = [
                        'Gene',
                        'raw',
                        'multi_to_isoform',
                        # 'unique_to_isoform',
                        'unique_to_gene',
                        'firstID',
                        'multi2all',
                        'multi_equal',
                        'multi_EM',
                        'Final_EM',
                        'Final_EQ',
                        'multi_equal_ratio',
                        'multi_EM_ratio'
                    ]
                    drop_cols = [
                        'unique',
                        'multi_EM_cannot_allocate_tpm',
                        'unique_to_isoform',
                    ]
                    for dc in drop_cols:
                        if dc in single_gene_df.columns:
                            single_gene_df = single_gene_df.drop(columns=[dc])
                    # if 'unique_count' in existing_cols:
                    #     single_gene_df = single_gene_df.drop(columns=['unique_count'])
                    ordered_cols = [c for c in preferred_order if c in single_gene_df.columns] + [
                        c for c in single_gene_df.columns if c not in preferred_order
                    ]
                    single_gene_df = single_gene_df[ordered_cols]

                    # 若启用定量计算：为目标列追加 TPM/RPKM 列
                    try:
                        if self.quant and self.quant != 'none' and add_quant_columns is not None and build_length_maps is not None:
                            df_anno = self.annotation_df if self.annotation_df is not None else pd.DataFrame()
                            id_col = 'Gene'
                            # 修正：传递 gene 层长度选择模式
                            length_map, eff_length_map = build_length_maps(df_anno, level='gene', mode=self.length_mode_gene)
                            target_cols = ['Final_EM','Final_EQ','Final_MA','firstID','unique_to_isoform','unique_to_gene_and_isoform','unique_to_gene_but_not_isoform','unique_to_gene']
                            present_cols = [c for c in target_cols if c in single_gene_df.columns]
                            if present_cols and length_map is not None:
                                single_gene_df = add_quant_columns(
                                    single_gene_df,
                                    id_col=id_col,
                                    count_cols=present_cols,
                                    length_map=length_map,
                                    eff_length_map=eff_length_map,
                                    methods=self.quant
                                )
                        elif self.quant and self.quant != 'none' and self.verbose:
                            print("定量模块不可用，跳过TPM/RPKM追加")
                    except Exception as e:
                        if self.verbose:
                            print(f"添加gene定量列失败: {e}")

                    gene_filename = self.output_dir / \
                        f'{base_name}.counts_gene_level_unique.csv'
                    def _fmt_int_or_float(col):
                        if pd.api.types.is_integer_dtype(single_gene_df[col]):
                            return None
                        return '%.2f'
                    float_fmt = None
                    # 保持整数列无小数，其他列两位小数
                    try:
                        float_fmt = '%.2f'
                        single_gene_df.to_csv(
                            gene_filename, index=False, float_format=float_fmt)
                    except Exception:
                        single_gene_df.to_csv(
                            gene_filename, index=False)
                    gene_files['gene'] = gene_filename
                    if self.verbose:
                        print(f"单个基因计数文件生成完成: {len(single_gene_df)} 个基因")
                else:
                    if self.verbose:
                        print("没有单个基因数据可生成文件")

            # 生成多基因组合的计数文件
            if has_multi_data:
                if self.verbose:
                    print("开始生成多基因组合计数文件...")
                multi_genes_data = []

                # 预先缓存长度与TPM视图，减少循环中的字典/数据框访问
                gene_unique_counter = self.gene_level_counts_unique_genes.get(f'{self.gene_prefix}unique_to_gene', Counter())
                # 构建 gene -> 有效长度 映射（修正：采用 build_length_maps 并对无效值回退）
                gene_lengths_map = {}
                gene_eff_lengths_map = {}
                try:
                    _len_map, _eff_map = build_length_maps(self.annotation_df, level='gene', mode=self.length_mode_gene)
                    gene_lengths_map = _len_map or {}
                    gene_eff_lengths_map = _eff_map or gene_lengths_map
                except Exception:
                    # 回退：从可用列直接聚合长度，并用于有效长度
                    length_map_col = None
                    if 'geneEffectiveLength' in self.annotation_df.columns:
                        length_map_col = 'geneEffectiveLength'
                    elif 'genelongesttxLength' in self.annotation_df.columns:
                        length_map_col = 'genelongesttxLength'
                    elif 'txLength' in self.annotation_df.columns:
                        length_map_col = 'txLength'
                    if length_map_col and 'geneName' in self.annotation_df.columns:
                        gene_lengths_map = dict(self.annotation_df.groupby('geneName')[length_map_col].max())
                        gene_eff_lengths_map = gene_lengths_map
                # 预先计算 TPMS（基于unique + 有效长度）
                gene_tpm_map = self._calculate_tpm(gene_unique_counter, gene_eff_lengths_map)
                # 本地引用计数器，减少属性查找与哈希查找次数
                gene_multi_counter = self.gene_level_counts_multi_genes.get(f'{self.gene_prefix}multi_to_gene', Counter())
                gene_m2a_counter = self.gene_level_counts_unique_genes.get(f'{self.gene_prefix}multi2all', Counter())
                gene_uniq_counter = gene_unique_counter
                gene_final_em_counter = self.gene_level_counts_unique_genes.get(f'{self.gene_prefix}Final_EM', Counter())
                gene_final_eq_counter = self.gene_level_counts_unique_genes.get(f'{self.gene_prefix}Final_EQ', Counter())

                # 收集所有多基因组合（过滤掉单基因条目，确保 gene multi 文件不包含 unique 到单基因的记录）
                all_multi_combinations = set()
                for counter in self.gene_level_counts_multi_genes.values():
                    if counter:  # 确保计数器非空
                        for k in counter.keys():
                            if (isinstance(k, str) and ',' in k) or (isinstance(k, tuple) and len(k) > 1):
                                all_multi_combinations.add(k)

                if self.verbose:
                    print(f"处理 {len(all_multi_combinations)} 个多基因组合")

                for gene_combo in all_multi_combinations:
                    # 修正：兼容 tuple 键，导出时统一格式化为逗号分隔字符串
                    genes_list = gene_combo.split(',') if isinstance(gene_combo, str) else list(gene_combo)
                    combo_row = {'Gene_Combination': gene_combo if isinstance(gene_combo, str) else ','.join(genes_list)}

                    # 收集该组合在所有计数类型中的值
                    for count_type, counter in self.gene_level_counts_multi_genes.items():
                        if counter:  # 确保计数器非空
                            # 修复：移除恒为False的条件，确保多基因组合的各计数列写入
                            base_count_type = count_type.replace(self.gene_prefix, '')
                            count_value = counter.get(gene_combo, 0)
                            combo_row[f'{base_count_type}'] = count_value

                    genes = genes_list
                    def _fmt_val(v):
                        if v is None:
                            return '*'
                        r = round(v)
                        if abs(v - r) < 1e-6:
                            return str(int(r))
                        return f'{v:.1f}'
                    event_count = gene_multi_counter.get(gene_combo, 0)
                    eq_vals = [_fmt_val(event_count / len(genes)) for _ in genes]
                    # 使用预计算的TPM视图进行分配
                    em_alloc = self._allocate_multi_reads_by_tpm_rescued(genes, gene_tpm_map)
                    if em_alloc:
                        em_vals = [_fmt_val(event_count * em_alloc.get(g, 0.0)) for g in genes]
                        combo_row['multi_EM_cannot_allocate_tpm_gene'] = 0
                    else:
                        em_vals = ['*' for _ in genes]
                        combo_row['multi_EM_cannot_allocate_tpm_gene'] = event_count
                    # 修正：gene multi 文件不再展示 multi2all（该指标在基因组合维度语义不清晰，避免误导）
                    # m2a_vals = [_fmt_val(float(gene_m2a_counter.get(g, 0))) for g in genes]
                    uniq_vals = [_fmt_val(float(gene_uniq_counter.get(g, 0))) for g in genes]
                    final_em_vals = [_fmt_val(float(gene_final_em_counter.get(g, 0))) for g in genes]
                    final_eq_vals = [_fmt_val(float(gene_final_eq_counter.get(g, 0))) for g in genes]
                    combo_row['multi_EM'] = ';'.join(em_vals)
                    combo_row['multi_equal'] = ';'.join(eq_vals)
                    # 修正：移除 multi2all 列
                    # combo_row['multi2all'] = ';'.join(m2a_vals)
                    # 修正：重命名 unique 列为 unique_to_gene，更清晰
                    combo_row['unique_to_gene'] = ';'.join(uniq_vals)
                    combo_row['Final_EM'] = ';'.join(final_em_vals)
                    combo_row['Final_EQ'] = ';'.join(final_eq_vals)
                    
                    multi_genes_data.append(combo_row)

                if multi_genes_data:
                    multi_genes_df = pd.DataFrame(multi_genes_data)
                    
                    multi_genes_filename = self.output_dir / \
                        f'{base_name}.counts_gene_level_multi.csv'
                    
                    try:
                        multi_genes_df.to_csv(multi_genes_filename, index=False, float_format='%.2f')
                        gene_files['multi_genes'] = multi_genes_filename
                    except PermissionError:
                        fallback_dir = self.output_dir / 'tmp_multi'
                        fallback_dir.mkdir(parents=True, exist_ok=True)
                        fallback_path = fallback_dir / f'{base_name}.counts_gene_level_multi.csv'
                        multi_genes_df.to_csv(fallback_path, index=False, float_format='%.2f')
                        gene_files['multi_genes'] = fallback_path
                    if self.verbose:
                        print(f"多基因组合计数文件生成完成: {len(multi_genes_df)} 个组合")
                else:
                    if self.verbose:
                        print("没有多基因组合数据可生成文件")

                if self.export_format in ['rsem', 'all']:
                    rsem_file = self._write_rsem_gene_results(base_name)
                    if rsem_file:
                        gene_files['rsem_genes'] = rsem_file

        except Exception as e:
            if self.verbose:
                print(f"生成基因水平计数文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()

        return gene_files

    def _write_rsem_gene_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.gene_level_counts_unique_genes.get(f"{self.gene_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        length_col = None
        if 'genelongesttxLength' in df_anno.columns:
            length_col = 'genelongesttxLength'
        elif 'txLength' in df_anno.columns:
            length_col = 'txLength'
        eff_len_col = 'geneEffectiveLength' if 'geneEffectiveLength' in df_anno.columns else length_col
        gene_len_map = {}
        gene_eff_map = {}
        if 'geneName' in df_anno.columns and length_col:
            gene_len_map = dict(df_anno.groupby('geneName')[length_col].max())
        if 'geneName' in df_anno.columns and eff_len_col:
            gene_eff_map = dict(df_anno.groupby('geneName')[eff_len_col].max())
        rows = []
        for gene, count in counter.items():
            length = gene_len_map.get(gene, 0)
            eff_len = gene_eff_map.get(gene, length)
            rows.append((gene, length, eff_len, round(float(count), 1)))
        out_df = pd.DataFrame(rows, columns=['gene_id', 'length', 'effective_length', 'expected_count'])
        rsem_path = self.output_dir / (f'{base_name}.rsem.genes.{ctype}.results' if count_type is not None else f'{base_name}.rsem.genes.results')
        out_df.to_csv(rsem_path, sep='\t', index=False)
        return rsem_path

    def _write_rsem_isoform_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.counts_data.get(f"{self.isoform_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        name_col = 'txname' if 'txname' in df_anno.columns else 'transcript_id'
        len_col = 'txLength' if 'txLength' in df_anno.columns else 'length'
        # 修正：isoform 有效长度列命名为 isoformEffectiveLength，若不存在则回退到总长度列
        eff_len_col = 'isoformEffectiveLength' if 'isoformEffectiveLength' in df_anno.columns else len_col
        length_map = {}
        eff_length_map = {}
        if name_col in df_anno.columns and len_col in df_anno.columns:
            length_map = dict(zip(df_anno[name_col], df_anno[len_col]))
        if name_col in df_anno.columns and eff_len_col in df_anno.columns:
            eff_length_map = dict(zip(df_anno[name_col], df_anno[eff_len_col]))
        rows = []
        for tid, count in counter.items():
            length = length_map.get(tid, 0)
            eff_len_raw = eff_length_map.get(tid, length)
            # 修正：当有效长度缺失或<=0时，回退到总长度，避免输出0长度
            eff_len = eff_len_raw if (eff_len_raw is not None and not pd.isna(eff_len_raw) and float(eff_len_raw) > 0) else length
            rows.append((tid, length, eff_len, round(float(count), 1)))
        out_df = pd.DataFrame(rows, columns=['transcript_id', 'length', 'effective_length', 'expected_count'])
        rsem_path = self.output_dir / (f'{base_name}.rsem.isoforms.{ctype}.results' if count_type is not None else f'{base_name}.rsem.isoforms.results')
        out_df.to_csv(rsem_path, sep='\t', index=False)
        return rsem_path

    def _write_salmon_isoform_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.counts_data.get(f"{self.isoform_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        name_col = 'txname' if 'txname' in df_anno.columns else 'transcript_id'
        len_col = 'txLength' if 'txLength' in df_anno.columns else 'length'
        eff_len_col = 'txEffectiveLength' if 'txEffectiveLength' in df_anno.columns else len_col
        length_map = {}
        eff_length_map = {}
        if name_col in df_anno.columns and len_col in df_anno.columns:
            length_map = dict(zip(df_anno[name_col], df_anno[len_col]))
        if name_col in df_anno.columns and eff_len_col in df_anno.columns:
            eff_length_map = dict(zip(df_anno[name_col], df_anno[eff_len_col]))
        # 计算TPM基于Final_EM（修正：优先使用有效长度映射，避免0长度导致TPM为0）
        tpm_map = self._calculate_tpm(counter, eff_length_map or length_map)
        rows = []
        for tid, count in counter.items():
            _len_val = length_map.get(tid, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = eff_length_map.get(tid, length)
            # 数值处理：整数不带小数，浮点保留两位
            cval = float(count)
            if abs(cval - round(cval)) < 1e-6:
                num_reads = int(round(cval))
            else:
                num_reads = round(cval, 2)
            tpm = round(float(tpm_map.get(tid, 0.0)), 2)
            rows.append((tid, length, eff_len, tpm, num_reads))
        out_df = pd.DataFrame(rows, columns=['Name', 'Length', 'EffectiveLength', 'TPM', 'NumReads'])
        out_path = self.output_dir / (f'{base_name}.salmon.{ctype}.quant.sf' if count_type is not None else f'{base_name}.salmon.quant.sf')
        out_df.to_csv(out_path, index=False)
        return out_path

    def _write_salmon_gene_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.gene_level_counts_unique_genes.get(f"{self.gene_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        name_col = 'geneName'
        len_col = 'genelongesttxLength' if 'genelongesttxLength' in df_anno.columns else ('txLength' if 'txLength' in df_anno.columns else None)
        eff_len_col = 'geneEffectiveLength' if 'geneEffectiveLength' in df_anno.columns else len_col
        gene_len_map = {}
        gene_eff_map = {}
        if len_col and 'geneName' in df_anno.columns:
            gene_len_map = dict(df_anno.groupby('geneName')[len_col].max())
        if eff_len_col and 'geneName' in df_anno.columns:
            gene_eff_map = dict(df_anno.groupby('geneName')[eff_len_col].max())
        tpm_map = self._calculate_tpm(counter, gene_len_map)
        rows = []
        for gid, count in counter.items():
            _len_val = gene_len_map.get(gid, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = gene_eff_map.get(gid, length)
            cval = float(count)
            num_reads = int(round(cval)) if abs(cval - round(cval)) < 1e-6 else round(cval, 2)
            tpm = round(float(tpm_map.get(gid, 0.0)), 2)
            rows.append((gid, length, eff_len, tpm, num_reads))
        out_df = pd.DataFrame(rows, columns=['Name', 'Length', 'EffectiveLength', 'TPM', 'NumReads'])
        out_path = self.output_dir / (f'{base_name}.salmon.genes.{ctype}.sf' if count_type is not None else f'{base_name}.salmon.genes.sf')
        out_df.to_csv(out_path, index=False)
        return out_path

    def _write_kallisto_isoform_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.counts_data.get(f"{self.isoform_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        name_col = 'txname' if 'txname' in df_anno.columns else 'transcript_id'
        len_col = 'txLength' if 'txLength' in df_anno.columns else 'length'
        eff_len_col = 'txEffectiveLength' if 'txEffectiveLength' in df_anno.columns else len_col
        length_map = {}
        eff_length_map = {}
        if name_col in df_anno.columns and len_col in df_anno.columns:
            length_map = dict(zip(df_anno[name_col], df_anno[len_col]))
        if name_col in df_anno.columns and eff_len_col in df_anno.columns:
            eff_length_map = dict(zip(df_anno[name_col], df_anno[eff_len_col]))
        tpm_map = self._calculate_tpm(counter, length_map)
        rows = []
        for tid, count in counter.items():
            _len_val = length_map.get(tid, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = eff_length_map.get(tid, length)
            cval = float(count)
            if abs(cval - round(cval)) < 1e-6:
                est_counts = int(round(cval))
            else:
                est_counts = round(cval, 2)
            tpm = round(float(tpm_map.get(tid, 0.0)), 2)
            rows.append((tid, length, eff_len, est_counts, tpm))
        out_df = pd.DataFrame(rows, columns=['target_id', 'length', 'eff_length', 'est_counts', 'tpm'])
        out_path = self.output_dir / (f'{base_name}.kallisto.isoforms.{ctype}.abundance.tsv' if count_type is not None else f'{base_name}.kallisto.isoforms.abundance.tsv')
        out_df.to_csv(out_path, index=False)
        return out_path

    def _write_kallisto_gene_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.gene_level_counts_unique_genes.get(f"{self.gene_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        len_col = 'genelongesttxLength' if 'genelongesttxLength' in df_anno.columns else ('txLength' if 'txLength' in df_anno.columns else None)
        eff_len_col = 'geneEffectiveLength' if 'geneEffectiveLength' in df_anno.columns else len_col
        gene_len_map = {}
        gene_eff_map = {}
        if len_col and 'geneName' in df_anno.columns:
            gene_len_map = dict(df_anno.groupby('geneName')[len_col].max())
        if eff_len_col and 'geneName' in df_anno.columns:
            gene_eff_map = dict(df_anno.groupby('geneName')[eff_len_col].max())
        tpm_map = self._calculate_tpm(counter, gene_len_map)
        rows = []
        for gid, count in counter.items():
            _len_val = gene_len_map.get(gid, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = gene_eff_map.get(gid, length)
            cval = float(count)
            est_counts = int(round(cval)) if abs(cval - round(cval)) < 1e-6 else round(cval, 2)
            tpm = round(float(tpm_map.get(gid, 0.0)), 2)
            rows.append((gid, length, eff_len, est_counts, tpm))
        out_df = pd.DataFrame(rows, columns=['target_id', 'length', 'eff_length', 'est_counts', 'tpm'])
        out_path = self.output_dir / (f'{base_name}.kallisto.genes.{ctype}.abundance.tsv' if count_type is not None else f'{base_name}.kallisto.genes.abundance.tsv')
        out_df.to_csv(out_path, index=False)
        return out_path

    def _write_featurecounts_gene_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.gene_level_counts_unique_genes.get(f"{self.gene_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        len_col = None
        if 'genelongesttxLength' in df_anno.columns:
            len_col = 'genelongesttxLength'
        elif 'txLength' in df_anno.columns:
            len_col = 'txLength'
        eff_len_col = 'geneEffectiveLength' if 'geneEffectiveLength' in df_anno.columns else len_col
        gene_len_map = {}
        gene_eff_map = {}
        if 'geneName' in df_anno.columns and len_col:
            gene_len_map = dict(df_anno.groupby('geneName')[len_col].max())
        if 'geneName' in df_anno.columns and eff_len_col:
            gene_eff_map = dict(df_anno.groupby('geneName')[eff_len_col].max())
        rows = []
        for gene, count in counter.items():
            _len_val = gene_len_map.get(gene, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = gene_eff_map.get(gene, length)
            cval = float(count)
            if abs(cval - round(cval)) < 1e-6:
                fc_count = int(round(cval))
            else:
                fc_count = round(cval, 2)
            rows.append((gene, length, eff_len, fc_count))
        out_df = pd.DataFrame(rows, columns=['Geneid', 'Length', 'EffectiveLength', 'Count'])
        out_path = self.output_dir / (f'{base_name}.featureCounts.genes.{ctype}.tsv' if count_type is not None else f'{base_name}.featureCounts.genes.tsv')
        out_df.to_csv(out_path, sep='\t', index=False)
        return out_path

    def _write_featurecounts_isoform_results(self, base_name, count_type=None):
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        counter = self.counts_data.get(f"{self.isoform_prefix}{ctype}", Counter())
        if not counter:
            return None
        df_anno = self.annotation_df
        name_col = 'txname' if 'txname' in df_anno.columns else 'transcript_id'
        len_col = 'txLength' if 'txLength' in df_anno.columns else 'length'
        eff_len_col = 'txEffectiveLength' if 'txEffectiveLength' in df_anno.columns else len_col
        length_map = {}
        eff_length_map = {}
        if name_col in df_anno.columns and len_col in df_anno.columns:
            length_map = dict(zip(df_anno[name_col], df_anno[len_col]))
        if name_col in df_anno.columns and eff_len_col in df_anno.columns:
            eff_length_map = dict(zip(df_anno[name_col], df_anno[eff_len_col]))
        rows = []
        for tid, count in counter.items():
            _len_val = length_map.get(tid, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = eff_length_map.get(tid, length)
            cval = float(count)
            fc_count = int(round(cval)) if abs(cval - round(cval)) < 1e-6 else round(cval, 2)
            rows.append((tid, length, eff_len, fc_count))
        out_df = pd.DataFrame(rows, columns=['Geneid', 'Length', 'EffectiveLength', 'Count'])
        out_path = self.output_dir / (f'{base_name}.featureCounts.isoforms.{ctype}.tsv' if count_type is not None else f'{base_name}.featureCounts.isoforms.tsv')
        out_df.to_csv(out_path, sep='\t', index=False)
        return out_path

    # 修正：抽象通用定量写出函数，统一处理 RSEM/Salmon/Kallisto/featureCounts 四种工具的 isoform/gene 两个层级
    from typing import Optional
    def _write_quant_file(self, tool: str, level: str, base_name: str, count_type: Optional[str] = None):
        """
        通用写出函数，减少重复代码：
        - tool: 'rsem' | 'salmon' | 'kallisto' | 'featureCounts'
        - level: 'isoform' | 'gene'
        - count_type: 计数类型，默认使用 self.export_count_type
        返回：生成的文件路径或 None
        """
        if self.annotation_df is None:
            return None
        ctype = count_type if count_type is not None else (self.export_count_type if self.export_count_type != 'all' else 'Final_EM')
        prefix = self.isoform_prefix if level == 'isoform' else self.gene_prefix
        counter = (self.counts_data if level == 'isoform' else self.gene_level_counts_unique_genes).get(f"{prefix}{ctype}", Counter())
        if not counter:
            return None

        df_anno = self.annotation_df
        # 选择ID与长度列
        if level == 'isoform':
            id_key = 'txname' if 'txname' in df_anno.columns else 'transcript_id'
            len_key = 'txLength' if 'txLength' in df_anno.columns else 'length'
            eff_key = 'txEffectiveLength' if 'txEffectiveLength' in df_anno.columns else len_key
            if id_key in df_anno.columns and len_key in df_anno.columns:
                length_map = dict(zip(df_anno[id_key], df_anno[len_key]))
            else:
                length_map = {}
            if id_key in df_anno.columns and eff_key in df_anno.columns:
                eff_length_map = dict(zip(df_anno[id_key], df_anno[eff_key]))
            else:
                eff_length_map = {}
        else:
            id_key = 'geneName'
            # gene 长度优先使用 genelongesttxLength → 回退到 txLength（按基因聚合）
            len_key = 'genelongesttxLength' if 'genelongesttxLength' in df_anno.columns else ('txLength' if 'txLength' in df_anno.columns else None)
            eff_key = 'geneEffectiveLength' if 'geneEffectiveLength' in df_anno.columns else len_key
            if len_key and id_key in df_anno.columns:
                if len_key == 'txLength':
                    length_map = dict(df_anno.groupby(id_key)[len_key].max())
                else:
                    length_map = dict(df_anno.groupby(id_key)[len_key].max())
            else:
                length_map = {}
            if eff_key and id_key in df_anno.columns:
                gene_eff_map = dict(df_anno.groupby(id_key)[eff_key].max())
            else:
                gene_eff_map = {}

        # 构建输出行
        rows = []
        if tool == 'rsem':
            # RSEM 不包含 TPM 输出，写 expected_count + 长度
            for ident, count in counter.items():
                if level == 'isoform':
                    length = length_map.get(ident, 0)
                    eff_len = eff_length_map.get(ident, length)
                    rows.append((ident, length, eff_len, round(float(count), 1)))
                    columns = ['transcript_id', 'length', 'effective_length', 'expected_count']
                    out_name = f"{base_name}.rsem.isoforms.{ctype}.results" if count_type is not None else f"{base_name}.rsem.isoforms.results"
                else:
                    length = length_map.get(ident, 0)
                    eff_len = gene_eff_map.get(ident, length)
                    rows.append((ident, length, eff_len, round(float(count), 1)))
                    columns = ['gene_id', 'length', 'effective_length', 'expected_count']
                    out_name = f"{base_name}.rsem.genes.{ctype}.results" if count_type is not None else f"{base_name}.rsem.genes.results"
            out_df = pd.DataFrame(rows, columns=columns)
            out_path = self.output_dir / out_name
            out_df.to_csv(out_path, sep='\t', index=False)
            return out_path

        # 计算 TPM（基于当前 counter 与长度）
        tpm_map = self._calculate_tpm(counter, length_map if level == 'isoform' else length_map)
        for ident, count in counter.items():
            _len_val = length_map.get(ident, 0)
            length = int(_len_val) if (isinstance(_len_val, (int, float)) and not pd.isna(_len_val)) else 0
            eff_len = (eff_length_map.get(ident, length) if level == 'isoform' else gene_eff_map.get(ident, length))
            cval = float(count)
            # 按工具选择列与数值格式
            if tool == 'salmon':
                num_reads = int(round(cval)) if abs(cval - round(cval)) < 1e-6 else round(cval, 2)
                tpm = round(float(tpm_map.get(ident, 0.0)), 2)
                rows.append((ident, length, eff_len, tpm, num_reads))
                columns = ['Name', 'Length', 'EffectiveLength', 'TPM', 'NumReads']
            elif tool == 'kallisto':
                est_counts = int(round(cval)) if abs(cval - round(cval)) < 1e-6 else round(cval, 2)
                tpm = round(float(tpm_map.get(ident, 0.0)), 2)
                if level == 'isoform':
                    columns = ['target_id', 'length', 'eff_length', 'est_counts', 'tpm']
                else:
                    columns = ['target_id', 'length', 'eff_length', 'est_counts', 'tpm']
                rows.append((ident, length, eff_len, est_counts, tpm))
            elif tool == 'featureCounts':
                fc_count = int(round(cval)) if abs(cval - round(cval)) < 1e-6 else round(cval, 2)
                if level == 'isoform':
                    columns = ['Geneid', 'Length', 'EffectiveLength', 'Count']
                else:
                    columns = ['Geneid', 'Length', 'EffectiveLength', 'Count']
                rows.append((ident, length, eff_len, fc_count))
            else:
                return None

        out_df = pd.DataFrame(rows, columns=columns)
        if tool == 'salmon':
            out_name = (f"{base_name}.salmon.{ctype}.quant.sf" if level == 'isoform' else f"{base_name}.salmon.genes.{ctype}.sf") if count_type is not None else (f"{base_name}.salmon.quant.sf" if level == 'isoform' else f"{base_name}.salmon.genes.sf")
        elif tool == 'kallisto':
            out_name = (f"{base_name}.kallisto.isoforms.{ctype}.abundance.tsv" if level == 'isoform' else f"{base_name}.kallisto.genes.{ctype}.abundance.tsv") if count_type is not None else (f"{base_name}.kallisto.isoforms.abundance.tsv" if level == 'isoform' else f"{base_name}.kallisto.genes.abundance.tsv")
        elif tool == 'featureCounts':
            out_name = (f"{base_name}.featureCounts.isoforms.{ctype}.tsv" if level == 'isoform' else f"{base_name}.featureCounts.genes.{ctype}.tsv") if count_type is not None else (f"{base_name}.featureCounts.isoforms.tsv" if level == 'isoform' else f"{base_name}.featureCounts.genes.tsv")
        else:
            return None

        out_path = self.output_dir / out_name
        # salmon/kallisto 输出 CSV，featureCounts 使用 TSV
        if tool == 'featureCounts':
            out_df.to_csv(out_path, sep='\t', index=False)
        else:
            out_df.to_csv(out_path, index=False)
        return out_path

    def _get_gene_annotation_data(self):
        """获取基因注释数据"""
        if self.annotation_df is None:
            return None

        # 检查可用的注释列
        available_columns = self.annotation_df.columns.tolist()

        # 选择基因相关的注释列
        selected_cols = ['geneName']

        # 添加其他可能有用的列
        optional_cols = {
            'symbol': ['symbol', 'genename', 'gene_name'],
            'genelongesttxLength': ['genelongesttxLength', 'genelonesttxlength', 'txLength'],
            'genelongestcdsLength': ['genelongestcdsLength', 'genelongestcdslength', 'cdsLength']
        }

        for target_col, source_cols in optional_cols.items():
            for col in source_cols:
                if col in available_columns:
                    selected_cols.append(col)
                    break

        if self.verbose:
            self.console.print(f"使用的基因注释列: {selected_cols}")

        # 获取去重的基因注释
        gene_annotation = self.annotation_df[selected_cols].drop_duplicates(subset=['geneName'])

        # 重命名列以保持一致性
        rename_map = {}
        if 'genename' in gene_annotation.columns:
            rename_map['genename'] = 'symbol'
        if 'genelonesttxlength' in gene_annotation.columns:
            rename_map['genelonesttxlength'] = 'genelongesttxLength'
        if 'genelongestcdslength' in gene_annotation.columns:
            rename_map['genelongestcdslength'] = 'genelongestcdsLength'

        if rename_map:
            gene_annotation = gene_annotation.rename(columns=rename_map)

        return gene_annotation

    # def _generate_multi_mapping_file(self, base_name):
    #     """生成多映射信息文件"""
    #     if not self.multi_mapping_info:
    #         return None

    #     # 创建多映射信息数据框
    #     multi_data = []
    #     for transcript_ids, read_names in self.multi_mapping_info.items():
    #         multi_data.append({
    #             'transcript_ids': transcript_ids,
    #             'read_count': len(read_names),
    #             'read_names': ';'.join(read_names)
    #         })

    #     multi_df = pd.DataFrame(multi_data)
    #     multi_filename = self.output_dir / \
    #         f'{base_name}_multi_mapping_info.csv'
    #     multi_df.to_csv(multi_filename, index=False)

    #     return multi_filename

    def generate_count_files(self):
        """
        生成isoform 和 gene level 计数文件
        """
        if self.output_filename:
            base_name = Path(self.output_filename).stem
        else:
            base_name = self.input_file.stem
        if base_name.endswith('.counts'):
            base_name = base_name[:-7]

        count_files = {}

        # 输出格式控制
        fmt = getattr(self, 'export_format', 'rsem')
        fmt_set = set()
        if fmt == 'all':
            fmt_set = {'rsem', 'salmon', 'kallisto', 'featureCounts'}
        elif fmt == 'single':
            fmt_set = {'rsem'}
        elif fmt == 'multi':
            fmt_set = {'salmon', 'kallisto', 'featureCounts'}
        else:
            fmt_set = {fmt}

        # 生成转录本水平计数文件
        if self.level in ['isoform', 'both']:
            try:
                isoform_files = self._generate_isoform_level_files(base_name)
                count_files.update(isoform_files)
                if self.verbose:
                    self.console.print("isoform 水平计数文件生成完成")
            except Exception as e:
                if self.verbose:
                    self.console.print(f"转录本水平计数文件生成失败: {e}")

        # 生成基因水平计数文件
        if self.annotation_df is not None and self.level in ['gene', 'both']:
            try:
                # 修复：检查基因水平计数数据是否存在?有可能生成了isoform，但是gff文件没有给合适，所以匹配不到gene名，导致这个情况。暂时先这样吧，，一般不会碰到。所以判断是否具有基因level数据的那部分先删除
                # has_gene_data = True
                # 修复：检查正确的基因水平数据位置multi_EM_cannot_allocate_tpm_count
                has_gene_data = False

                # 检查 gene_level_counts_unique_genes
                if hasattr(self, 'gene_level_counts_unique_genes') and self.gene_level_counts_unique_genes:
                    for counter in self.gene_level_counts_unique_genes.values():
                        if counter and len(counter) > 0:  # 检查计数器是否非空
                            has_gene_data = True
                            break

                # 检查 gene_level_counts_multi_genes
                if not has_gene_data and hasattr(self, 'gene_level_counts_multi_genes') and self.gene_level_counts_multi_genes:
                    for counter in self.gene_level_counts_multi_genes.values():
                        if counter and len(counter) > 0:  # 检查计数器是否非空
                            has_gene_data = True
                            break

                if self.verbose:
                    self.console.print(f'has_gene_data: {has_gene_data}')
                if has_gene_data:
                    gene_files = self._generate_gene_level_files(base_name)
                    count_files.update(gene_files)
                    if self.verbose:
                        self.console.print("基因水平计数文件生成完成")
                else:
                    if self.verbose:
                        self.console.print("没有基因水平计数数据，跳过基因水平文件生成")
            except Exception as e:
                self.console.print(f"基因水平计数文件生成失败: {e}")

        # 生成RSEM兼容输出(iso/gene按level) —— 使用通用写出函数
        if 'rsem' in fmt_set:
            if getattr(self, 'export_count_type', 'Final_EM') == 'all':
                ctype_list = ['raw','unique','firstID','Final_EM','Final_EQ','Final_MA']
                for ct in ctype_list:
                    if self.level in ['isoform','both']:
                        rsem_iso_file = self._write_quant_file('rsem', 'isoform', base_name, ct)
                        if rsem_iso_file:
                            count_files[f'rsem_isoform_{ct}'] = rsem_iso_file
                    if self.level in ['gene','both']:
                        rsem_gene_file = self._write_quant_file('rsem', 'gene', base_name, ct)
                        if rsem_gene_file:
                            count_files[f'rsem_gene_{ct}'] = rsem_gene_file
            else:
                if self.level in ['isoform','both']:
                    rsem_iso_file = self._write_quant_file('rsem', 'isoform', base_name)
                    if rsem_iso_file:
                        count_files['rsem_isoform'] = rsem_iso_file
                if self.level in ['gene','both']:
                    rsem_gene_file = self._write_quant_file('rsem', 'gene', base_name)
                    if rsem_gene_file:
                        count_files['rsem_gene'] = rsem_gene_file

        # 生成 Salmon/Kallisto/featureCounts 兼容输出（统一调用）
        if 'salmon' in fmt_set:
            if getattr(self, 'export_count_type', 'Final_EM') == 'all':
                ctype_list = ['raw','unique','firstID','Final_EM','Final_EQ','Final_MA']
                for ct in ctype_list:
                    if self.level in ['isoform','both']:
                        salmon_iso_file = self._write_quant_file('salmon', 'isoform', base_name, ct)
                        if salmon_iso_file:
                            count_files[f'salmon_isoform_{ct}'] = salmon_iso_file
                    if self.level in ['gene','both']:
                        salmon_gene_file = self._write_quant_file('salmon', 'gene', base_name, ct)
                        if salmon_gene_file:
                            count_files[f'salmon_gene_{ct}'] = salmon_gene_file
            else:
                if self.level in ['isoform','both']:
                    salmon_iso_file = self._write_quant_file('salmon', 'isoform', base_name)
                    if salmon_iso_file:
                        count_files['salmon_isoform'] = salmon_iso_file
                if self.level in ['gene','both']:
                    salmon_gene_file = self._write_quant_file('salmon', 'gene', base_name)
                    if salmon_gene_file:
                        count_files['salmon_gene'] = salmon_gene_file

        if 'kallisto' in fmt_set:
            if getattr(self, 'export_count_type', 'Final_EM') == 'all':
                ctype_list = ['raw','unique','firstID','Final_EM','Final_EQ','Final_MA']
                for ct in ctype_list:
                    if self.level in ['isoform','both']:
                        kallisto_iso_file = self._write_quant_file('kallisto', 'isoform', base_name, ct)
                        if kallisto_iso_file:
                            count_files[f'kallisto_isoform_{ct}'] = kallisto_iso_file
                    if self.level in ['gene','both']:
                        kallisto_gene_file = self._write_quant_file('kallisto', 'gene', base_name, ct)
                        if kallisto_gene_file:
                            count_files[f'kallisto_gene_{ct}'] = kallisto_gene_file
            else:
                if self.level in ['isoform','both']:
                    kallisto_iso_file = self._write_quant_file('kallisto', 'isoform', base_name)
                    if kallisto_iso_file:
                        count_files['kallisto_isoform'] = kallisto_iso_file
                if self.level in ['gene','both']:
                    kallisto_gene_file = self._write_quant_file('kallisto', 'gene', base_name)
                    if kallisto_gene_file:
                        count_files['kallisto_gene'] = kallisto_gene_file

        if 'featureCounts' in fmt_set:
            if getattr(self, 'export_count_type', 'Final_EM') == 'all':
                ctype_list = ['raw','unique','firstID','Final_EM','Final_EQ','Final_MA']
                for ct in ctype_list:
                    if self.level in ['isoform','both']:
                        fc_iso_file = self._write_quant_file('featureCounts', 'isoform', base_name, ct)
                        if fc_iso_file:
                            count_files[f'featureCounts_isoform_{ct}'] = fc_iso_file
                    if self.level in ['gene','both']:
                        fc_gene_file = self._write_quant_file('featureCounts', 'gene', base_name, ct)
                        if fc_gene_file:
                            count_files[f'featureCounts_gene_{ct}'] = fc_gene_file
            else:
                if self.level in ['isoform','both']:
                    fc_iso_file = self._write_quant_file('featureCounts', 'isoform', base_name)
                    if fc_iso_file:
                        count_files['featureCounts_isoform'] = fc_iso_file
                if self.level in ['gene','both']:
                    fc_gene_file = self._write_quant_file('featureCounts', 'gene', base_name)
                    if fc_gene_file:
                        count_files['featureCounts_gene'] = fc_gene_file

        return count_files
##########################################################################################
    # 主运行进程

    def run(self):
        """运行完整的计数流程"""
        if self.verbose:
            self.console.print("=" * 60)
            self.console.print("fansetools count - Starting processing")
            self.console.print("=" * 60)

        if self.level in ['gene', 'both'] and self.annotation_df is None:
            self.console.print("注意：生成 gene level counts 需要提供 --gxf gff/gtf 文件")
            return {}

        # 1. 解析fanse3文件并直接获得计数
        # counts_data, total_count = self.parse_fanse_file()
        counts_data, total_count = self.parse_fanse_file_optimized_final()

        # 2. 生成isoform水平计数
        self.generate_isoform_level_counts(counts_data, total_count)

        # 3. 生成基因水平计数,基因水平聚合（如果有注释）
        if self.annotation_df is not None and self.level in ['gene', 'both']:
            gene_level_counts_unique_genes, gene_level_counts_multi_genes = self.aggregate_gene_level_counts()

            # 修复：确保正确存储到实例变量
            self.gene_level_counts_unique_genes = gene_level_counts_unique_genes
            self.gene_level_counts_multi_genes = gene_level_counts_multi_genes

            if self.verbose and self.gene_level_counts_unique_genes:
                self.console.print(
                    f"Gene level aggregation completed: {len(self.gene_level_counts_unique_genes)} unique-gene count types")
            
            if self.verbose and self.gene_level_counts_multi_genes:
                self.console.print(
                    f"Gene level aggregation completed: {len(self.gene_level_counts_multi_genes)} multi-gene count types")
        else:
            if self.verbose:
                self.console.print("No annotation provided, skipping gene level aggregation")
            self.gene_level_counts_unique_genes = {}
            self.gene_level_counts_multi_genes = {}

        # 4. 生成计数文件
        count_files = self.generate_count_files()

        # 5. 生成摘要报告
        self.generate_summary()

        if self.verbose:
            self.console.print("fansetools count - Processing completed")
            self.console.print("=" * 60)

        return count_files

    def generate_summary(self):
        """生成处理摘要"""
        summary_file = self.output_dir / f"{self.input_file.stem}_summary.txt"

        with open(summary_file, 'w') as f:
            f.write("fansetools count - Processing Summary\n")
            f.write("=" * 50 + "\n")
            f.write(f"Input file: {self.input_file}\n")
            f.write(f"Output directory: {self.output_dir}\n")
            f.write(
                f"Processing mode: {'Paired-end' if self.paired_end else 'Single-end'}\n")
            f.write(f"Level parameter: {self.level}\n")
            f.write(f"Annotation provided: {self.annotation_df is not None}\n")

            if self.annotation_df is not None:
                f.write(f"Annotation transcripts: {len(self.annotation_df)}\n")
                f.write(
                    f"Annotation genes: {self.annotation_df['geneName'].nunique()}\n")

            f.write("\nStatistics:\n")
            for stat, value in self.summary_stats.items():
                f.write(f"{stat}: {value}\n")

            f.write(f"\nMulti-mapping statistics:\n")
            iso_multi_key = f"{self.isoform_prefix}multi_to_isoform"
            if iso_multi_key in self.counts_data:
                f.write(
                    f"Isoform-level multi-mapping events: {len(self.counts_data[iso_multi_key])}\n")
                if self.counts_data[iso_multi_key]:
                    total_multi_reads = sum(self.counts_data[iso_multi_key].values())
                    avg_reads_per_event = total_multi_reads / \
                        len(self.counts_data[iso_multi_key])
                    f.write(f"Isoform-level total multi-mapped reads: {total_multi_reads}\n")
                    f.write(
                        f"Isoform-level average reads per multi-mapping event: {avg_reads_per_event:.2f}\n")
            gene_multi_key = f"{self.gene_prefix}multi_to_gene"
            # 修正：基因层面的多映射信息存储在 gene_level_counts_multi_genes 中
            if hasattr(self, 'gene_level_counts_multi_genes') and self.gene_level_counts_multi_genes and gene_multi_key in self.gene_level_counts_multi_genes:
                f.write(
                    f"Gene-level multi-mapping events: {len(self.gene_level_counts_multi_genes[gene_multi_key])}\n")
                if self.gene_level_counts_multi_genes[gene_multi_key]:
                    total_multi_reads = sum(self.gene_level_counts_multi_genes[gene_multi_key].values())
                    avg_reads_per_event = total_multi_reads / \
                        len(self.gene_level_counts_multi_genes[gene_multi_key])
                    f.write(f"Gene-level total multi-mapped reads: {total_multi_reads}\n")
                    f.write(
                        f"Gene-level average reads per multi-mapping event: {avg_reads_per_event:.2f}\n")

    def debug_gene_level_data(self):
        """调试基因水平数据"""
        if self.verbose:
            self.console.print("=== 调试基因水平数据 ===")

        # 检查实例变量
        if self.verbose:
            self.console.print(
                f"gene_level_counts_unique_genes 存在: {hasattr(self, 'gene_level_counts_unique_genes')}")
        if hasattr(self, 'gene_level_counts_unique_genes'):
            if self.verbose:
                self.console.print(f"类型: {type(self.gene_level_counts_unique_genes)}")
            if isinstance(self.gene_level_counts_unique_genes, dict):
                if self.verbose:
                    self.console.print(f"键数量: {len(self.gene_level_counts_unique_genes)}")
                for key, value in self.gene_level_counts_unique_genes.items():
                    if hasattr(value, '__len__'):
                        if self.verbose:
                            self.console.print(f"  {key}: {len(value)} 个条目")
                    else:
                        if self.verbose:
                            self.console.print(f"  {key}: {type(value)}")
            else:
                if self.verbose:
                    self.console.print(f"值: {self.gene_level_counts_unique_genes}")

        if self.verbose:
            self.console.print(
                f"gene_level_counts_multi_genes 存在: {hasattr(self, 'gene_level_counts_multi_genes')}")
        if hasattr(self, 'gene_level_counts_multi_genes'):
            if self.verbose:
                self.console.print(f"类型: {type(self.gene_level_counts_multi_genes)}")
            if isinstance(self.gene_level_counts_multi_genes, dict):
                if self.verbose:
                    self.console.print(f"键数量: {len(self.gene_level_counts_multi_genes)}")
                for key, value in self.gene_level_counts_multi_genes.items():
                    if hasattr(value, '__len__'):
                        if self.verbose:
                            self.console.print(f"  {key}: {len(value)} 个条目")
                    else:
                        if self.verbose:
                            self.console.print(f"  {key}: {type(value)}")
            else:
                if self.verbose:
                    self.console.print(f"值: {self.gene_level_counts_multi_genes}")

# %% some other function


def print_mini_fansetools():
    """
    最小的可识别版本
    https://www.ascii-art-generator.org/
    """
    # mini_art = [
    #     '''
    #     #######                                #######
    #     #         ##   #    #  ####  ######       #     ####   ####  #       ####
    #     #        #  #  ##   # #      #            #    #    # #    # #      #
    #     #####   #    # # #  #  ####  #####        #    #    # #    # #       ####
    #     #       ###### #  # #      # #            #    #    # #    # #           #
    #     #       #    # #   ## #    # #            #    #    # #    # #      #    #
    #     #       #    # #    #  ####  ######       #     ####   ####  ######  ####
    #     '''
    # ]

    mini_art = ['''
     FANSeTools Count - Summary the RNA-seq Count in various levels and methods.
     ''']

    console = Console(force_terminal=True)
    for line in mini_art:
        console.print(line, style="bold cyan")


def load_annotation_data(args):
    """加载注释数据"""
    console = Console(force_terminal=True)
    if not args.gxf:
        console.print("[bold red]错误: 需要提供 --gxf 参数[/bold red]")
        return None

    # Use PathProcessor to clean/validate the path
    processor = PathProcessor()
    try:
        gxf_files = processor.parse_input_paths(args.gxf, ['.gtf', '.gff', '.gff3', '.refflat'])
        if not gxf_files:
            console.print(f"[bold red]Error: Invalid GXF file path: {args.gxf}[/bold red]")
            return None
        # Use the first valid path (cleaned)
        args.gxf = str(gxf_files[0])
    except Exception as e:
        console.print(f"[bold red]Error parsing GXF path: {e}[/bold red]")
        return None

    if getattr(args, 'verbose', False):
        console.print(f"\nLoading annotation from {args.gxf}")

    # 检查是否存在同名的refflat文件
    refflat_file = os.path.splitext(args.gxf)[0] + ".genomic.refflat"

    if os.path.exists(refflat_file):
        if getattr(args, 'verbose', False):
            console.print(f"Found existing refflat file: {refflat_file}")
        try:
            annotation_df = read_refflat_with_commented_header(refflat_file)
            if getattr(args, 'verbose', False):
                console.print(
                    f"Successfully loaded {len(annotation_df)} transcripts from existing refflat file")
            return annotation_df
        except Exception as e:
            if getattr(args, 'verbose', False):
                console.print(f"[bold red]Error loading refflat file: {e}[/bold red]")
                console.print("Converting GXF file instead...")

    if getattr(args, 'verbose', False):
        console.print(f"No existing refflat file found at {refflat_file}")
        console.print("Converting GXF file to refflat format...")

    if args.annotation_output:
        # Generate both genomic and RNA coordinate files
        genomic_df, rna_df = convert_gxf_to_refflat(
            args.gxf, args.annotation_output, add_header=True
        )
        return genomic_df
    else:
        # Just load the data without saving
        genomic_df = load_annotation_to_dataframe(args.gxf)
        return genomic_df

# 方法1：先读取注释行获取列名，然后读取数据
def read_refflat_with_commented_header(file_path):
    """读取带有注释头部的refflat文件"""
    # 首先读取注释行获取列名
    with open(file_path, 'r') as f:
        header_line = None
        for line in f:
            if line.startswith('#'):
                header_line = line.strip()
                break

    if header_line:
        # 提取列名（去掉#和空格）
        columns = header_line[1:].strip().split('\t')
        # 读取数据，跳过注释行
        df = pd.read_csv(file_path, sep='\t', comment='#',
                         header=None, names=columns, low_memory=False, dtype={'chrom': str})
    else:
        # 如果没有注释头部，使用默认列名
        default_columns = [
            "geneName", "txname", "chrom", "strand", "txStart", "txEnd",
            "cdsStart", "cdsEnd", "exonCount", "exonStarts", "exonEnds",
            "symbol", "g_biotype", "t_biotype", "description", "protein_id",
            "txLength", "cdsLength", "utr5Length", "utr3Length",
            "genelongesttxLength", "genelongestcdsLength", "geneEffectiveLength",
            "geneNonOverlapLength", "geneReadCoveredLength"
        ]
        df = pd.read_csv(file_path, sep='\t', header=None,
                         names=default_columns, dtype={'chrom': str})

    return df

def add_count_subparser(subparsers):
    """
    添加count子命令的参数解析器。
    此函数定义了FANSe3文件计数处理的所有命令行参数，包括输入/输出、注释文件、计数水平、
    输出格式、计数类型选择、并行处理等。

    :param subparsers: 子命令解析器对象。
    :return: None
    """

    parser = subparsers.add_parser(
        'count',
        help='运行FANSe to count，输出readcount',
        formatter_class=CustomHelpFormatter,
        epilog="""
        [bold]使用示例:[/bold]
            [dim]默认isoform level[/dim]
          [bold cyan]单个文件/单端测序文件处理:[/bold cyan]
            fanse count -i sample.fanse3 -o results/ --gxf annotation.gtf

          [bold cyan]批量处理目录中所有fanse3文件:[/bold cyan]
            fanse count -i /data/*.fanse3 -o /output/ --gxf annotation.gtf

          [bold cyan]双端测序数据:[/bold cyan] [dim](不支持通配符和文件夹等用此方法处理)[/dim]
            fanse count -1 R1.fanse3 -2 R2.fanse3 -o results/ --gxf annotation.gtf

        [bold yellow]**如需要基因水平计数，需要输入gtf/gff/refflat/简单g-t对应文件，--gxf都可以解析[/bold yellow]
          [bold cyan]基因水平计数:[/bold cyan]
            fanse count -i *.fanse3 -o results/ --gxf annotation.gtf --level gene

          [bold cyan]同时输出基因和转录本水平:[/bold cyan]
            fanse count -i *.fanse3 -o results/ --gxf annotation.gtf --level both

          [bold cyan]处理中断后重新运行:[/bold cyan] [dim]（自动跳过已处理的文件）[/dim]
            fanse count -i *.fanse3 -o results/ --gxf annotation.gtf --resume

            [dim]# 指定4个并行进程，展示总体运行进度和简易进度条[/dim]
            fanse count -i "*.fanse3" -o results --gxf annotation.gtf --p 4

          [bold cyan]使用所有CPU核心并行处理:[/bold cyan]
            fanse count -i *.fanse3 -o results --gxf annotation.gtf -p 0

        [bold]基因定量归一化长度指标说明 (--len):[/bold]
          - [green]geneEffectiveLength[/green]: 所有转录本外显子并集的非重叠长度，用于更稳健的TPM归一化
          - [green]genelongesttxLength[/green]: 每个基因的最长转录本长度，常作为简化替代
          - [green]txLength[/green]: 基于转录本长度的回退选项（按基因取最大）
          - [green]geneNonOverlapLength[/green]: 与 geneEffectiveLength 一致，显式提供非重叠外显子长度
          - [green]geneReadCoveredLength[/green]: 依据reads覆盖区间的长度（无覆盖信息时退化为有效长度）
          - [green]other[/green]: 自定义列名（若不存在自动回退至有效长度）
                """
    )

    parser.add_argument('-i', '--input', required=False,
                        help='输入FANSe3文件/目录/通配符（支持批量处理）')
    parser.add_argument('-1', '--read1', required=False,
                        help='双端测序 Read1 输入文件')
    parser.add_argument('-2', '--read2', required=False,
                        help='双端测序 Read2 输入文件,暂时不可用，输入无效')
    # parser.add_argument('-r', '--paired-end', required=False,
    #                     help='已废弃：双端 Read2 输入文件（请使用 -2/--read2）')
    parser.add_argument('-o', '--output', required=False,
                        help='Output directory,输出路径（文件或目录，自动检测）')

    parser.add_argument('-g', '--gxf', required=False,
                        help='Input GXF file (GTF or GFF3),if not provided, just give out isoform level readcounts')
    parser.add_argument('-a', '--annotation-output',
                        help='Output refFlat file prefix (optional)')

    parser.add_argument('-l' ,'--level', choices=['gene', 'isoform', 'both'], default='gene',
                        help='RNA seq Counting level')
    parser.add_argument('-f', '--format', choices=['rsem', 'salmon', 'kallisto', 'featureCounts', 'all', 'single', 'multi'], default='salmon',
                        help='输出常见定量格式选择，默认salmon')
    parser.add_argument('-c', '--count-type', choices=['raw','unique','firstID','Final_EM','Final_EQ','Final_MA','all'], default='Final_EM',
                        help="定量文件中用作计数与TPM计算的计数类型；选择'all'时为每种计数类型分别导出定量文件")

    # 长度指标选择：兼容旧参数 + 新增分别指定 isoform/gene
    parser.add_argument('--len', dest='len',
                        choices=['geneEffectiveLength', 'genelongesttxLength', 'txLength',
                                 'geneNonOverlapLength', 'geneReadCoveredLength', 'other'],
                        default='genelongesttxLength',
                        help='兼容旧参数：仅用于 gene 层；默认 genelongesttxLength')
    parser.add_argument('--len-isoform', dest='len_isoform',
                        choices=['txLength', 'cdsLength', 'isoformEffectiveLength'],
                        default='txLength',
                        help='isoform 层的长度选择：txLength/cdsLength/isoformEffectiveLength')
    parser.add_argument('--len-gene', dest='len_gene',
                        choices=['geneEffectiveLength', 'genelongesttxLength', 'txLength', 'genelongestcdsLength'],
                        default='genelongesttxLength',
                        help='gene 层的长度选择：geneEffectiveLength/genelongesttxLength/txLength/genelongestcdsLength')

    parser.add_argument('-p', '--processes',  type=int, default=1,
                        help='并行任务数 (默认=1: CPU核心数, 1=串行)')
    parser.add_argument('-v', '--verbose', action='store_true', help='显示详细信息')

    parser.add_argument( '--resume', required=False, action='store_true',
                        help='可从上次运行断掉的地方自动开始，自动检测文件夹中是否有输入文件对应的结果文件，有则跳过不计入，统计没有结果文件的输入文件并开始')

    # 新增：解析批处理大小，用于优化解析阶段性能与内存占用的权衡
    parser.add_argument('--batch-size', dest='batch_size', type=int, default=None,
                        help='解析阶段的批处理大小（默认: 2000000）。数值越大，函数调用与哈希查找开销越少，但占用内存更高；请根据机器内存与文件规模调整。')

    # 新增：定量方法选择，用于在唯一结果文件中追加表达量列
    parser.add_argument('-q','--quant', choices=['none','tpm','rpkm','both'], default='none',
                        help='在唯一文件中追加表达量列：none(不追加)/tpm/RPKM/both')
    # 新增：解析引擎选择（auto/python/rust）
    parser.add_argument('--engine', choices=['auto','python','rust'], default='auto',
                        help='解析引擎：auto(优先使用Rust，失败回退Python)/python(仅Python解析)/rust(强制Rust，失败回退Python)')

    # 根据是否并行选择执行函数
    def count_main_wrapper(args):
        if getattr(args, 'processes', None) != 1:
            return count_main_parallel(args)
        else:
            return count_main(args)

    # 设置处理函数，而不是直接解析参数
    parser.set_defaults(func=count_main)


def main():
    """主函数 - 用于直接运行此脚本"""
    parser = argparse.ArgumentParser(
        description='fansetools Count - Process RNA seq read counting for fanse3 files in isoform  and gene level'
    )

    # 添加子命令
    subparsers = parser.add_subparsers(
        dest='command', help='Available commands')
    add_count_subparser(subparsers)

    args = parser.parse_args()

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

    def test_gene_level_counting():
        """测试基因水平计数功能"""
        import tempfile
        import shutil

        # 创建临时目录
        temp_dir = tempfile.mkdtemp()
        print(f"测试目录: {temp_dir}")

        try:
            # 测试文件路径
            # fanse_file = r"\\fs2\D\DATA\Zhaojing\3.fanse3_result\old_s14\26.9311-Endosperm_RNC_R1_trimmed.fanse3"
            fanse_file = r'\\fs2\D\DATA\ZhaoJing\0.test\test1.fanse3'
            refflat_file = r'\\fs2\D\DATA\Zhaojing\202209数据汇缴\0.Ref_seqs\20251024Oryza_sativa.IRGSP_9311.rna.refflat'
            # 创建模拟的注释数据（用于测试）

            # annotation_df = load_annotation_data(gtf_file )
            annotation_df = read_refflat_with_commented_header(refflat_file)

            # 创建计数器实例
            counter = FanseCounter(
                input_file=fanse_file,
                output_dir=temp_dir,
                level='both',
                # minreads=0,
                gxf_file=None,
                annotation_df=annotation_df
            )

            print("开始解析fanse文件...")
            # counter.parse_fanse_file()
            # counter.generate_isoform_level_counts()
            counts_data, total_count = counter.parse_fanse_file_optimized_final()
            # counter.generate_isoform_level_counts(counts_data, total_count)  # 传递参数

            print(f"解析完成，共 {total_count} 条记录")
            print(f"计数数据包含 {len(counts_data)} 种计数类型")

            print("\n开始生成isoform水平计数...")
            # 正确调用：传递参数
            counter.generate_isoform_level_counts(counts_data, total_count)

            print("开始基因水平聚合...")
            counter.gene_level_counts_unique_genes, counter.gene_level_counts_multi_genes = counter.aggregate_gene_level_counts()

            # 在 generate_count_files 方法开始处添加
            counter.debug_gene_level_data()
            print("\n生成计数文件...")
            count_files = counter.generate_count_files()
            print(f"生成的文件: {list(count_files.keys())}")

            print("\n解析统计:")
            print(f"总reads数: {counter.summary_stats['total_reads']}")
            print(f"唯一映射reads: {counter.summary_stats['unique_mapped']}")
            print(f"多映射reads: {counter.summary_stats['multi_mapped']} PEI25k ")

            print("\n转录本水平计数统计:")
            for count_type, counter_data in counter.counts_data.items():
                print(f"{count_type}: {len(counter_data)} 组转录本ID")
            #     if len(counter_data) > 0:
            #         top5 = counter_data.most_common(5)
            #         print(f"  前5个: {top5}")
###################################################################################################
            print("\n进行基因水平聚合...")
            counter.annotation_df = annotation_df
            gene_counts = counter.aggregate_gene_level_counts()

            if gene_counts:
                print("\n基因水平计数统计:")
                for count_type, gene_counter in gene_counts.items():
                    print(f"{count_type}: {len(gene_counter)} 个基因")
                    # if len(gene_counter) > 0:
                    #     top5 = gene_counter.most_common(5)
                    #     print(f"  前5个基因: {top5}")

            print("\n生成计数文件...")
            count_files = counter.generate_count_files()
            print(f"生成的文件: {list(count_files.keys())}")

            # 验证文件内容
            for file_type, file_path in count_files.items():
                if file_path.exists():
                    df = pd.read_csv(file_path)
                    print(f"\n{file_type} 文件信息:")
                    print(f"  行数: {len(df)}")
                    print(f"  列数: {len(df.columns)}")
                    if len(df) > 0:
                        print(f"  前3行:")
                        print(df.head(3))

            # 检查多映射信息
            if counter.multi_mapping_info:
                print(f"\n多映射事件数量: {len(counter.multi_mapping_info)}")
                multi_events = list(counter.multi_mapping_info.items())[:3]
                for transcript_ids, read_names in multi_events:
                    print(
                        f"  转录本: {transcript_ids}, reads数: {len(read_names)}")

            print("\n测试完成!")

        except Exception as e:
            print(f"测试过程中出现错误: {str(e)}")
            import traceback
            traceback.print_exc()

        finally:
            # 清理临时目录
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"\n清理测试目录: {temp_dir}")
    # （修正位置）-q/--quant 参数已移动到 add_count_subparser 内部统一解析
