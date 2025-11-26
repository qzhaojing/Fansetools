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

import os
import sys
import argparse
import pandas as pd
import glob
from collections import Counter, defaultdict
from tqdm import tqdm
import time
from pathlib import Path
# import defaultdict

# 导入新的路径处理器
from fansetools.utils.path_utils import PathProcessor
# 导入新的fanse_parser
from fansetools.parser import fanse_parser, FANSeRecord, fanse_parser_high_performance
from fansetools.gxf2refflat_plus import convert_gxf_to_refflat, load_annotation_to_dataframe


# 在您的FanseCounter类中添加并行处理方法
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm.contrib.concurrent import process_map

# %% ParallelFanseCounter


class ParallelFanseCounter:
    """并行处理多个fanse3文件的计数器"""

    def __init__(self, max_workers=None):
        self.max_workers = max_workers or min(mp.cpu_count(), 8)  # 限制最大进程数
        print(f"初始化并行处理器: {self.max_workers} 个进程")

    def process_files_parallel(self, file_list, output_base_dir, gxf_file=None, level='gene', paired_end=None, annotation_df=None):
        """并行处理多个文件 - 修复版本"""
        print(f" 开始并行处理 {len(file_list)} 个文件，使用 {self.max_workers} 个进程")

        # 准备任务参数
        tasks = []
        for input_file in file_list:
            # 为每个文件创建独立的输出目录
            file_stem = input_file.stem
            output_dir = Path(output_base_dir) / file_stem
            output_dir.mkdir(parents=True, exist_ok=True)

            task = {
                'input_file': str(input_file),
                'output_dir': str(output_dir),
                'gxf_file': gxf_file,
                'level': level,
                'paired_end': paired_end,
                'file_stem': file_stem  # 用于进度显示
            }
            tasks.append(task)

        # 使用进程池并行处理
        results = []
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_task = {}
            for task in tasks:
                future = executor.submit(
                    self._process_single_file, task, annotation_df)
                future_to_task[future] = task

            # 使用tqdm显示总体进度
            with tqdm(total=len(tasks), desc="总体进度", position=0) as pbar:
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        results.append((task['input_file'], True, result))
                        pbar.set_description(f" 完成: {task['file_stem']}")
                    except Exception as e:
                        results.append((task['input_file'], False, str(e)))
                        pbar.set_description(f" 失败: {task['file_stem']}")
                    finally:
                        pbar.update(1)

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
                paired_end=task['paired_end'],
                annotation_df=annotation_df  # 传递已加载的注释数据
            )

            # 运行计数处理
            result = counter.run()
            return f"成功处理 {task['file_stem']}"

        except Exception as e:
            raise Exception(f"处理文件 {task['input_file']} 失败: {str(e)}")


def count_main_parallel(args):
    """支持并行的主函数"""
    print_mini_fansetools()
    processor = PathProcessor()

    try:
        # 1. 解析输入文件
        input_files = processor.parse_input_paths(
            args.input, ['.fanse', '.fanse3', '.fanse3.gz', '.fanse.gz'])
        if not input_files:
            print("错误: 未找到有效的输入文件")
            return

        print(f"找到 {len(input_files)} 个输入文件")

        # 2. 加载注释文件（主进程加载，然后传递给工作进程）
        annotation_df = None
        if args.gxf:
            annotation_df = load_annotation_data(args)
            if annotation_df is None:
                print("错误: 无法加载注释数据")
                return
            print(f"已加载注释数据: {len(annotation_df)} 个转录本")
        else:
            print("警告: 未提供注释文件，将只生成isoform水平计数")

        # 3. 设置输出目录
        output_dir = Path(
            args.output) if args.output else Path.cwd() / "fansetools_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"输出目录: {output_dir}")

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
                    individual_output_dir / f"{file_stem}_isoform_level.counts.csv")
            if args.level in ['gene', 'both'] and args.gxf:
                output_files_to_check.append(
                    individual_output_dir / f"{file_stem}_gene_level.counts.csv")

            # 检查文件是否存在
            all_files_exist = all(f.exists() for f in output_files_to_check)

            if args.resume and all_files_exist:
                print(f"  跳过: {input_file.name} - 输出文件已存在")
                skipped_files += 1
            else:
                files_to_process.append(input_file)

        if not files_to_process:
            print("所有文件均已处理完成")
            return

        print(
            f"断点续传: 跳过 {skipped_files} 个文件，剩余 {len(files_to_process)} 个文件待处理")

        # 5. 并行处理
        max_workers = args.processes if hasattr(
            args, 'processes') and args.processes > 1 else min(mp.cpu_count(), len(files_to_process))

        if max_workers == 1:
            print("使用串行处理模式")
            return count_main_serial(args)  # 回退到串行处理

        parallel_counter = ParallelFanseCounter(max_workers=max_workers)

        print("🚀 开始并行处理...")
        print("=" * 60)

        start_time = time.time()
        results = parallel_counter.process_files_parallel(
            file_list=files_to_process,
            output_base_dir=output_dir,
            gxf_file=args.gxf,
            level=args.level,
            paired_end=args.paired_end,
            annotation_df=annotation_df
        )

        duration = time.time() - start_time

        # 6. 输出结果摘要
        print("\n" + "=" * 60)
        print(" 处理结果摘要")
        print("=" * 60)

        success_count = sum(1 for _, success, _ in results if success)
        failed_count = len(results) - success_count

        print(f" 成功: {success_count} 个文件")
        print(f" 失败: {failed_count} 个文件")
        print(f" 总耗时: {duration:.2f} 秒")

        if failed_count > 0:
            print("\n失败详情:")
            for input_file, success, result in results:
                if not success:
                    print(f"  - {Path(input_file).name}: {result}")

        print(f"\n 处理完成! 结果保存在: {output_dir}")

    except Exception as e:
        print(f"错误: {str(e)}")
        import traceback
        traceback.print_exc()


def count_main_serial(args):
    """串行处理版本（原有的count_main函数）"""
    print("使用单任务处理模式...")
    processor = PathProcessor()

    try:
        # 原有的串行处理逻辑...
        input_files = processor.parse_input_paths(
            args.input, ['.fanse', '.fanse3', '.fanse3.gz', '.fanse.gz'])
        if not input_files:
            print("错误: 未找到有效的输入文件")
            return

        # 加载注释文件
        annotation_df = None
        if args.gxf:
            annotation_df = load_annotation_data(args)
            if annotation_df is None:
                print("错误: 无法加载注释数据")
                return
        else:
            print("警告: 未提供注释文件，将只生成isoform水平计数")

        # 生成输出映射
        output_map = processor.generate_output_mapping(
            input_files, args.output, '.counts.csv')

        # 断点续传检查
        skipped_files = 0
        if args.resume:
            print("启用断点续传模式，检查已存在的输出文件...")
            files_to_process = {}

            for input_file, output_file in output_map.items():
                output_dir = Path(output_file).parent
                input_stem = input_file.stem

                output_files_to_check = []
                if args.level in ['isoform', 'both']:
                    output_files_to_check.append(
                        output_dir / f"{input_stem}_isoform_level.counts.csv")
                if args.level in ['gene', 'both']:
                    output_files_to_check.append(
                        output_dir / f"{input_stem}_gene_level.counts.csv")
                    output_files_to_check.append(
                        output_dir / f"{input_stem}_multi_genes_level.counts.csv")

                all_files_exist = all(f.exists()
                                      for f in output_files_to_check)
                if all_files_exist:
                    print(f"  跳过: {input_file.name} - 输出文件已存在")
                    skipped_files += 1
                else:
                    files_to_process[input_file] = output_file

            output_map = files_to_process
            print(f"断点续传: 跳过 {skipped_files} 个文件，剩余 {len(output_map)} 个文件待处理")

            if not output_map:
                print("所有文件均已处理完成")
                return

        # 串行处理每个文件
        for i, (input_file, output_file) in enumerate(output_map.items(), 1):
            print(
                f"\n[{i + skipped_files}/{len(input_files)}] 处理: {input_file.name}")
            print(f"  输出: {output_file}")

            try:
                counter = FanseCounter(
                    input_file=str(input_file),
                    output_dir=str(output_file.parent),
                    output_filename=output_file.name,
                    gxf_file=args.gxf,
                    level=args.level if annotation_df is not None else 'isoform',
                    paired_end=args.paired_end,
                    annotation_df=annotation_df,
                )
                count_files = counter.run()
                print(" 完成")
            except Exception as e:
                print(f" 处理失败: {str(e)}")

        print(f"\n处理完成: 总共 {len(input_files)} 个文件")

    except Exception as e:
        print(f"错误: {str(e)}")


def count_main(args):
    """主入口函数，根据参数选择并行或串行"""
    if hasattr(args, 'processes') and args.processes != 1:
        return count_main_parallel(args)
    else:
        return count_main_serial(args)


class FanseCounter:
    """fanse3文件计数处理器"""

    def __init__(self, input_file, output_dir, level='isoform',
                 # minreads=0,
                 rpkm=0,
                 gxf_file=None,
                 paired_end=None,
                 output_filename=None,
                 annotation_df=None):

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
        self.paired_end = paired_end
        self.output_filename = output_filename  # 新增：支持自定义输出文件名
        self.annotation_df = annotation_df  # 新增：注释数据框

        # # 存储计数结果
        # self.counts_data = {}
        # self.summary_stats = {}
        # self.multi_mapping_info = defaultdict(list)  # 存储多映射信息
        # 存储计数结果
        self.counts_data = {
            # isoform水平计数
            f'{self.isoform_prefix}raw': Counter(),
            f'{self.isoform_prefix}unique': Counter(),
            f'{self.isoform_prefix}multi': Counter(),
            f'{self.isoform_prefix}firstID': Counter(),
            f'{self.isoform_prefix}multi2all': Counter(),
            f'{self.isoform_prefix}multi_equal': Counter(),
            f'{self.isoform_prefix}multi_EM': Counter(),
            f'{self.isoform_prefix}Final_em': Counter(),
            f'{self.isoform_prefix}Final_eq': Counter(),

            # gene水平计数
            f'{self.gene_prefix}unique': Counter(),
            f'{self.gene_prefix}multi_equal': Counter(),
            f'{self.gene_prefix}multi_EM': Counter(),
            f'{self.gene_prefix}Final_em': Counter(),
            f'{self.gene_prefix}Final_eq': Counter(),
        }
        self.summary_stats = {}
        self.multi_mapping_info = defaultdict(list)

    def judge_sequence_mode(self):
        """判断测序模式（单端/双端）"""
        if self.paired_end and os.path.isfile(self.paired_end):
            print('Pair-End mode detected.')
            return True
        else:
            print('Single-End mode detected.')
            return False


# %% parser

    # def parse_fanse_file(self):  # 局部优化变量后的，测试20251114
    #     """
    #     专门负责解析fanse3文件，直接进行基本计数
    #     """
    #     # 选择优化版本
    #     if self.input_file.stat().st_size > 1024 * 1024 * 1024:  # 大于1024 MB
    #         fanse_parser = fanse_parser_high_performance
    #     else:
    #         fanse_parser = fanse_parser

    #     print(f'Parsing {self.input_file.name}，预定义加速版')
    #     start_time = time.time()

    #     # 初始化所有计数器 - 使用带前缀的键
    #     counts_data = {
    #         f'{self.isoform_prefix}raw': Counter(),
    #         f'{self.isoform_prefix}multi': Counter(),
    #         f'{self.isoform_prefix}unique': Counter(),
    #         f'{self.isoform_prefix}firstID': Counter(),
    #         f'{self.isoform_prefix}multi2all': Counter(),
    #         f'{self.isoform_prefix}multi_equal': Counter(),
    #         f'{self.isoform_prefix}multi_EM': Counter(),
    #         f'{self.isoform_prefix}multi_EM_cannot_allocate_tpm': Counter(),
    #         f'{self.isoform_prefix}Final_em': Counter(),
    #         f'{self.isoform_prefix}Final_eq': Counter(),
    #     }
    #     # 使用局部变量加速
    #     raw, multi, unique, firstID, multi2all = (
    #         counts_data[f'{self.isoform_prefix}raw'],
    #         counts_data[f'{self.isoform_prefix}multi'],
    #         counts_data[f'{self.isoform_prefix}unique'],
    #         counts_data[f'{self.isoform_prefix}firstID'],
    #         counts_data[f'{self.isoform_prefix}multi2all']
    #     )

    #     total_count = 0

    #     files_to_process = [self.input_file]
    #     if self.paired_end:
    #         files_to_process.append(Path(self.paired_end))

    #     for fanse_file in files_to_process:
    #         if not fanse_file.exists():
    #             continue

    #         try:
    #             sample_size = 100000  # 采样数目，用来估算总reads数
    #             estimated_records = self.calculate_file_record_estimate(
    #                 fanse_file, sample_size)

    #             with tqdm(total=estimated_records, unit='reads', mininterval=5, unit_scale=True) as pbar:
    #                 for record in fanse_parser(str(fanse_file)):
    #                     if record.ref_names:
    #                         transcript_ids = record.ref_names
    #                         is_multi = record.is_multi

    #                         # 直接更新计数器 - 使用预先定义的带前缀的键加速
    #                         raw_id = transcript_ids[0] if len(
    #                             transcript_ids) == 1 else ','.join(transcript_ids)
    #                         raw[raw_id] += 1
    #                         firstID[transcript_ids[0]] += 1

    #                         if is_multi:
    #                             multi[raw_id] += 1
    #                             for tid in transcript_ids:
    #                                 multi2all[tid] += 1
    #                         else:
    #                             unique[raw_id] += 1

    #                         total_count += 1
    #                         pbar.update(1)

    #         except Exception as e:
    #             print(f"Error parsing file {fanse_file}: {str(e)}")
    #             continue

    #     parsing_time = time.time() - start_time
    #     print(
    #         f"Parsing completed in {parsing_time:.2f} seconds, {total_count} records")

    #     return counts_data, total_count

    def parse_fanse_file_optimized_final(self, position=0):
        """综合优化版本"""

        # # 添加并行检查（新增代码）
        # if hasattr(self, 'num_processes') and self.num_processes > 1:
        #     file_size_mb = self.input_file.stat().st_size / (1024 * 1024)
        #     if file_size_mb > 500:  # 大于500MB使用并行
        #         return self._parse_parallel(self.num_processes)

        # 选择优化版本
        if self.input_file.stat().st_size > 1024 * 1024 * 1024:  # 大于1024 MB
            fanse_parser_selected = fanse_parser_high_performance
        else:
            fanse_parser_selected = fanse_parser

        print(f'Parsing {self.input_file.name}')
        start_time = time.time()

        # 预初始化数据结构
        counts_data = {
            f'{self.isoform_prefix}raw': Counter(),
            f'{self.isoform_prefix}multi': Counter(),
            f'{self.isoform_prefix}unique': Counter(),
            f'{self.isoform_prefix}firstID': Counter(),
            f'{self.isoform_prefix}multi2all': Counter(),
            f'{self.isoform_prefix}multi_equal': Counter(),
            f'{self.isoform_prefix}multi_EM': Counter(),
            f'{self.isoform_prefix}multi_EM_cannot_allocate_tpm': Counter(),
            f'{self.isoform_prefix}Final_em': Counter(),
            f'{self.isoform_prefix}Final_eq': Counter(),
        }

        total_count = 0
        batch_size = 600000
        # update_interval = 10000

        # 使用局部变量加速
        raw, multi, unique, firstID, multi2all = (
            counts_data[f'{self.isoform_prefix}raw'],
            counts_data[f'{self.isoform_prefix}multi'],
            counts_data[f'{self.isoform_prefix}unique'],
            counts_data[f'{self.isoform_prefix}firstID'],
            counts_data[f'{self.isoform_prefix}multi2all']
        )

        for position, fanse_file in enumerate([self.input_file] + ([Path(self.paired_end)] if self.paired_end else [])):
            if not fanse_file.exists():
                continue

            try:
                batch = []
                # last_update = 0

                # file_size = fanse_file.stat().st_size
                # estimated_records = max(1, file_size // 527)
                # 智能估算记录数
                sample_size = 100000  # 采样数目，用来估算总reads数
                estimated_records = self.calculate_file_record_estimate(
                    fanse_file, sample_size)

                with tqdm(total=estimated_records, unit='reads', mininterval=5, unit_scale=True, position=position, leave=False) as pbar:
                    # 进度条更新频率控制
                    update_interval = 1000
                    update_counter = 0

                    for i, record in enumerate(fanse_parser_selected(str(fanse_file))):
                        if record.ref_names:
                            total_count += 1

                            # 批量处理
                            batch.append(record)
                            if len(batch) >= batch_size:
                                self._fast_batch_process(
                                    batch, raw, multi, unique, firstID, multi2all)
                                batch = []

                            # 智能更新
                            update_counter += 1
                            if update_counter >= update_interval:
                                pbar.update(update_counter)
                                update_counter = 0
                            # 减少进度更新频率
                            # if i - last_update >= update_interval:
                            #     print(f"Processed {i} records...", end='\r')
                            #     last_update = i
                        else:
                            update_counter += 1

                    # 更新剩余的进度
                    if update_counter > 0:
                        pbar.update(update_counter)
                        # pbar.update(1)
                    # 处理剩余批次
                    if batch:
                        self._fast_batch_process(
                            batch, raw, multi, unique, firstID, multi2all)

            except Exception as e:
                print(f"Error: {e}")
                continue

        duration = time.time() - start_time
        print(
            f" Completed: {total_count} records in {duration:.2f}s ({total_count/duration:.0f} rec/sec)")

        return counts_data, total_count

    def _fast_batch_process(self, batch, raw, multi, unique, firstID, multi2all):
        """快速批量处理"""
        for record in batch:
            ids = record.ref_names
            is_multi = record.is_multi

            # 最小化字符串操作
            first_id = ids[0]
            raw_id = first_id if len(ids) == 1 else ','.join(ids)

            raw[raw_id] += 1
            firstID[first_id] += 1

            if is_multi:
                multi[raw_id] += 1
                # 使用集合操作优化多ID处理
                for tid in ids:
                    multi2all[tid] += 1
            else:
                unique[raw_id] += 1

    def calculate_average_record_size(self, file_path, sample_size=100000):
        """
        通过采样计算fanse3文件的平均记录大小

        参数:
            file_path: 文件路径
            sample_size: 采样记录数（默认10000条）

        返回:
            平均每条记录的字节数
        """
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
                print(f"采样完成: {record_count} 条记录，平均大小: {avg_size:.1f} 字节")
                return avg_size
            else:
                print("警告: 无法采样记录，使用默认值527")
                return 527

        except Exception as e:
            print(f"采样失败: {e}，使用默认值527")
            return 527

    def calculate_file_record_estimate(self, file_path, sample_size=100000):
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
        if file_size < 100 * 1024 * 1024:  # 小于10MB的文件
            print("小文件，直接计数...")
            try:
                record_count = sum(
                    1 for _ in fanse_parser(str(file_path)))
                print(f"直接计数完成: {record_count} 条记录")
                return record_count
            except:
                pass

        # 对于大文件，使用采样估算
        avg_size = self.calculate_average_record_size(
            file_path, sample_size)*0.8  # 经验减去50字节，人为增大一点估算的reads总数，反而比较符合实际
        estimated_records = max(1, int(file_size / avg_size))

        print(f"文件大小: {file_size} 字节")
        print(f"平均记录大小: {avg_size:.1f} 字节")
        print(f"估计Fanse记录数: {estimated_records} 条")

        return estimated_records


# %% generate counts

    def _rescue_multi_mappings_by_tpm(self, counts_data, prefix=None, length_dict=None, annotation_df=None):
        """
        通用多映射处理 - 
        1.支持isoform和gene level前缀
        2.支持记录multi mapped reads的分配比例
        参数:
            counts_data: 计数数据字典
            prefix: 前缀类型 ('isoform_' 或 'gene_')，如果为None则使用实例的isoform_prefix
            length_dict: ID到长度的映射字典，如果为None则从annotation_df获取
            annotation_df: 注释数据框，如果为None则使用实例的annotation_df
        返回:
            分配比例字典，格式: {多映射ID: {分配ID1: 比例1, 分配ID2: 比例2, ...}}
        """
        if prefix is None:
            prefix = self.isoform_prefix  # 默认使用isoform前缀

        print(f"开始高级多映射分析 (前缀: {prefix})...")

        # 检查是否有multi数据
        multi_key = f'{prefix}multi'
        if multi_key not in counts_data or not counts_data[multi_key]:
            print(f"没有{prefix}多映射数据，跳过高级分析")
            return

        # 获取长度信息
        if length_dict is None:
            length_dict = {}
            current_annotation_df = annotation_df if annotation_df is not None else self.annotation_df

            if current_annotation_df is not None:
                # 根据前缀选择不同的列名映射
                if prefix == self.isoform_prefix:
                    # isoform水平：使用转录本长度
                    id_col = 'txname' if 'txname' in current_annotation_df.columns else 'transcript_id'
                    length_col = 'txLength' if 'txLength' in current_annotation_df.columns else 'length'
                else:
                    # gene水平：使用基因长度
                    id_col = 'geneName' if 'geneName' in current_annotation_df.columns else 'gene_id'
                    # 对于基因水平，使用最长转录本长度
                    length_col = 'genelongesttxLength' if 'genelongesttxLength' in current_annotation_df.columns else 'txLength'

                if id_col in current_annotation_df.columns and length_col in current_annotation_df.columns:
                    if prefix == self.gene_prefix:
                        # 对于基因水平，需要计算每个基因的最长转录本长度
                        gene_lengths = current_annotation_df.groupby(id_col)[
                            length_col].max()
                        length_dict = gene_lengths.to_dict()
                    else:
                        # isoform水平直接映射
                        length_dict = dict(
                            zip(current_annotation_df[id_col], current_annotation_df[length_col]))

                    print(f"加载了 {len(length_dict)} 个{prefix}ID的长度信息")

        # 通过unique部分计算TPM
        unique_key = f'{prefix}unique'
        tpm_values = self._calculate_tpm(
            counts_data.get(unique_key, Counter()), length_dict)
        print(f"计算了 {len(tpm_values)} 个具有unique reads {prefix}ID的TPM值")

        # 初始化计数器
        multi_equal_counter = Counter()
        multi_em_counter = Counter()
        multi_em_cannot_allocate_tpm_counter = Counter()

        allocation_ratios = {}

        processed_events = 0
        total_events = len(counts_data[multi_key])

        print(f"开始处理 {total_events} 个{prefix}多映射事件...")

        for ids_str, event_count in counts_data[multi_key].items():
            try:
                # 分割ID（可能是转录本ID或基因ID）
                ids = ids_str.split(',')

                # multi_equal: 平均分配
                equal_share_per_read = 1.0 / len(ids)
                for id_val in ids:
                    multi_equal_counter[id_val] += event_count * \
                        equal_share_per_read
                equal_allocation = {id_val: equal_share_per_read for id_val in ids}
                allocation_ratios[ids_str] = {}
                allocation_ratios[ids_str]['equal'] = equal_allocation
                allocation_ratios[ids_str]['multi2all'] = {id_val: 1.0 for id_val in ids}

                # multi_EM: 按TPM比例分配
                allocation = self._allocate_multi_reads_by_tpm_rescued(
                    ids, tpm_values)
                if allocation:
                    for id_val, share_ratio in allocation.items():
                        multi_em_counter[id_val] += event_count * share_ratio
                    allocation_ratios[ids_str]['EM'] = allocation
                else:
                    # 无法分配的情况
                    multi_em_cannot_allocate_tpm_counter[ids_str] += event_count
                    allocation_ratios[ids_str]['EM'] = None

                processed_events += 1
                if processed_events % 10000 == 0:
                    print(
                        f"已处理 {processed_events}/{total_events} 个{prefix}多映射事件")

            except Exception as e:
                print(f"处理{prefix}多映射事件 {ids_str} 时出错: {str(e)}")
                continue

        # 更新计数器
        counts_data[f'{prefix}multi_equal'] = multi_equal_counter
        counts_data[f'{prefix}multi_EM'] = multi_em_counter
        counts_data[f'{prefix}multi_EM_cannot_allocate_tpm'] = multi_em_cannot_allocate_tpm_counter

        counts_data[f'{prefix}allocation_ratios'] = allocation_ratios

        print(f"{prefix}高级多映射分析完成：")
        print(f"  - {prefix}multi_equal: {len(multi_equal_counter)} 个ID")
        print(f"  - {prefix}multi_EM: {len(multi_em_counter)} 个ID")
        print(f"  - 无法分配TPM的事件: {len(multi_em_cannot_allocate_tpm_counter)} 个")
        print(f"  - 记录分配比例: {len(allocation_ratios)} 个事件")

        return allocation_ratios

    def _rescue_multi_mappings_by_tpm_isoform(self, counts_data):
        """isoform水平的多映射处理（向后兼容）"""
        return self._rescue_multi_mappings_by_tpm(counts_data, prefix=self.isoform_prefix)

    def _rescue_multi_mappings_by_tpm_gene(self, counts_data):
        """gene水平的多映射处理"""
        return self._rescue_multi_mappings_by_tpm(counts_data, prefix=self.gene_prefix)

    def _calculate_tpm(self, unique_counts, transcript_lengths):
        '''
        """计算每个基因的TPM值"""
        TPM是一种常用的基因表达标准化方法，能够消除基因长度和测序深度的影响。
        正确的计算步骤分为两步：
        - 第一步是RPK标准化，用基因的原始reads数除以基因长度(以千碱基为单位)；
        - 第二步是总和标准化，将所有基因的RPK值相加，然后用每个基因的RPK值除以这个总和再乘以一百万。
        '''
        if not unique_counts or not transcript_lengths:
            return {}

        # 计算RPK (Reads Per Kilobase)
        rpk_values = {}
        total_rpk = 0

        for transcript, count in unique_counts.items():
            if transcript in transcript_lengths and transcript_lengths[transcript] > 0:
                length_kb = transcript_lengths[transcript] / 1000
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

        # 过滤掉没有TPM值的转录本
        valid_transcripts = [
            tid for tid in transcript_ids if tid in tpm_values and tpm_values[tid] > 0]

        if not valid_transcripts:
            # 回退到平均分配，，，这个有点不太合适，可以放在另一个表格multi_EM_cannot_allocate_tpm里，暂时不参与分配  20251111
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

    def generate_isoform_level_counts(self, counts_data, total_count):
        """
        根据解析的计数数据生成isoform水平的各种计数
        """
        print("Generating isoform level counts...")
        start_time = time.time()

        # 第二阶段：高级多映射计数rescue multi mapped reads
        # 修复：使用带前缀的键
        if counts_data[f'{self.isoform_prefix}multi']:
            print("Starting advanced multi-mapping analysis...")
            # self._rescue_multi_mappings_by_tpm(counts_data)
            self._rescue_multi_mappings_by_tpm(
                counts_data, prefix=self.isoform_prefix)
            print("Advanced multi-mapping analysis completed.")

        # 第三阶段:计算正确的counts，合并raw和multi_em，以及multi_equal 的counts
        print("Starting third stage: merging counts...")

        # 初始化合并计数器
        counts_data[f'{self.isoform_prefix}Final_em'] = Counter()
        counts_data[f'{self.isoform_prefix}Final_eq'] = Counter()

        # 1. 合并 unique 和 multi_EM 计数 (Final_em)
        for transcript, count in counts_data[f'{self.isoform_prefix}unique'].items():
            counts_data[f'{self.isoform_prefix}Final_em'][transcript] += count

        for transcript, count in counts_data[f'{self.isoform_prefix}multi_EM'].items():
            counts_data[f'{self.isoform_prefix}Final_em'][transcript] += count

        # 2. 合并 unique 和 multi_equal 计数 (Final_eq)
        for transcript, count in counts_data[f'{self.isoform_prefix}unique'].items():
            counts_data[f'{self.isoform_prefix}Final_eq'][transcript] += count

        for transcript, count in counts_data[f'{self.isoform_prefix}multi_equal'].items():
            counts_data[f'{self.isoform_prefix}Final_eq'][transcript] += count

        # 验证合并结果
        total_em = sum(counts_data[f'{self.isoform_prefix}Final_em'].values())
        total_eq = sum(counts_data[f'{self.isoform_prefix}Final_eq'].values())
        total_unique = sum(
            counts_data[f'{self.isoform_prefix}unique'].values())
        total_multi_em = sum(
            counts_data[f'{self.isoform_prefix}multi_EM'].values())
        total_multi_eq = sum(
            counts_data[f'{self.isoform_prefix}multi_equal'].values())

        print("合并验证:")
        print(f"  - unique计数总计: {total_unique}")
        print(f"  - multi_EM计数总计: {round(total_multi_em)}")
        print(f"  - multi_equal计数总计: {round(total_multi_eq)}")
        print(f"  - Final_em总计: {round(total_em)} ")
        print(f"  - Final_eq总计: {round(total_eq)} ")

        # 更新实例变量
        self.counts_data = counts_data
        self.summary_stats = {
            'total_reads': total_count,
            'unique_mapped': sum(counts_data[f'{self.isoform_prefix}unique'].values()),
            'multi_mapped': sum(counts_data[f'{self.isoform_prefix}multi'].values()),
            'raw': sum(counts_data[f'{self.isoform_prefix}raw'].values()),
            'firstID': sum(counts_data[f'{self.isoform_prefix}firstID'].values()),
            'multi_equal': sum(counts_data[f'{self.isoform_prefix}multi_equal'].values()),
            'multi_EM': sum(counts_data[f'{self.isoform_prefix}multi_EM'].values()),
            'multi_EM_cannot_allocate_tpm': sum(counts_data[f'{self.isoform_prefix}multi_EM_cannot_allocate_tpm'].values()),
            'Final_em': total_em,
            'Final_eq': total_eq,
            'processing_time': time.time() - start_time
        }

        print(
            f"Count generation completed in {self.summary_stats['processing_time']:.2f} seconds")
        print("最终计数统计:")
        print(
            f"  - Final_em: {len(counts_data[f'{self.isoform_prefix}Final_em'])} 个转录本, {round(total_em)} 条reads")
        print(
            f"  - Final_eq: {len(counts_data[f'{self.isoform_prefix}Final_eq'])} 个转录本, {round(total_eq)} 条reads")

    def aggregate_gene_level_counts(self):
        """
        基因水平计数聚合
        """
        if self.annotation_df is None:
            print("Warning: Cannot aggregate gene level counts without annotation data")
            return {}, {}

        print("Aggregating gene level counts...")
        start_time = time.time()

        # 创建转录本到基因的映射列表
        transcript_to_gene = dict(
            zip(self.annotation_df['txname'], self.annotation_df['geneName']))

        # 初始化基因水平计数器
        gene_level_counts_unique_genes = {}
        gene_level_counts_multi_genes = {}

        # 初始化所有基因计数类型
        for count_type in self.counts_data.keys():
            if count_type.startswith(self.isoform_prefix):
                # 将isoform替换为gene的计数类型
                base_type = count_type.replace(self.isoform_prefix, '')
                gene_level_counts_unique_genes[f'{self.gene_prefix}{base_type}'] = Counter(
                )
                gene_level_counts_multi_genes[f'{self.gene_prefix}{base_type}'] = Counter(
                )

        # 第一步：计算基因水平的unique reads计数和multi计数
        # gene_unique_counts = Counter()
        for count_type, counter in self.counts_data.items():
            if not count_type.startswith('isoform_allocation_ratios'):
                pass
            else:
                continue

            # 获取对应的基因水平计数器，各种type都提前在这里设置好了
            gene_counter_unique = gene_level_counts_unique_genes.get(
                count_type.replace(self.isoform_prefix,
                                   self.gene_prefix), Counter()
            )
            gene_counter_multi = gene_level_counts_multi_genes.get(
                count_type.replace(self.isoform_prefix,
                                   self.gene_prefix), Counter()
            )

            # 开始不同的type挨个搞一遍，分别保存在两种列表中，uinque和multi
            for transcript_ids_str, event_count in counter.items():
                # 处理转录本ID（可能是单个或多个）
                if ',' not in transcript_ids_str:
                    # 单映射情况，获取一个gene单名，count加到一起
                    gene = transcript_to_gene.get(transcript_ids_str)
                    if gene:
                        gene_counter_unique[gene] += event_count
                        # if count_type == f'{self.isoform_prefix}unique':
                        #     gene_unique_counts[gene] += event_count
                else:
                    # 多映射情况：先检查是否映射到同一个基因
                    transcript_ids = transcript_ids_str.split(',')
                    genes = set()

                    for tid in transcript_ids:
                        gene = transcript_to_gene.get(tid)
                        if gene:
                            genes.add(gene)
                    # 是同一个基因来的
                    if len(genes) == 1:
                        # 映射到同一个基因，存到unique列表，必须是+=才可以
                        gene = list(genes)[0]
                        # 这条，原本的multi，暂时先注释掉 --20251124
                        gene_counter_unique[gene] += event_count
                        # 直接存到unique的条目中，这本是isoform中的multi，现在gene中是unique了
                        gene_level_counts_unique_genes[f'{self.gene_prefix}unique'][gene] += event_count
                    # 不是同一个基因来的，存到multi
                    elif len(genes) > 1:
                        # 映射到多个基因
                        gene_key = ','.join(sorted(genes))
                        gene_counter_multi[gene_key] += event_count

        print(f"基因水平unique reads计数完成: {len(gene_counter_unique)} 个基因")

        # 第二步：使用基因水平的unique reads计算TPM,暂时用最长转录本长度
        # 1. 检查unique和gene水平的东西都存在哎
        if hasattr(self, 'gene_level_counts_unique_genes') and self.gene_level_counts_unique_genes:
            # 检查是否有gene水平的multi数据
            gene_multi_key = f'{self.gene_prefix}multi'
            if gene_multi_key in self.gene_level_counts_multi_genes and self.gene_level_counts_multi_genes[gene_multi_key]:
                print("Starting advanced multi-mapping analysis for gene level...")

                # 用有unique reads的genes为gene水平的基因们构建长度字典（这里目前采用基因最长转录本长度，还可以采用其他种类长度替代）
                gene_lengths = {}
                for gene_name in set(self.gene_level_counts_unique_genes[f'{self.gene_prefix}unique'].keys()):
                    gene_transcripts = self.annotation_df[self.annotation_df['geneName'] == gene_name]
                    if not gene_transcripts.empty:
                        max_length = gene_transcripts['txLength'].max()
                        gene_lengths[gene_name] = max_length

                # 使用通用方法处理self.gene_level_counts_multi_genes列表里面水平的gene水平的多映射并分配
                self._rescue_multi_mappings_by_tpm(
                    counts_data=self.gene_level_counts_multi_genes,
                    prefix=self.gene_prefix,
                    length_dict=gene_lengths,
                    annotation_df=self.annotation_df
                )
                print("Gene level advanced multi-mapping analysis completed.")

        # 第四步：合并计数
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_em'] = Counter(
        )
        gene_level_counts_unique_genes[f'{self.gene_prefix}Final_eq'] = Counter(
        )

        # 1. 合并 unique 和 multi_EM 计数 (最终的Final_em)
        #Final_em = gene_unique + gene_EM
        for gene, count in gene_level_counts_unique_genes[f'{self.gene_prefix}unique'].items():
            gene_level_counts_unique_genes[f'{self.gene_prefix}Final_em'][gene] += count
        # for gene, count in gene_level_counts_unique_genes[f'{self.gene_prefix}multi'].items():
        #     gene_level_counts_unique_genes[f'{self.gene_prefix}Final_em'][gene] += count
        for gene, count in gene_level_counts_unique_genes[f'{self.gene_prefix}multi_EM'].items():
            gene_level_counts_unique_genes[f'{self.gene_prefix}Final_em'][gene] += count

        # 2. 合并 unique 和 multi_equal 计数 (Final_eq)
        #Final_eq = gene_unique+gene_multi_equal
        for gene, count in gene_level_counts_unique_genes[f'{self.gene_prefix}unique'].items():
            gene_level_counts_unique_genes[f'{self.gene_prefix}Final_eq'][gene] += count
        for gene, count in gene_level_counts_unique_genes[f'{self.gene_prefix}multi_equal'].items():
            gene_level_counts_unique_genes[f'{self.gene_prefix}Final_eq'][gene] += count

        processing_time = time.time() - start_time
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

            print(f"找到转录本水平计数类型: {isoform_count_types}")

            if not isoform_count_types:
                print("没有转录本水平计数数据")
                return {}

            # 使用firstID作为基础数据框
            firstID_type = f'{self.isoform_prefix}firstID'
            if firstID_type not in self.counts_data or not self.counts_data[firstID_type]:
                print("没有firstID计数数据，无法生成转录本水平文件")
                return {}

            combined_df = pd.DataFrame(self.counts_data[firstID_type].items(),
                                       columns=['Transcript', 'firstID_count'])

            # 合并所有计数类型
            for count_type in isoform_count_types:
                if count_type == 'firstID':  # 已经作为基础，跳过
                    continue

                full_type = f'{self.isoform_prefix}{count_type}'
                if full_type in self.counts_data and self.counts_data[full_type]:
                    temp_df = pd.DataFrame(self.counts_data[full_type].items(),
                                           columns=['Transcript', f'{count_type}_count'])
                    combined_df = combined_df.merge(
                        temp_df, on='Transcript', how='outer')

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

            # 填充NaN值为0
            count_columns = [
                col for col in combined_df.columns if col.endswith('_count')]
            combined_df[count_columns] = combined_df[count_columns].fillna(0)

            # 保存文件
            combined_filename = self.output_dir / \
                f'{base_name}_isoform_level.counts.csv'
            combined_df.to_csv(combined_filename,
                               index=False, float_format='%.2f')
            isoform_files['isoform'] = combined_filename

            ratios_key = f'{self.isoform_prefix}allocation_ratios'
            if ratios_key in self.counts_data and self.counts_data[ratios_key]:
                rows = []
                for ids_str, maps in self.counts_data[ratios_key].items():
                    ids = ids_str.split(',')
                    em_map = maps.get('EM')
                    equal_map = maps.get('equal')
                    other_map = maps.get('multi2all')
                    em_str = ';'.join([str(em_map.get(t, '')) if em_map is not None else '' for t in ids])
                    eq_str = ';'.join([str(equal_map.get(t, '')) if equal_map is not None else '' for t in ids])
                    oth_str = ';'.join([str(other_map.get(t, '')) if other_map is not None else '' for t in ids])
                    rows.append({
                        'Transcripts': ids_str,
                        'allocation_ratios_count_EM': em_str,
                        'allocation_ratios_count_equal': eq_str,
                        'allocation_ratios_count_multi2all': oth_str
                    })
                iso_multi_df = pd.DataFrame(rows)
                iso_multi_filename = self.output_dir / f'{base_name}_isoform_multi_level.counts.csv'
                iso_multi_df.to_csv(iso_multi_filename, index=False)
                isoform_files['isoform_multi'] = iso_multi_filename

            print(f"转录本水平计数文件生成完成: {len(combined_df)} 个转录本")

        except Exception as e:
            print(f"生成转录本水平计数文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()

        return isoform_files

    def _generate_gene_level_files(self, base_name):
        """生成基因水平计数文件 - 完整修复版"""
        if self.annotation_df is None:
            print("没有注释信息，跳过基因水平文件生成")
            return {}

        print("生成基因水平计数文件...")

        # 调试信息：检查基因水平数据
        print(
            f"gene_level_counts_unique_genes 存在: {hasattr(self, 'gene_level_counts_unique_genes')}")
        if hasattr(self, 'gene_level_counts_unique_genes'):
            print(
                f"gene_level_counts_unique_genes 类型: {type(self.gene_level_counts_unique_genes)}")
            if self.gene_level_counts_unique_genes:
                print(
                    f"gene_level_counts_unique_genes 键: {list(self.gene_level_counts_unique_genes.keys())}")
                for key, counter in self.gene_level_counts_unique_genes.items():
                    print(f"  {key}: {len(counter)} 个条目")
            else:
                print("gene_level_counts_unique_genes 为空")

        print(
            f"gene_level_counts_multi_genes 存在: {hasattr(self, 'gene_level_counts_multi_genes')}")
        if hasattr(self, 'gene_level_counts_multi_genes'):
            print(
                f"gene_level_counts_multi_genes 类型: {type(self.gene_level_counts_multi_genes)}")
            if self.gene_level_counts_multi_genes:
                print(
                    f"gene_level_counts_multi_genes 键: {list(self.gene_level_counts_multi_genes.keys())}")
                for key, counter in self.gene_level_counts_multi_genes.items():
                    print(f"  {key}: {len(counter)} 个条目")
            else:
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
                print("发现 unique genes 数据")

            # 检查 multi genes 数据
            if (hasattr(self, 'gene_level_counts_multi_genes') and
                self.gene_level_counts_multi_genes and
                    any(len(counter) > 0 for counter in self.gene_level_counts_multi_genes.values())):
                has_multi_data = True
                print("发现 multi genes 数据")

            if not has_unique_data and not has_multi_data:
                print("没有基因水平计数数据，跳过文件生成")
                return {}

            # 生成单个基因的计数文件
            if has_unique_data:
                print("开始生成单个基因计数文件...")
                single_gene_data = []

                # 收集所有唯一的基因
                all_genes = set()
                for counter in self.gene_level_counts_unique_genes.values():
                    if counter:  # 确保计数器非空
                        all_genes.update(counter.keys())

                print(f"处理 {len(all_genes)} 个唯一基因")

                # 为每个基因构建数据行
                for gene in all_genes:
                    gene_row = {'Gene': gene}

                    # 收集该基因在所有计数类型中的值
                    for count_type, counter in self.gene_level_counts_unique_genes.items():
                        if counter:  # 确保计数器非空
                            # 提取基础计数类型名称（去掉 gene_ 前缀）
                            base_count_type = count_type.replace(
                                self.gene_prefix, '')
                            count_value = counter.get(gene, 0)
                            gene_row[f'{base_count_type}_count'] = count_value

                    single_gene_data.append(gene_row)

                if single_gene_data:
                    # 转换为DataFrame
                    single_gene_df = pd.DataFrame(single_gene_data)
                    print(f"单个基因数据框形状: {single_gene_df.shape}")

                    # 添加基因注释信息
                    gene_annotation = self._get_gene_annotation_data()
                    if gene_annotation is not None:
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

                    # 保存文件
                    gene_filename = self.output_dir / \
                        f'{base_name}_gene_level.counts.csv'
                    single_gene_df.to_csv(
                        gene_filename, index=False, float_format='%.2f')
                    gene_files['gene'] = gene_filename
                    print(f"单个基因计数文件生成完成: {len(single_gene_df)} 个基因")
                else:
                    print("没有单个基因数据可生成文件")

            # 生成多基因组合的计数文件
            if has_multi_data:
                print("开始生成多基因组合计数文件...")
                multi_genes_data = []

                # 收集所有多基因组合
                all_multi_combinations = set()
                for counter in self.gene_level_counts_multi_genes.values():
                    if counter:  # 确保计数器非空
                        all_multi_combinations.update(counter.keys())

                print(f"处理 {len(all_multi_combinations)} 个多基因组合")

                for gene_combo in all_multi_combinations:
                    combo_row = {'Gene_Combination': gene_combo}

                    # 收集该组合在所有计数类型中的值
                    for count_type, counter in self.gene_level_counts_multi_genes.items():
                        if counter:  # 确保计数器非空
                            base_count_type = count_type.replace(
                                self.gene_prefix, '')
                            count_value = counter.get(gene_combo, 0)
                            combo_row[f'{base_count_type}_count'] = count_value

                    ratios = self.gene_level_counts_multi_genes.get(f'{self.gene_prefix}allocation_ratios', {})
                    if ratios:
                        genes = gene_combo.split(',')
                        if gene_combo in ratios:
                            eq_map = ratios[gene_combo].get('equal')
                            em_map = ratios[gene_combo].get('EM')
                            m2a_map = ratios[gene_combo].get('multi2all')
                            
                            if eq_map:
                                combo_row['allocation_ratios_count_equal'] = ';'.join([f"{eq_map.get(g, '') if g in eq_map else ''}" for g in genes])
                            else:
                                combo_row['allocation_ratios_count_equal'] = ';'.join(['' for _ in genes])
                            
                            if em_map is not None:
                                combo_row['allocation_ratios_count_EM'] = ';'.join([f"{em_map.get(g, '') if g in em_map else ''}" for g in genes])
                            else:
                                combo_row['allocation_ratios_count_EM'] = ';'.join(['' for _ in genes])
                            
                            if m2a_map:
                                combo_row['allocation_ratios_count_multi2all'] = ';'.join([f"{m2a_map.get(g, '') if g in m2a_map else ''}" for g in genes])
                            else:
                                combo_row['allocation_ratios_count_multi2all'] = ';'.join(['' for _ in genes])
                    
                    multi_genes_data.append(combo_row)

                if multi_genes_data:
                    multi_genes_df = pd.DataFrame(multi_genes_data)
                    
                    multi_genes_filename = self.output_dir / \
                        f'{base_name}_multi_genes_level.counts.csv'
                    
                    multi_genes_df.to_csv(
                        multi_genes_filename, index=False, float_format='%.2f')
                   
                    gene_files['multi_genes'] = multi_genes_filename
                    print(f"多基因组合计数文件生成完成: {len(multi_genes_df)} 个组合")
                
                else:
                    print("没有多基因组合数据可生成文件")

        except Exception as e:
            print(f"生成基因水平计数文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()

        return gene_files

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

        print(f"使用的基因注释列: {selected_cols}")

        # 获取去重的基因注释
        gene_annotation = self.annotation_df[selected_cols].drop_duplicates(subset=[
                                                                            'geneName'])

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

        count_files = {}

        # 生成转录本水平计数文件
        if self.level in ['isoform', 'both']:
            try:
                isoform_files = self._generate_isoform_level_files(base_name)
                count_files.update(isoform_files)
                print("isoform 水平计数文件生成完成")
            except Exception as e:
                print(f"转录本水平计数文件生成失败: {e}")

        # 生成基因水平计数文件
        if self.annotation_df is not None and self.level in ['gene', 'both']:
            try:
                # 修复：检查基因水平计数数据是否存在?有可能生成了isoform，但是gff文件没有给合适，所以匹配不到gene名，导致这个情况。暂时先这样吧，，一般不会碰到。所以判断是否具有基因level数据的那部分先删除
                # has_gene_data = True
                # 修复：检查正确的基因水平数据位置
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

                print(f'has_gene_data: {has_gene_data}')
                if has_gene_data:
                    # print('has_gene_data', has_gene_data)
                    gene_files = self._generate_gene_level_files(base_name)
                    count_files.update(gene_files)
                    print("基因水平计数文件生成完成")
                else:
                    print("没有基因水平计数数据，跳过基因水平文件生成")
            except Exception as e:
                print(f"基因水平计数文件生成失败: {e}")

        return count_files
##########################################################################################
# 新加的，看行不行，行就注释掉上面的20251125

    # def generate_count_files(self):
    #     """
    #     生成isoform和gene level计数文件（包含分配比例）
    #     """
    #     if self.output_filename:
    #         base_name = Path(self.output_filename).stem
    #     else:
    #         base_name = self.input_file.stem

    #     count_files = {}

    #     # 生成转录本水平计数文件
    #     if self.level in ['isoform', 'both']:
    #         try:
    #             isoform_files = self._generate_isoform_level_files(base_name)
    #             count_files.update(isoform_files)
    #             print("isoform水平计数文件生成完成")
    #         except Exception as e:
    #             print(f"转录本水平计数文件生成失败: {e}")

    #     # 生成基因水平计数文件（使用新版本，包含分配比例）
    #     if self.annotation_df is not None and self.level in ['gene', 'both']:
    #         try:
    #             # 使用新版本的基因水平文件生成方法
    #             gene_files = self._generate_gene_level_files_with_allocation(
    #                 base_name)
    #             count_files.update(gene_files)

    #             # 生成分配比例摘要报告
    #             if hasattr(self, 'counts_data') and f'{self.gene_prefix}allocation_ratios' in self.counts_data:
    #                 report_file = self.generate_allocation_summary_report(
    #                     base_name)
    #                 if report_file:
    #                     count_files['allocation_summary'] = report_file

    #             print("基因水平计数文件（包含分配比例）生成完成")
    #         except Exception as e:
    #             print(f"基因水平计数文件生成失败: {e}")

    #     return count_files

    # def _generate_gene_level_files_with_allocation(self, base_name):
    #     """生成包含分配比例的基因水平计数文件"""
    #     if self.annotation_df is None:
    #         print("没有注释信息，跳过基因水平文件生成")
    #         return {}

    #     print("生成基因水平计数文件（包含分配比例）...")
    #     gene_files = {}

    #     try:
    #         # 检查是否有基因水平计数数据
    #         has_unique_data = hasattr(
    #             self, 'gene_level_counts_unique_genes') and self.gene_level_counts_unique_genes
    #         has_multi_data = hasattr(
    #             self, 'gene_level_counts_multi_genes') and self.gene_level_counts_multi_genes

    #         if not has_unique_data and not has_multi_data:
    #             print("没有基因水平计数数据，跳过文件生成")
    #             return {}

    #         # 获取分配比例数据
    #         allocation_ratios = {}
    #         if hasattr(self, 'counts_data') and f'{self.gene_prefix}allocation_ratios' in self.counts_data:
    #             allocation_ratios = self.counts_data[f'{self.gene_prefix}allocation_ratios']
    #             print(f"加载了 {len(allocation_ratios)} 个分配比例记录")

    #         # 生成单个基因的计数文件
    #         if has_unique_data:
    #             print("开始生成单个基因计数文件（包含分配比例）...")
    #             single_gene_data = []

    #             # 收集所有唯一的基因
    #             all_genes = set()
    #             for counter in self.gene_level_counts_unique_genes.values():
    #                 if counter:
    #                     all_genes.update(counter.keys())

    #             print(f"处理 {len(all_genes)} 个唯一基因")

    #             # 为每个基因构建数据行
    #             for gene in all_genes:
    #                 gene_row = {'Gene': gene}

    #                 # 收集该基因在所有计数类型中的值
    #                 for count_type, counter in self.gene_level_counts_unique_genes.items():
    #                     if counter:
    #                         # 提取基础计数类型名称（去掉 gene_ 前缀）
    #                         base_count_type = count_type.replace(
    #                             self.gene_prefix, '')
    #                         count_value = counter.get(gene, 0)
    #                         gene_row[f'{base_count_type}_count'] = count_value

    #                 # 添加分配比例信息（单基因的分配比例默认为1.0）
    #                 gene_row['equal_allocation_ratio'] = 1.0
    #                 gene_row['em_allocation_ratio'] = 1.0
    #                 gene_row['allocation_method'] = 'single_gene'

    #                 single_gene_data.append(gene_row)

    #             if single_gene_data:
    #                 # 转换为DataFrame
    #                 single_gene_df = pd.DataFrame(single_gene_data)
    #                 print(f"单个基因数据框形状: {single_gene_df.shape}")

    #                 # 添加基因注释信息
    #                 gene_annotation = self._get_gene_annotation_data()
    #                 if gene_annotation is not None:
    #                     print(f"合并基因注释信息，注释数据形状: {gene_annotation.shape}")
    #                     single_gene_df = single_gene_df.merge(
    #                         gene_annotation,
    #                         left_on='Gene',
    #                         right_on='geneName',
    #                         how='left'
    #                     )

    #                     # 移除重复的geneName列
    #                     if 'geneName' in single_gene_df.columns and 'Gene' in single_gene_df.columns:
    #                         single_gene_df = single_gene_df.drop(
    #                             'geneName', axis=1)

    #                 # 保存文件
    #                 gene_filename = self.output_dir / \
    #                     f'{base_name}_gene_level.counts.csv'
    #                 single_gene_df.to_csv(
    #                     gene_filename, index=False, float_format='%.2f')
    #                 gene_files['gene'] = gene_filename
    #                 print(f"单个基因计数文件生成完成: {len(single_gene_df)} 个基因")
    #             else:
    #                 print("没有单个基因数据可生成文件")

    #         # 生成多基因组合的计数文件（包含详细的分配比例）
    #         if has_multi_data:
    #             print("开始生成多基因组合计数文件（包含分配比例）...")
    #             multi_genes_data = []

    #             # 收集所有多基因组合
    #             all_multi_combinations = set()
    #             for counter in self.gene_level_counts_multi_genes.values():
    #                 if counter:
    #                     all_multi_combinations.update(counter.keys())

    #             print(f"处理 {len(all_multi_combinations)} 个多基因组合")

    #             for gene_combo in all_multi_combinations:
    #                 combo_row = {'Gene_Combination': gene_combo}

    #                 # 收集该组合在所有计数类型中的值
    #                 for count_type, counter in self.gene_level_counts_multi_genes.items():
    #                     if counter:
    #                         base_count_type = count_type.replace(
    #                             self.gene_prefix, '')
    #                         count_value = counter.get(gene_combo, 0)
    #                         combo_row[f'{base_count_type}_count'] = count_value

    #                 # 添加分配比例信息
    #                 genes = gene_combo.split(',')

    #                 # 查找对应的分配比例
    #                 equal_ratio_key = f"{gene_combo}_equal"
    #                 em_ratio_key = f"{gene_combo}_EM"

    #                 if equal_ratio_key in allocation_ratios:
    #                     equal_ratios = allocation_ratios[equal_ratio_key]
    #                     # 格式化为字符串，如 "geneA:0.4,geneB:0.6"
    #                     equal_ratio_str = ','.join(
    #                         [f"{gene}:{ratio:.3f}" for gene, ratio in equal_ratios.items()])
    #                     combo_row['equal_allocation_ratios'] = equal_ratio_str
    #                 else:
    #                     # 默认平均分配
    #                     equal_share = 1.0 / len(genes)
    #                     equal_ratio_str = ','.join(
    #                         [f"{gene}:{equal_share:.3f}" for gene in genes])
    #                     combo_row['equal_allocation_ratios'] = equal_ratio_str

    #                 if em_ratio_key in allocation_ratios and allocation_ratios[em_ratio_key] is not None:
    #                     em_ratios = allocation_ratios[em_ratio_key]
    #                     em_ratio_str = ','.join(
    #                         [f"{gene}:{ratio:.3f}" for gene, ratio in em_ratios.items()])
    #                     combo_row['em_allocation_ratios'] = em_ratio_str
    #                     combo_row['allocation_method'] = 'EM'
    #                 else:
    #                     # 无法分配TPM的情况
    #                     combo_row['em_allocation_ratios'] = 'N/A'
    #                     combo_row['allocation_method'] = 'equal_or_cannot_allocate'

    #                 # 计算分配比例的总和（应为1.0）
    #                 if equal_ratio_key in allocation_ratios:
    #                     total_equal = sum(
    #                         allocation_ratios[equal_ratio_key].values())
    #                     combo_row['equal_allocation_sum'] = f"{total_equal:.3f}"

    #                 if em_ratio_key in allocation_ratios and allocation_ratios[em_ratio_key] is not None:
    #                     total_em = sum(
    #                         allocation_ratios[em_ratio_key].values())
    #                     combo_row['em_allocation_sum'] = f"{total_em:.3f}"

    #                 multi_genes_data.append(combo_row)

    #             if multi_genes_data:
    #                 multi_genes_df = pd.DataFrame(multi_genes_data)
    #                 multi_genes_filename = self.output_dir / \
    #                     f'{base_name}_multi_genes_level.counts.csv'
    #                 multi_genes_df.to_csv(
    #                     multi_genes_filename, index=False, float_format='%.2f')
    #                 gene_files['multi_genes'] = multi_genes_filename
    #                 print(f"多基因组合计数文件生成完成: {len(multi_genes_df)} 个组合")
    #             else:
    #                 print("没有多基因组合数据可生成文件")

    #         # 生成分配比例详情文件
    #         if allocation_ratios:
    #             print("开始生成分配比例详情文件...")
    #             allocation_detail_data = []

    #             for ratio_key, ratios in allocation_ratios.items():
    #                 if ratios is None:
    #                     # 无法分配的情况
    #                     detail_row = {
    #                         'multi_mapping_event': ratio_key.replace('_equal', '').replace('_EM', ''),
    #                         'allocation_method': ratio_key.split('_')[-1],
    #                         'allocation_ratios': 'N/A (cannot allocate)',
    #                         'genes_involved': ratio_key.split('_')[0],
    #                         'ratio_sum': 'N/A'
    #                     }
    #                 else:
    #                     # 正常分配情况
    #                     genes = list(ratios.keys())
    #                     ratio_str = ','.join(
    #                         [f"{gene}:{ratio:.4f}" for gene, ratio in ratios.items()])
    #                     ratio_sum = sum(ratios.values())

    #                     detail_row = {
    #                         'multi_mapping_event': ratio_key.replace('_equal', '').replace('_EM', ''),
    #                         'allocation_method': ratio_key.split('_')[-1],
    #                         'allocation_ratios': ratio_str,
    #                         'genes_involved': ','.join(genes),
    #                         'ratio_sum': f"{ratio_sum:.4f}",
    #                         'gene_count': len(genes)
    #                     }

    #                 allocation_detail_data.append(detail_row)

    #             if allocation_detail_data:
    #                 allocation_df = pd.DataFrame(allocation_detail_data)
    #                 allocation_filename = self.output_dir / \
    #                     f'{base_name}_allocation_details.csv'
    #                 allocation_df.to_csv(allocation_filename, index=False)
    #                 gene_files['allocation_details'] = allocation_filename
    #                 print(f"分配比例详情文件生成完成: {len(allocation_df)} 条记录")

    #         return gene_files

    #     except Exception as e:
    #         print(f"生成基因水平计数文件时出错: {str(e)}")
    #         import traceback
    #         traceback.print_exc()
    #         return {}

    # def _get_gene_annotation_data(self):
    #     """获取基因注释数据（包含分配比例信息）"""
    #     if self.annotation_df is None:
    #         return None

    #     # 检查可用的注释列
    #     available_columns = self.annotation_df.columns.tolist()

    #     # 选择基因相关的注释列
    #     selected_cols = ['geneName']

    #     # 添加其他可能有用的列
    #     optional_cols = {
    #         'symbol': ['symbol', 'genename', 'gene_name'],
    #         'genelongesttxLength': ['genelongesttxLength', 'genelonesttxlength', 'txLength'],
    #         'genelongestcdsLength': ['genelongestcdsLength', 'genelongestcdslength', 'cdsLength'],
    #         'description': ['description', 'gene_description', 'product']
    #     }

    #     for target_col, source_cols in optional_cols.items():
    #         for col in source_cols:
    #             if col in available_columns:
    #                 selected_cols.append(col)
    #                 break

    #     print(f"使用的基因注释列: {selected_cols}")

    #     # 获取去重的基因注释
    #     gene_annotation = self.annotation_df[selected_cols].drop_duplicates(subset=[
    #                                                                         'geneName'])

    #     # 重命名列以保持一致性
    #     rename_map = {}
    #     if 'genename' in gene_annotation.columns:
    #         rename_map['genename'] = 'symbol'
    #     if 'genelonesttxlength' in gene_annotation.columns:
    #         rename_map['genelonesttxlength'] = 'genelongesttxLength'
    #     if 'genelongestcdslength' in gene_annotation.columns:
    #         rename_map['genelongestcdslength'] = 'genelongestcdsLength'

    #     if rename_map:
    #         gene_annotation = gene_annotation.rename(columns=rename_map)

    #     return gene_annotation

    # def _format_allocation_ratios(self, ratios_dict):
    #     """格式化分配比例为字符串"""
    #     if not ratios_dict:
    #         return "N/A"

    #     ratio_strings = []
    #     for gene, ratio in ratios_dict.items():
    #         ratio_strings.append(f"{gene}:{ratio:.3f}")

    #     return ','.join(ratio_strings)

    # def _calculate_allocation_summary(self, allocation_ratios):
    #     """计算分配比例的统计摘要"""
    #     if not allocation_ratios:
    #         return {}

    #     summary = {
    #         'total_events': len(allocation_ratios),
    #         'equal_events': 0,
    #         'em_events': 0,
    #         'cannot_allocate_events': 0,
    #         'average_genes_per_event': 0,
    #         'ratio_deviation_stats': {}
    #     }

    #     gene_counts = []
    #     ratio_deviations = []

    #     for key, ratios in allocation_ratios.items():
    #         if ratios is None:
    #             summary['cannot_allocate_events'] += 1
    #             continue

    #         if key.endswith('_equal'):
    #             summary['equal_events'] += 1
    #         elif key.endswith('_EM'):
    #             summary['em_events'] += 1

    #         gene_counts.append(len(ratios))

    #         # 计算比例与平均分配的偏差
    #         if len(ratios) > 1:
    #             equal_share = 1.0 / len(ratios)
    #             deviations = [abs(ratio - equal_share)
    #                           for ratio in ratios.values()]
    #             avg_deviation = sum(deviations) / len(deviations)
    #             ratio_deviations.append(avg_deviation)

    #     if gene_counts:
    #         summary['average_genes_per_event'] = sum(
    #             gene_counts) / len(gene_counts)

    #     if ratio_deviations:
    #         summary['ratio_deviation_stats'] = {
    #             'avg_deviation_from_equal': sum(ratio_deviations) / len(ratio_deviations),
    #             'max_deviation': max(ratio_deviations) if ratio_deviations else 0,
    #             'min_deviation': min(ratio_deviations) if ratio_deviations else 0
    #         }

    #     return summary

    # def generate_allocation_summary_report(self, base_name):
    #     """生成分配比例摘要报告"""
    #     if not hasattr(self, 'counts_data') or f'{self.gene_prefix}allocation_ratios' not in self.counts_data:
    #         return None

    #     allocation_ratios = self.counts_data[f'{self.gene_prefix}allocation_ratios']
    #     summary = self._calculate_allocation_summary(allocation_ratios)

    #     if not summary:
    #         return None

    #     # 生成报告文件
    #     report_lines = [
    #         "基因水平多映射分配比例摘要报告",
    #         "=" * 50,
    #         f"报告生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
    #         f"输入文件: {self.input_file.name}",
    #         f"注释文件: {self.gxf_file if self.gxf_file else 'N/A'}",
    #         "",
    #         "分配事件统计:",
    #         f"  - 总多映射事件数: {summary['total_events']}",
    #         f"  - 平均分配事件数: {summary['equal_events']}",
    #         f"  - EM分配事件数: {summary['em_events']}",
    #         f"  - 无法分配事件数: {summary['cannot_allocate_events']}",
    #         f"  - 平均每个事件的基因数: {summary['average_genes_per_event']:.2f}",
    #         ""
    #     ]

    #     if summary['ratio_deviation_stats']:
    #         stats = summary['ratio_deviation_stats']
    #         report_lines.extend([
    #             "分配比例偏差统计 (与平均分配相比):",
    #             f"  - 平均偏差: {stats['avg_deviation_from_equal']:.4f}",
    #             f"  - 最大偏差: {stats['max_deviation']:.4f}",
    #             f"  - 最小偏差: {stats['min_deviation']:.4f}",
    #             ""
    #         ])

    #     # 添加前10个分配事件的详细信息
    #     report_lines.append("前10个多映射事件分配详情:")
    #     report_lines.append("-" * 50)

    #     event_count = 0
    #     for key, ratios in list(allocation_ratios.items())[:10]:
    #         if ratios is None:
    #             report_lines.append(f"{key}: 无法分配TPM")
    #         else:
    #             ratio_str = self._format_allocation_ratios(ratios)
    #             report_lines.append(f"{key}: {ratio_str}")
    #         event_count += 1
    #         if event_count >= 10:
    #             break

    #     # 写入文件
    #     report_filename = self.output_dir / \
    #         f'{base_name}_allocation_summary.txt'
    #     with open(report_filename, 'w', encoding='utf-8') as f:
    #         f.write('\n'.join(report_lines))

    #     print(f"分配比例摘要报告生成完成: {report_filename}")
    #     return report_filename
    # 主运行进程

    def run(self):
        """运行完整的计数流程"""
        print("=" * 60)
        print("fansetools count - Starting processing")
        print("=" * 60)

        if self.level in ['gene', 'both'] and self.annotation_df is None:
            print("注意：生成 gene level counts 需要提供 --gxf gff/gtf 文件")
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

            if self.gene_level_counts_unique_genes:
                print(
                    f"Gene level aggregation completed: {len(self.gene_level_counts_unique_genes)} unique-gene count types")
            if self.gene_level_counts_multi_genes:
                print(
                    f"Gene level aggregation completed: {len(self.gene_level_counts_multi_genes)} multi-gene count types")
        else:
            print("No annotation provided, skipping gene level aggregation")
            self.gene_level_counts_unique_genes = {}
            self.gene_level_counts_multi_genes = {}

        # 4. 生成计数文件
        count_files = self.generate_count_files()

        # 5. 生成摘要报告
        self.generate_summary()

        print("fansetools count - Processing completed")
        print("=" * 60)

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
            f.write(
                f"Multi-mapping events: {len(self.counts_data['multi'])}\n")
            if self.counts_data['multi']:
                total_multi_reads = sum(self.counts_data['multi'].values())
                avg_reads_per_event = total_multi_reads / \
                    len(self.counts_data['multi'])
                f.write(f"Total multi-mapped reads: {total_multi_reads}\n")
                f.write(
                    f"Average reads per multi-mapping event: {avg_reads_per_event:.2f}\n")

    def debug_gene_level_data(self):
        """调试基因水平数据"""
        print("=== 调试基因水平数据 ===")

        # 检查实例变量
        print(
            f"gene_level_counts_unique_genes 存在: {hasattr(self, 'gene_level_counts_unique_genes')}")
        if hasattr(self, 'gene_level_counts_unique_genes'):
            print(f"类型: {type(self.gene_level_counts_unique_genes)}")
            if isinstance(self.gene_level_counts_unique_genes, dict):
                print(f"键数量: {len(self.gene_level_counts_unique_genes)}")
                for key, value in self.gene_level_counts_unique_genes.items():
                    if hasattr(value, '__len__'):
                        print(f"  {key}: {len(value)} 个条目")
                    else:
                        print(f"  {key}: {type(value)}")
            else:
                print(f"值: {self.gene_level_counts_unique_genes}")

        print(
            f"gene_level_counts_multi_genes 存在: {hasattr(self, 'gene_level_counts_multi_genes')}")
        if hasattr(self, 'gene_level_counts_multi_genes'):
            print(f"类型: {type(self.gene_level_counts_multi_genes)}")
            if isinstance(self.gene_level_counts_multi_genes, dict):
                print(f"键数量: {len(self.gene_level_counts_multi_genes)}")
                for key, value in self.gene_level_counts_multi_genes.items():
                    if hasattr(value, '__len__'):
                        print(f"  {key}: {len(value)} 个条目")
                    else:
                        print(f"  {key}: {type(value)}")
            else:
                print(f"值: {self.gene_level_counts_multi_genes}")

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
     FANSeTools - Summary the RNA-seq Count
     ''']

    for line in mini_art:
        print(line)


def load_annotation_data(args):
    """加载注释数据"""
    if not args.gxf:
        print("错误: 需要提供 --gxf 参数")
        return None

    print(f"\nLoading annotation from {args.gxf}")

    # 检查是否存在同名的refflat文件
    refflat_file = os.path.splitext(args.gxf)[0] + ".genomic.refflat"

    if os.path.exists(refflat_file):
        print(f"Found existing refflat file: {refflat_file}")
        try:
            annotation_df = read_refflat_with_commented_header(refflat_file)
            print(
                f"Successfully loaded {len(annotation_df)} transcripts from existing refflat file")
            return annotation_df
        except Exception as e:
            print(f"Error loading refflat file: {e}")
            print("Converting GXF file instead...")

    print(f"No existing refflat file found at {refflat_file}")
    print("Converting GXF file to refflat format...")

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
                         header=None, names=columns)
    else:
        # 如果没有注释头部，使用默认列名
        default_columns = [
            "geneName", "txname", "chrom", "strand", "txStart", "txEnd",
            "cdsStart", "cdsEnd", "exonCount", "exonStarts", "exonEnds",
            "symbol", "g_biotype", "t_biotype", "description", "protein_id",
            "txLength", "cdsLength", "utr5Length", "utr3Length",
            "genelongesttxLength", "genelongestcdsLength", "geneEffectiveLength"
        ]
        df = pd.read_csv(file_path, sep='\t', header=None,
                         names=default_columns)

    return df


def add_count_subparser(subparsers):
    """命令行主函数"""

    parser = subparsers.add_parser(
        'count',
        help='运行FANSe to count，输出readcount',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        使用示例:
            默认isoform level
          单个文件处理:
            fanse count -i sample.fanse3 -o results/ --gxf annotation.gtf

          批量处理目录中所有fanse3文件:
            fanse count -i /data/*.fanse3 -o /output/ --gxf annotation.gtf

          双端测序数据:
            fanse count -i R1.fanse3 -r R2.fanse3 -o results/ --gxf annotation.gtf

        **如需要基因水平计数，需要输入gtf/gff/refflat/简单g-t对应文件，--gxf都可以解析
          基因水平计数:
            fanse count -i *.fanse3 -o results/ --gxf annotation.gtf --level gene

          同时输出基因和转录本水平:
            fanse count -i *.fanse3 -o results/ --gxf annotation.gtf --level both

          处理中断后重新运行（自动跳过已处理的文件[输出文件夹中存在对应结果文件，需重新运行请删除]）
            fanse count -i *.fanse3 -o results/ --gxf annotation.gtf --resume

            # 指定4个并行进程
            fanse count -i "*.fanse3" -o results --gxf annotation.gtf --p 4

            使用所有CPU核心并行处理:
            fanse count -i *.fanse3 -o results --gxf annotation.gtf -p 0
                """
    )

    parser.add_argument('-i', '--input', required=True,
                        help='Input fanse3 file,输入FANSe3文件/目录/通配符（支持批量处理）')
    parser.add_argument('-r', '--paired-end',
                        help='Paired-end fanse3 file (optional)')
    parser.add_argument('-o', '--output', required=False,
                        help='Output directory,输出路径（文件或目录，自动检测）')

    # parser.add_argument('--minreads', type=int, default=0,
    #                     help='Minimum reads threshold for filtering')
    parser.add_argument('--rpkm', type=float, default=0,
                        help='RPKM threshold for filtering，尚未完成')

    parser.add_argument('--gxf', required=False,
                        help='Input GXF file (GTF or GFF3),if not provided, just give out isoform level readcounts')
    parser.add_argument('--annotation-output',
                        help='Output refFlat file prefix (optional)')

    parser.add_argument('--level', choices=['gene', 'isoform', 'both'], default='gene',
                        help='Counting level')

    parser.add_argument('--resume', required=False, action='store_true',
                        help='可从上次运行断掉的地方自动开始，自动检测文件夹中是否有输入文件对应的结果文件，有则跳过')

    parser.add_argument('-p', '--processes',  type=int, default=1,
                        help='并行进程数 (默认: CPU核心数, 1=串行)')

    # 根据是否并行选择执行函数
    def count_main_wrapper(args):
        if getattr(args, 'processes', None) != 1:  # 不是明确设置为1
            return count_main_parallel(args)
        else:
            return count_main(args)  # 原有的串行版本

    # 关键修复：设置处理函数，而不是直接解析参数
    parser.set_defaults(func=count_main)


def main():
    """主函数 - 用于直接运行此脚本"""
    parser = argparse.ArgumentParser(
        description='fansetools count - Process fanse3 files for read counting'
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

            # # 显示一些统计信息
            # for count_type, counter_obj in counts_data.items():
            #     if counter_obj:  # 只显示非空的计数器
            #         print(f"{count_type}: {len(counter_obj)} 个转录本")
            #         # 显示前5个最高计数的转录本
            #         top5 = counter_obj.most_common(5)
            #         print(f"  前5个转录本: {top5}")

            print("\n开始生成isoform水平计数...")
            # 正确调用：传递参数
            counter.generate_isoform_level_counts(counts_data, total_count)

            print("开始基因水平聚合...")
            counter.gene_level_counts_unique_genes, counter.gene_level_counts_multi_genes = counter.aggregate_gene_level_counts()

            # if gene_counts_unique_genes:
            #     print("\n基因水平计数统计:")
            #     for count_type, gene_counter in gene_counts_unique_genes.items():
            #         if gene_counter:  # 只显示非空的计数器
            #             print(f"{count_type}: {len(gene_counter)} 个基因")
            #             top5_genes = gene_counter.most_common(5)
            #             print(f"  前5个基因: {top5_genes}")

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

    # def debug_gene_aggregation():
    #     """调试基因聚合功能"""
    #     # 创建测试数据
    #     test_counts = {
    #         'firstID': Counter({
    #             'transcript1': 100,
    #             'transcript2': 50,
    #             'transcript3': 75,
    #             'transcript4': 25
    #         }),
    #         'multi': Counter({
    #             'transcript1,transcript2': 30,
    #             'transcript3,transcript4': 20
    #         })
    #     }

    #     test_annotation = pd.DataFrame({
    #         'txname': ['transcript1', 'transcript2', 'transcript3', 'transcript4'],
    #         'geneName': ['geneA', 'geneA', 'geneB', 'geneB']
    #     })

    #     # 手动测试聚合逻辑
    #     transcript_to_gene = dict(zip(test_annotation['txname'], test_annotation['geneName']))
    #     print("转录本到基因的映射:", transcript_to_gene)

    #     for count_type, counter in test_counts.items():
    #         print(f"\n处理 {count_type}:")
    #         gene_counter = Counter()

    #         for transcript_ids_str, count in counter.items():
    #             print(f"  处理 '{transcript_ids_str}': 计数={count}")

    #             if ',' in transcript_ids_str:
    #                 transcript_ids = transcript_ids_str.split(',')
    #                 print(f"    多映射转录本: {transcript_ids}")

    #                 gene_counts = {}
    #                 for tid in transcript_ids:
    #                     gene = transcript_to_gene.get(tid)
    #                     if gene:
    #                         gene_counts[gene] = gene_counts.get(gene, 0) + 1

    #                 print(f"    基因分布: {gene_counts}")

    #                 if gene_counts:
    #                     for gene, gene_count in gene_counts.items():
    #                         allocation = count * (gene_count / len(transcript_ids))
    #                         gene_counter[gene] += allocation
    #                         print(f"    分配给基因 {gene}: {allocation}")
    #             else:
    #                 gene = transcript_to_gene.get(transcript_ids_str)
    #                 if gene:
    #                     gene_counter[gene] += count
    #                     print(f"    单映射: 基因 {gene} 增加 {count}")

    #         print(f"  最终基因计数: {dict(gene_counter)}")

    # if __name__ == '__main__':
    #     # 运行测试
    #     print("=" * 60)
    #     print("开始测试基因水平计数功能")
    #     print("=" * 60)

    #     # 先运行调试
    #     debug_gene_aggregation()

    #     print("\n" + "=" * 60)
    #     print("开始完整测试")
    #     print("=" * 60)

    #     # 运行完整测试
    #     test_gene_level_counting()
