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
如果想实现这个，可能得转换坐标，将基因组坐标转换为转录组坐标，refflat文件中对应的坐标都进行更改，全部都减去第一个start来转换。
长度需要有多个，
"""

import os
import sys
import argparse
import pandas as pd
import glob
from collections import Counter,defaultdict
from tqdm import tqdm
import time
from pathlib import Path
# import defaultdict

# 导入新的路径处理器
from fansetools.utils.path_utils import PathProcessor
# 导入新的fanse_parser
from fansetools.parser import fanse_parser, FANSeRecord,fanse_parser_high_performance
from fansetools.gxf2refflat_plus import convert_gxf_to_refflat, load_annotation_to_dataframe


# 在您的FanseCounter类中添加并行处理方法
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm.contrib.concurrent import process_map

class ParallelFanseCounter:
    """并行处理多个fanse3文件的计数器"""
    
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or min(mp.cpu_count(), 8)  # 限制最大进程数
        print(f"初始化并行处理器: {self.max_workers} 个进程")
    
    def process_files_parallel(self, file_list, output_base_dir, gxf_file=None, level='gene', paired_end=None, annotation_df=None):
        """并行处理多个文件 - 修复版本"""
        print(f"🎯 开始并行处理 {len(file_list)} 个文件，使用 {self.max_workers} 个进程")
        
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
                future = executor.submit(self._process_single_file, task, annotation_df)
                future_to_task[future] = task
            
            # 使用tqdm显示总体进度
            with tqdm(total=len(tasks), desc="总体进度", position=0) as pbar:
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        results.append((task['input_file'], True, result))
                        pbar.set_description(f"✅ 完成: {task['file_stem']}")
                    except Exception as e:
                        results.append((task['input_file'], False, str(e)))
                        pbar.set_description(f"❌ 失败: {task['file_stem']}")
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
    """支持并行的主函数 - 修复版本"""
    print_mini_fansetools()
    processor = PathProcessor()
    
    try:
        # 1. 解析输入文件
        input_files = processor.parse_input_paths(args.input, ['.fanse','.fanse3', '.fanse3.gz', '.fanse.gz'])
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
        output_dir = Path(args.output) if args.output else Path.cwd() / "fansetools_results"
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
                output_files_to_check.append(individual_output_dir / f"{file_stem}_isoform_level.counts.csv")
            if args.level in ['gene', 'both'] and args.gxf:
                output_files_to_check.append(individual_output_dir / f"{file_stem}_gene_level.counts.csv")
            
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
        
        print(f"断点续传: 跳过 {skipped_files} 个文件，剩余 {len(files_to_process)} 个文件待处理")
        
        # 5. 并行处理
        max_workers = args.processes if hasattr(args, 'processes') and args.processes > 1 else min(mp.cpu_count(), len(files_to_process))
        
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
        print("📊 处理结果摘要")
        print("=" * 60)
        
        success_count = sum(1 for _, success, _ in results if success)
        failed_count = len(results) - success_count
        
        print(f"✅ 成功: {success_count} 个文件")
        print(f"❌ 失败: {failed_count} 个文件")
        print(f"⏱️  总耗时: {duration:.2f} 秒")
        
        if failed_count > 0:
            print("\n失败详情:")
            for input_file, success, result in results:
                if not success:
                    print(f"  - {Path(input_file).name}: {result}")
        
        print(f"\n🎉 处理完成! 结果保存在: {output_dir}")
        
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
        input_files = processor.parse_input_paths(args.input, ['.fanse','.fanse3', '.fanse3.gz', '.fanse.gz'])       
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
        output_map = processor.generate_output_mapping(input_files, args.output, '.counts.csv')

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
                    output_files_to_check.append(output_dir / f"{input_stem}_isoform_level.counts.csv")
                if args.level in ['gene', 'both']:
                    output_files_to_check.append(output_dir / f"{input_stem}_gene_level.counts.csv")
                    output_files_to_check.append(output_dir / f"{input_stem}_multi_genes_level.counts.csv")
                
                all_files_exist = all(f.exists() for f in output_files_to_check)
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
            print(f"\n[{i + skipped_files}/{len(input_files)}] 处理: {input_file.name}")
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
                print("✅ 完成")
            except Exception as e:
                print(f"❌ 处理失败: {str(e)}")
        
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
        
        # 存储计数结果
        self.counts_data = {}
        self.summary_stats = {}
        # self.multi_mapping_info = defaultdict(list)  # 存储多映射信息
        
    def judge_sequence_mode(self):
        """判断测序模式（单端/双端）"""
        if self.paired_end and os.path.isfile(self.paired_end):
            print('Pair-End mode detected.')
            return True
        else:
            print('Single-End mode detected.')
            return False
      
 

    def parse_fanse_file(self):
        """
        专门负责解析fanse3文件，直接进行基本计数

            'raw': Counter(),     #unique和multi都包括在内，全部
            'multi': Counter(),   #只保存multi id的
            'unique': Counter(),  #只保存unique ID 的
            'firstID': Counter(), #只保存raw中第一个ID，multi只取第一个ID来进行统计
            'multi2all': Counter(), #multi中的每一个ID，统计时候都+1
            'multi_equal': Counter(),  # 预先初始化，后面填充。multi中的每一个ID都有，统计时候平均分配count
            'multi_EM': Counter(),     # 预先初始化，后面填充。multi中的每一个ID，仅具有unique read的统计时候有权重分配比例，按比例分配。没有unique 的不分配。
            'multi_EM_cannot_allocate_tpm': multi 中的所有ID，均没有unique reads的部分。
        
        """
        
        # 选择优化版本
        if self.input_file.stat().st_size > 1024 * 1024 * 1024:  # 大于1024 MB
            fanse_parser = fanse_parser_high_performance
        else:
            fanse_parser = fanse_parser
    
    
        print(f'Parsing {self.input_file.name}')
        start_time = time.time()
        
        # 初始化所有计数器
        counts_data = {
            'raw': Counter(),     #unique和multi都包括在内，全部
            'multi': Counter(),   #只保存multi id的
            'unique': Counter(),  #只保存unique ID 的
            'firstID': Counter(), #只保存raw中第一个ID，multi只取第一个ID来进行统计
            'multi2all': Counter(), #multi中的每一个ID，统计时候都+1
            'multi_equal': Counter(),  # 预先初始化，后面填充。multi中的每一个ID都有，统计时候平均分配count
            'multi_EM': Counter(),     # 预先初始化，后面填充。multi中的每一个ID，仅具有unique read的统计时候有权重分配比例，按比例分配。没有unique 的不分配。
            'multi_EM_cannot_allocate_tpm': Counter(), 
            'counts_em': Counter(),    #合并raw和multi_em
            'counts_eq': Counter(),     #合并raw和multi_equal           
        }
        
        total_count = 0
        
        files_to_process = [self.input_file]
        if self.paired_end:
            files_to_process.append(Path(self.paired_end))
        
        for fanse_file in files_to_process:
            if not fanse_file.exists():
                continue
                
            # print(f'Reading {fanse_file.name}')
            try:
                file_size = fanse_file.stat().st_size
                estimated_records = max(1, file_size // 527)
                
                with tqdm(total=estimated_records, unit='reads', mininterval=5, unit_scale=True) as pbar:
                    for record in fanse_parser(str(fanse_file)):
                        if record.ref_names:
                            transcript_ids = record.ref_names
                            is_multi = record.is_multi
                            
                            # 直接更新计数器
                            raw_id = transcript_ids[0] if len(transcript_ids) == 1 else ','.join(transcript_ids)
                            #不管神么样，raw 都要统计到位
                            counts_data['raw'][raw_id] += 1
                            
                            #firstID只取第一个ID，其他的ID舍弃，不论是否multi，因此放在这里足够了
                            counts_data['firstID'][transcript_ids[0]] += 1
                            
                            if is_multi:
                                #multi的以字符串形式加入multi 统计
                                counts_data['multi'][raw_id] += 1
                                
                                #每一个multiID的成员都给multi2all 贡献一个点
                                for tid in transcript_ids:
                                    counts_data['multi2all'][tid] += 1
                            
                            else:   #unique reads 部分这里是
                                # tid = transcript_ids[0]    #不用重新赋值了，直接用上面raw_id即可
                                counts_data['unique'][raw_id] += 1
                                
                                # counts_data['firstID'][raw_id] += 1
                            
                            total_count += 1
                            pbar.update(1)
                            
            except Exception as e:
                print(f"Error parsing file {fanse_file}: {str(e)}")
                continue
        
        parsing_time = time.time() - start_time
        print(f"Parsing completed in {parsing_time:.2f} seconds, {total_count} records")
        
        return counts_data, total_count



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
                record_count = sum(1 for _ in fanse_parser(str(file_path)))
                print(f"直接计数完成: {record_count} 条记录")
                return record_count
            except:
                pass
        
        # 对于大文件，使用采样估算
        avg_size = self.calculate_average_record_size(file_path, sample_size)-50   #经验减去50字节，人为增大一点估算的reads总数，反而比较符合实际
        estimated_records = max(1, int(file_size / avg_size))
        
        print(f"文件大小: {file_size} 字节")
        print(f"平均记录大小: {avg_size:.1f} 字节")
        print(f"估计Fanse记录数: {estimated_records} 条")
        
        return estimated_records

    def parse_fanse_file_optimized_final(self, position=0):
        """综合优化版本"""
        print(f'Parsing {self.input_file.name}')
        start_time = time.time()
        
        # 预初始化数据结构
        counts_data = {
            'raw': Counter(), 'multi': Counter(), 'unique': Counter(),
            'firstID': Counter(), 'multi2all': Counter()
        }
        
        total_count = 0
        batch_size = 600000
        # update_interval = 10000
        
        # 使用局部变量加速
        raw, multi, unique, firstID, multi2all = (
            counts_data['raw'], counts_data['multi'], counts_data['unique'],
            counts_data['firstID'], counts_data['multi2all']
        )
        
        for position, fanse_file in enumerate([self.input_file] + ([Path(self.paired_end)] if self.paired_end else []) ):
            if not fanse_file.exists():
                continue
                
            try:
                batch = []
                # last_update = 0
                
                # file_size = fanse_file.stat().st_size
                # estimated_records = max(1, file_size // 527)
                # 智能估算记录数
                sample_size = 100000    #采样数目，用来估算总reads数
                estimated_records = self.calculate_file_record_estimate(fanse_file, sample_size)
                
                with tqdm(total=estimated_records, unit='reads', mininterval=5, unit_scale=True, position=position, leave=False) as pbar:
                    #进度条更新频率控制
                    update_interval = 1000
                    update_counter = 0
                    
                    for i, record in enumerate(fanse_parser(str(fanse_file))):
                        if record.ref_names:
                            total_count += 1
                            
                            # 批量处理
                            batch.append(record)
                            if len(batch) >= batch_size:
                                self._fast_batch_process(batch, raw, multi, unique, firstID, multi2all)
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
                        self._fast_batch_process(batch, raw, multi, unique, firstID, multi2all)
                    
            except Exception as e:
                print(f"Error: {e}")
                continue
        
        duration = time.time() - start_time
        print(f"Completed: {total_count} records in {duration:.2f}s ({total_count/duration:.0f} rec/sec)")
        
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




    def generate_isoform_level_counts(self, counts_data, total_count):
        """
        根据解析的计数数据生成isoform水平的各种计数
        """
        print("Generating isoform level counts...")
        start_time = time.time()
        
        # 第二阶段：高级多映射计数
        if counts_data['multi']:
            print("Starting advanced multi-mapping analysis...")
            self._process_advanced_multi_mapping(counts_data)
            print("Advanced multi-mapping analysis completed.")
        
        #第三阶段:计算正确的counts，合并raw和multi_em，以及multi_equal 的counts
        print("Starting third stage: merging counts...")

        # 初始化合并计数器
        counts_data['counts_em'] = Counter()
        counts_data['counts_eq'] = Counter()
        
        # 1. 合并 unique 和 multi_EM 计数 (counts_em)
        # 首先添加所有unique计数
        for transcript, count in counts_data['unique'].items():
            counts_data['counts_em'][transcript] += count
        
        # 然后添加multi_EM计数
        for transcript, count in counts_data['multi_EM'].items():
            counts_data['counts_em'][transcript] += count
        
        # 2. 合并 unique 和 multi_equal 计数 (counts_eq)
        # 首先添加所有unique计数
        for transcript, count in counts_data['unique'].items():
            counts_data['counts_eq'][transcript] += count
        
        # 然后添加multi_equal计数
        for transcript, count in counts_data['multi_equal'].items():
            counts_data['counts_eq'][transcript] += count
        
        # 3. 验证合并结果
        total_em = sum(counts_data['counts_em'].values())
        total_eq = sum(counts_data['counts_eq'].values())
        total_unique = sum(counts_data['unique'].values())
        total_multi_em = sum(counts_data['multi_EM'].values())
        total_multi_eq = sum(counts_data['multi_equal'].values())
        
        print("合并验证:")
        print(f"  - unique计数总计: {total_unique}")
        print(f"  - multi_EM计数总计: {total_multi_em}")
        print(f"  - multi_equal计数总计: {total_multi_eq}")
        print(f"  - counts_em总计: {total_em} (应为 {total_unique + total_multi_em})")
        print(f"  - counts_eq总计: {total_eq} (应为 {total_unique + total_multi_eq})")        
        
        # 更新实例变量
        self.counts_data = counts_data
        self.summary_stats = {
            'total_reads': total_count,
            'unique_mapped': sum(counts_data['unique'].values()),
            'multi_mapped': sum(counts_data['multi'].values()),
            'raw': sum(counts_data['raw'].values()),
            'firstID': sum(counts_data['firstID'].values()),
            'multi_equal': sum(counts_data['multi_equal'].values()),
            'multi_EM': sum(counts_data['multi_EM'].values()),
            'multi_EM_cannot_allocate_tpm': sum(counts_data['multi_EM_cannot_allocate_tpm'].values()),
            'counts_em': total_em,
            'counts_eq': total_eq,
            'processing_time': time.time() - start_time
        }
        
        print(f"Count generation completed in {self.summary_stats['processing_time']:.2f} seconds")
        print("最终计数统计:")
        print(f"  - counts_em: {len(counts_data['counts_em'])} 个转录本, {total_em} 条reads")
        print(f"  - counts_eq: {len(counts_data['counts_eq'])} 个转录本, {total_eq} 条reads")

    def _process_advanced_multi_mapping(self, counts_data):
        """完整的修复版：处理高级多映射计数
        multi部分，和unique部分是否有重叠？
         - 没有。unique是单独的reads，multi部分处理得到的reads可以和unique部分加和，才是最终应该的reads。
         - 新情形
             - integred_2all  =  unique + multi2all
             - integred_equal  =  unique + multi_equal
             - integred_em  =  unique + multi_em    
        """
        print("开始高级多映射分析...")
        
        if not counts_data['multi']:
            print("没有多映射数据，跳过高级分析")
            return
        
        # 获取转录本长度信息
        transcript_lengths = {}
        if self.annotation_df is not None:
            transcript_lengths = dict(zip(self.annotation_df['txname'], self.annotation_df['txLength']))
            print(f"加载了 {len(transcript_lengths)} 个转录本的长度信息")
        
        # 通过unique部分计算TPM，因此只有具有unique read的转录本才计入，**会丢掉没有unique reads的部分/完全重叠转录本。
        tpm_values = self._calculate_tpm(counts_data['unique'], transcript_lengths)
        print(f"计算了 {len(tpm_values)} 个转录本的TPM值")
        
        # 初始化计数器
        multi_equal_counter = Counter()
        multi_em_counter = Counter()
        multi_em_cannot_allocate_tpm_counter  = Counter()
        
        #multi部分的总记录数累计
        processed_events = 0
        
        for transcript_ids_str, event_count in counts_data['multi'].items():
            transcript_ids = transcript_ids_str.split(',')
            
        # multi_equal: 平均分配, 不论是否具有unique reads，后果，有部分基因原本没表达，强行安排。假阳性（比例估计？）
            equal_share_per_read = 1.0 / len(transcript_ids)
            for tid in transcript_ids:
                multi_equal_counter[tid] += event_count * equal_share_per_read
            
        # multi_EM: 按TPM比例分配，只有具有unique reads的才参与分配；没有的暂时另存一个columns，可考虑平均分配，作为参考。这部分可能是序列高度重叠的基因，但是无法区分，也不能完全认为基因不表达。
            allocation = self._allocate_by_tpm(transcript_ids, tpm_values)
            if allocation:
                for tid, share_ratio in allocation.items():
                    multi_em_counter[tid] += event_count * share_ratio
            else: 
        #allocation为None的情况，即无法通过tpm分配的无unique reads的部分
                multi_em_cannot_allocate_tpm_counter[transcript_ids_str] = event_count
                
            processed_events += 1
            if processed_events % 10000 == 0:
                print(f"已处理 {processed_events} 个多映射事件")
        
        # 更新计数器
        counts_data['multi_equal'] = multi_equal_counter
        counts_data['multi_EM'] = multi_em_counter
        counts_data['multi_EM_cannot_allocate_tpm'] = multi_em_cannot_allocate_tpm_counter
        
        print( "高级多映射分析完成：")
        print(f"  - multi_equal: {len(multi_equal_counter)} 个转录本")
        print(f"  - multi_EM: {len(multi_em_counter)} 个转录本")

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
                total_rpk += rpk   #计算总rpk
        
        # 计算TPM (Transcripts Per Million)
        tpm_values = {}
        if total_rpk > 0:
            scaling_factor = 1e6 / total_rpk
            for transcript, rpk in rpk_values.items():
                tpm_values[transcript] = rpk * scaling_factor
        
        return tpm_values

    def _allocate_by_tpm(self, transcript_ids, tpm_values):
        """根据unique 计算的  TPM值分配多映射reads"""
        allocation = {}
        
        # 过滤掉没有TPM值的转录本
        valid_transcripts = [tid for tid in transcript_ids if tid in tpm_values and tpm_values[tid] > 0]
        
        if not valid_transcripts:
            # 回退到平均分配，，，这个有点不太合适，可以放在另一个表格multi_EM_cannot_allocate_tpm里，暂时不参与分配  20251111
            return None
            # share = 1.0 / len(transcript_ids)
            # return  {tid: share for tid in transcript_ids}
        
        # 计算总TPM
        total_tpm = sum(tpm_values[tid] for tid in valid_transcripts)
        
        # 按TPM比例分配
        for tid in valid_transcripts:
            allocation[tid] = tpm_values[tid] / total_tpm
        
        # 处理不在valid_transcripts中的转录本
        invalid_transcripts = [tid for tid in transcript_ids if tid not in valid_transcripts]
        if invalid_transcripts and total_tpm > 0:
            remaining_share = 1.0 - sum(allocation.values())
            if remaining_share > 0:
                share_per_invalid = remaining_share / len(invalid_transcripts)
                for tid in invalid_transcripts:
                    allocation[tid] = share_per_invalid
        
        return allocation

    def aggregate_gene_level_counts(self):
        """
        基因水平计数聚合
        
        仅仅是针对isoform中的各种类型的组合中，，只做转换，和count加到一起，其他没了
            - 够不够用？ 
            - 行不行？
            - 应该后面怎么加一些才对？
                - 需要加到一起的，加到一起
        """
        if self.annotation_df is None:
            print("Warning: Cannot aggregate gene level counts without annotation data")
            return {}, {}
        
        print("Aggregating gene level counts...")
        start_time = time.time()
        
        # 创建转录本到基因的映射
        transcript_to_gene = dict(zip(self.annotation_df['txname'], self.annotation_df['geneName']))
        gene_level_counts_unique_genes = {}
        gene_level_counts_multi_genes = {}
        
        # 初始化所有基因计数类型
        for count_type in self.counts_data.keys():
            gene_level_counts_unique_genes[count_type] = Counter()
            gene_level_counts_multi_genes[count_type] = Counter()
       
        # 单独处理多基因组合
        # gene_level_counts_unique_genes['multi_genes'] = Counter()
        
        for count_type, counter in self.counts_data.items():
            gene_counter_unique = gene_level_counts_unique_genes[count_type]
            gene_counter_multi = gene_level_counts_multi_genes[count_type]
            
            for transcript_ids_str, event_count in counter.items():
                # 处理转录本ID（可能是单个或多个）
                if ',' not in transcript_ids_str:
                    # 单映射情况
                    gene = transcript_to_gene.get(transcript_ids_str)
                    if gene:   #因为只有一个gene ID，所以直接加count即可
                        gene_counter_unique[gene] +=  event_count
                
                else:
                    # 多映射情况：检查是否映射到同一个基因
                    transcript_ids = transcript_ids_str.split(',')
                    genes = set()   #如果是同一个基因，则只会保留一个GENE ID
                    
                    for tid in transcript_ids:
                        gene = transcript_to_gene.get(tid)
                        if gene:
                            genes.add(gene)
                    
                    if len(genes) == 1:
                        # 映射到同一个基因
                        gene = list(genes)[0]
                        gene_counter_unique[gene] +=  event_count
                    elif len(genes) > 1:
                        # 映射到多个基因
                        gene_key = ','.join(sorted(genes))
                        gene_counter_multi[gene_key] +=  event_count   #实际还是gene_level_counts_unique_genes['multi_genes']，因此不用单独返回，已经包括
        
        processing_time = time.time() - start_time
        print(f"Gene level aggregation completed in {processing_time:.2f} seconds")
        
        #下一阶段聚合，生成正确的counts
        # 初始化合并计数器
        gene_level_counts_unique_genes['counts_em'] = Counter()
        gene_level_counts_unique_genes['counts_eq'] = Counter()
        #gene_level_counts_multi_genes
        
        # 1. 合并 unique 和 multi_EM 计数 (counts_em)
        # 首先添加所有unique raw计数
        for transcript, count in gene_level_counts_unique_genes['raw'].items():
            gene_level_counts_unique_genes['counts_em'][transcript] += count
        
        # 然后添加multi_EM计数
        for transcript, count in gene_level_counts_unique_genes['multi_EM'].items():
            gene_level_counts_unique_genes['counts_em'][transcript] += count
        
        # 2. 合并 unique 和 multi_equal 计数 (counts_eq)
        # 首先添加所有unique raw计数
        for transcript, count in gene_level_counts_unique_genes['raw'].items():
            gene_level_counts_unique_genes['counts_eq'][transcript] += count
        
        # 然后添加multi_equal计数
        for transcript, count in gene_level_counts_unique_genes['multi_equal'].items():
            gene_level_counts_unique_genes['counts_eq'][transcript] += count
        
        
        return gene_level_counts_unique_genes, gene_level_counts_multi_genes

   
    def _generate_isoform_level_files(self, base_name):
        """生成转录本水平计数文件"""
        isoform_files = {}
        
        # 生成合并的转录本计数文件
        combined_df = pd.DataFrame(self.counts_data['firstID'].items(), 
                                 columns=['Transcript', 'firstID'])
        
        # 合并所有计数类型
        for count_type in ['raw', 'unique', 'multi', 'multi2all', 'multi_EM', 'multi_equal','counts_em','counts_eq']:
            if count_type in self.counts_data:
                temp_df = pd.DataFrame(self.counts_data[count_type].items(),
                                    columns=['Transcript', f'{count_type}_count'])
                combined_df = combined_df.merge(temp_df, on='Transcript', how='outer')
        
        # 添加注释信息（如果有）
        if self.annotation_df is not None:
            annotation_subset = self.annotation_df[['txname', 'geneName', 'txLength', 'cdsLength',]]
            combined_df = combined_df.merge(
                annotation_subset, 
                left_on='Transcript', 
                right_on='txname', 
                how='left'
            ).drop('txname', axis=1)
        
        combined_filename = self.output_dir / f'{base_name}_isoform_level.counts.csv'
        combined_df.to_csv(combined_filename, index=False, float_format='%.2f')
        isoform_files['isoform'] = combined_filename
        
        return isoform_files

   

    #20251111
    def _generate_gene_level_files(self, base_name):
        """生成基因水平计数文件 - 根据新的返回结构修改"""
        if self.annotation_df is None:
            print("没有注释信息，跳过基因水平文件生成")
            return {} 
        # 检查实际的列名
        print(f"Annotation DataFrame 列名: {list(self.annotation_df.columns)}")
        
        if not hasattr(self, 'gene_level_counts_unique_genes') or not self.gene_level_counts_unique_genes:
            print("Warning: No gene level counts available")
            return {}
        
        gene_files = {}
        
        # 生成单个基因的计数文件（来自gene_level_counts_unique_genes）
        single_gene_df = pd.DataFrame(self.gene_level_counts_unique_genes['firstID'].items(), 
                                     columns=['Gene', 'firstID_count'])
        
        # 合并所有计数类型（单个基因）
        for count_type in ['raw', 'unique', 'multi', 'multi2all', 'multi_EM', 'multi_equal','counts_em','counts_eq']:
            if count_type in self.gene_level_counts_unique_genes:
                temp_df = pd.DataFrame(self.gene_level_counts_unique_genes[count_type].items(),
                                    columns=['Gene', f'{count_type}_count'])
                single_gene_df = single_gene_df.merge(temp_df, on='Gene', how='outer')
        
        # 生成多基因组合的计数文件（来自gene_level_counts_multi_genes）
        if hasattr(self, 'gene_level_counts_multi_genes') and self.gene_level_counts_multi_genes:
            # 首先检查是否有任何多基因计数数据
            has_multi_data = False
            for count_type, counter in self.gene_level_counts_multi_genes.items():
                if counter:  # 检查计数器是否非空
                    has_multi_data = True
                    break
            
            if has_multi_data:
                # 使用firstID作为基础（如果没有firstID，使用第一个可用的计数类型）
                base_count_type = None
                for count_type in ['firstID', 'raw', 'unique', 'multi']:
                    if count_type in self.gene_level_counts_multi_genes and self.gene_level_counts_multi_genes[count_type]:
                        base_count_type = count_type
                        break
                
                if base_count_type:
                    multi_genes_df = pd.DataFrame(self.gene_level_counts_multi_genes[base_count_type].items(),
                                                columns=['Gene_Combination', 'firstID_count'])
                    
                    # 合并其他计数类型（多基因组合）
                    for count_type in ['raw', 'unique', 'multi', 'multi2all', 'multi_EM', 'multi_equal']:
                        if count_type in self.gene_level_counts_multi_genes and self.gene_level_counts_multi_genes[count_type]:
                            temp_df = pd.DataFrame(self.gene_level_counts_multi_genes[count_type].items(),
                                                columns=['Gene_Combination', f'{count_type}_count'])
                            multi_genes_df = multi_genes_df.merge(temp_df, on='Gene_Combination', how='outer')
                else:
                    # 如果没有基础计数类型，创建一个空的DataFrame
                    multi_genes_df = pd.DataFrame(columns=['Gene_Combination', 'firstID_count'])
            else:
                multi_genes_df = None
        else:
            multi_genes_df = None
        
        
        # # 添加基因注释信息和转录本信息
        # if self.annotation_df is not None:
        #     # 获取基因到转录本的映射
        #     gene_to_transcripts = defaultdict(list)
        #     for _, row in self.annotation_df.iterrows():
        #         # gene_to_transcripts[row['geneName']].append(row['txname'])
        #         gene_name = row.get('geneName', row.get('gene_name', ''))  # 处理不同的列名
        #         tx_name = row.get('txname', row.get('transcript_id', ''))  # 处理不同的列名
        #         if gene_name and tx_name:
        #             gene_to_transcripts[gene_name].append(tx_name)
            
        #     # 为单个基因文件添加转录本信息
        #     single_gene_df['Transcripts'] = single_gene_df['Gene'].map(
        #         lambda x: ','.join(gene_to_transcripts.get(x, [])) if x in gene_to_transcripts else ''
        #     )
        #     single_gene_df['Transcript_Count'] = single_gene_df['Gene'].map(
        #         lambda x: len(gene_to_transcripts.get(x, []))
        #     )
            
        #     # 为多基因组合文件添加转录本信息
        #     if multi_genes_df is not None and not multi_genes_df.empty:
        #         multi_genes_df['Transcripts'] = multi_genes_df['Gene_Combination'].map(
        #             lambda x: ','.join([','.join(gene_to_transcripts.get(g, [])) for g in x.split(',')])
        #         )
        #         multi_genes_df['Transcript_Count'] = multi_genes_df['Gene_Combination'].map(
        #             lambda x: sum(len(gene_to_transcripts.get(g, [])) for g in x.split(','))
        #         )
        
        # # 添加其他基因注释信息
        # if self.annotation_df is not None:
        #     gene_annotation = self.annotation_df[['geneName', 'symbol', 'txLength', 'cdsLength','genelongesttxLength','genelongestcdsLength']].drop_duplicates()
        #     # gene_annotation = annotation_df[['geneName', 'symbol', 'txLength', 'cdsLength']].drop_duplicates()
        #     gene_annotation = gene_annotation.groupby('geneName').agg({
        #         # 'symbol': 'first',
        #         'txLength': 'max',
        #         'cdsLength': 'max'
        #     }).reset_index()
            
        #     single_gene_df = single_gene_df.merge(
        #         gene_annotation, 
        #         left_on='Gene', 
        #         right_on='geneName', 
        #         how='left'
        #     ).drop('geneName', axis=1)
        
        # 添加基因注释信息和转录本信息
        if self.annotation_df is not None:
            # 获取基因到转录本的映射
            gene_to_transcripts = defaultdict(list)
            for _, row in self.annotation_df.iterrows():
                # 处理不同的列名可能性
                gene_name = row.get('geneName', row.get('gene_name', ''))
                tx_name = row.get('txname', row.get('transcript_id', ''))
                if gene_name and tx_name:
                    gene_to_transcripts[gene_name].append(tx_name)
            
            # 为单个基因文件添加转录本信息
            single_gene_df['Transcripts'] = single_gene_df['Gene'].map(
                lambda x: ','.join(gene_to_transcripts.get(x, [])) if x in gene_to_transcripts else ''
            )
            single_gene_df['Transcript_Count'] = single_gene_df['Gene'].map(
                lambda x: len(gene_to_transcripts.get(x, []))
            )
            
            # 修复：动态检查并选择可用的列
            available_columns = self.annotation_df.columns.tolist()
            
            # 检查并选择基因注释列
            gene_annotation_cols = ['geneName']
            symbol_cols = ['symbol', 'genename', 'gene_name']
            txlength_cols = ['genelongesttxLength', 'genelonesttxlength', 'txLength']
            cdslength_cols = ['genelongestcdsLength', 'genelongestcdslength', 'cdsLength']
            
            # 选择实际存在的列
            selected_cols = ['geneName']
            
            # 添加symbol列（如果存在）
            for col in symbol_cols:
                if col in available_columns:
                    selected_cols.append(col)
                    break
            
            # 添加txLength列（如果存在）
            for col in txlength_cols:
                if col in available_columns:
                    selected_cols.append(col)
                    break
            
            # 添加cdsLength列（如果存在）
            for col in cdslength_cols:
                if col in available_columns:
                    selected_cols.append(col)
                    break
            
            print(f"使用的基因注释列: {selected_cols}")
            
            # 去重并合并
            gene_annotation = self.annotation_df[selected_cols].drop_duplicates()
            
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
            
            # 合并到结果数据框
            single_gene_df = single_gene_df.merge(
                gene_annotation, 
                left_on='Gene', 
                right_on='geneName', 
                how='left'
            )
            
            # 移除重复的geneName列（如果存在）
            if 'geneName' in single_gene_df.columns and 'Gene' in single_gene_df.columns:
                single_gene_df = single_gene_df.drop('geneName', axis=1)        
        
        
        # 保存unique genes文件
        single_gene_filename = self.output_dir / f'{base_name}_gene_level.counts.csv'
        single_gene_df.to_csv(single_gene_filename, index=False, float_format='%.2f')
        gene_files['gene'] = single_gene_filename
        
        # 保存multi genes组合文件（如果有数据）
        if multi_genes_df is not None and not multi_genes_df.empty:
            multi_genes_filename = self.output_dir / f'{base_name}_multi_genes_level.counts.csv'
            multi_genes_df.to_csv(multi_genes_filename, index=False, float_format='%.2f')
            gene_files['multi_genes'] = multi_genes_filename
        
        return gene_files
  
    def _generate_multi_mapping_file(self, base_name):
        """生成多映射信息文件"""
        if not self.multi_mapping_info:
            return None
        
        # 创建多映射信息数据框
        multi_data = []
        for transcript_ids, read_names in self.multi_mapping_info.items():
            multi_data.append({
                'transcript_ids': transcript_ids,
                'read_count': len(read_names),
                'read_names': ';'.join(read_names)  
            })
        
        multi_df = pd.DataFrame(multi_data)
        multi_filename = self.output_dir / f'{base_name}_multi_mapping_info.csv'
        multi_df.to_csv(multi_filename, index=False)
        
        return multi_filename

 
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
                print(" isoform 水平计数文件生成完成")
            except Exception as e:
                print(f"转录本水平计数文件生成失败: {e}")
        
        
        # 生成基因水平计数文件
        if self.annotation_df is not None and self.level in ['gene', 'both']:
            try:
                # # 更健壮的条件检查
                if (hasattr(self, 'gene_level_counts_unique_genes') and \
                    self.gene_level_counts_unique_genes):
     
                    gene_files = self._generate_gene_level_files(base_name)
                    count_files.update(gene_files)
                    print("基因水平计数文件生成完成")
                else:
                    print("没有基因水平计数数据，跳过基因水平文件生成")
            except Exception as e:
                print(f" 基因水平计数文件生成失败: {e}")
        
        #     # # 生成多映射信息文件
        #     # multi_mapping_file = self._generate_multi_mapping_file(base_name)
        #     # if multi_mapping_file:
        #     #     count_files['multi_mapping'] = multi_mapping_file
        
        return count_files

    
    # def filter_by_minreads(self, minreads=None):
    #     """根据最小reads数过滤"""
    #     if minreads is None:
    #         minreads = self.minreads
        
    #     if minreads > 0:
    #         filtered_counts = {
    #             k: Counter({acc: count for acc, count in v.items() if count >= minreads})
    #             for k, v in self.counts_data.items()
    #         }
            
    #         base_name = self.input_file.stem
    #         for count_type, counter in filtered_counts.items():
    #             df = pd.DataFrame(counter.items(), columns=['Accession', 'count'])
    #             filename = self.output_dir / f'{base_name}_{count_type}_min{minreads}.csv'
    #             df.to_csv(filename, index=False)
            
    #         print(f"Filtered by minreads {minreads}, remaining genes: {len(filtered_counts['normal'])}")
    
    
    def run(self):
        """运行完整的计数流程"""
        print("=" * 60)
        print("fansetools count - Starting processing")
        print("=" * 60)
        
        # 1. 解析fanse3文件并直接获得计数
        counts_data, total_count = self.parse_fanse_file_optimized_final()
        # counts_data, total_count = self.parse_fanse_file()
        
        # 2. 生成isoform水平计数
        self.generate_isoform_level_counts(counts_data, total_count)
        
        # 3. 生成基因水平计数
        if self.annotation_df is not None:
            # self.gene_counts = self.aggregate_gene_level_counts()
            self.gene_level_counts_unique_genes, self.gene_level_counts_multi_genes  = self.aggregate_gene_level_counts()
            if self.gene_level_counts_unique_genes:
                print(f"Gene level aggregation completed: {len(self.gene_level_counts_unique_genes)} unique-gene count types")
            if self.gene_level_counts_multi_genes:
                print(f"Gene level aggregation completed: {len(self.gene_level_counts_multi_genes)} multi-gene count types")
        else:
            print("No annotation provided, skipping gene level aggregation")
            # self.gene_counts = {}
            self.gene_level_counts_unique_genes = {}
            self.gene_level_counts_multi_genes  = {}
        # 4. 生成计数文件
        count_files = self.generate_count_files()
        
        # 5. 可选过滤reads数目，此处不建议
        # if self.minreads > 0:
        #     self.filter_by_minreads()
        
        # 6. 生成摘要报告
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
            f.write(f"Processing mode: {'Paired-end' if self.paired_end else 'Single-end'}\n")
            f.write(f"Level parameter: {self.level}\n")
            f.write(f"Annotation provided: {self.annotation_df is not None}\n")
            
            if self.annotation_df is not None:
                f.write(f"Annotation transcripts: {len(self.annotation_df)}\n")
                f.write(f"Annotation genes: {self.annotation_df['geneName'].nunique()}\n")
            
            f.write("\nStatistics:\n")
            for stat, value in self.summary_stats.items():
                f.write(f"{stat}: {value}\n")
            
            f.write(f"\nMulti-mapping statistics:\n")
            f.write(f"Multi-mapping events: {len(self.counts_data['multi'])}\n")
            if self.counts_data['multi']:
                total_multi_reads = sum(self.counts_data['multi'].values())
                avg_reads_per_event = total_multi_reads / len(self.counts_data['multi'])
                f.write(f"Total multi-mapped reads: {total_multi_reads}\n")
                f.write(f"Average reads per multi-mapping event: {avg_reads_per_event:.2f}\n")


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
    
    mini_art =  ['''
     FANSeTools - Functional ANnotation SEquence Tools
     ''']
    
    for line in mini_art:
        print(line)
        
# def count_main(args):
#     """使用新路径处理器的count主函数"""
#     #打印个logo
#     print_mini_fansetools()
    
#     processor = PathProcessor()
    
#     try:
#         # 1. 解析输入路径
#         input_files = processor.parse_input_paths(args.input, ['.fanse','.fanse3', '.fanse3.gz', '.fanse.gz'])
#         # input_files = processor.parse_input_paths(input_file, ['.fanse','.fanse3', '.fanse3.gz', '.fanse.gz'])        
#         if not input_files:
#             print("错误: 未找到有效的输入文件")
#             sys.exit(1)
            
#         # 2. 加载注释文件
#         annotation_df = None
#         if args.gxf:
#             annotation_df = load_annotation_data(args)
#             if annotation_df is None:
#                 print("错误: 无法加载注释数据")
#                 sys.exit(1)  
#         else:
#             print("警告: 未提供注释文件，将只生成isoform水平计数")
            
#         # 3. 生成输出映射
#         output_map = processor.generate_output_mapping(input_files, args.output, '.counts.csv')

#         # 4. 断点续传: 检查已存在的输出文件
#         skipped_files = 0
#         if args.resume:
#             print("启用断点续传模式，检查已存在的输出文件...")
#             files_to_process = {}
            
#             for input_file, output_file in output_map.items():
#                 # 检查输出目录中是否已存在相应的结果文件
#                 output_dir = Path(output_file).parent
#                 input_stem = input_file.stem
                
#                 # 根据level参数检查相应的输出文件
#                 output_files_to_check = []
                
#                 if args.level in ['isoform', 'both']:
#                     output_files_to_check.append(output_dir / f"{input_stem}.counts_isoform_level.counts.csv")
                
#                 if args.level in ['gene', 'both']:
#                     output_files_to_check.append(output_dir / f"{input_stem}.counts_gene_level.counts.csv")
#                     output_files_to_check.append(output_dir / f"{input_stem}.counts_multi_genes_level.counts.csv")
                
#                     print(output_files_to_check)
#                 # 检查所有相关文件是否存在
#                 all_files_exist = any(f.exists() for f in output_files_to_check)
#                 print(all_files_exist)
#                 if all_files_exist:
#                     print(f"  跳过: {input_file.name} - 输出文件已存在")
#                     skipped_files += 1
#                 else:
#                     files_to_process[input_file] = output_file
            
#             output_map = files_to_process
#             print(f"断点续传: 跳过 {skipped_files} 个已处理的文件，剩余 {len(output_map)} 个文件待处理")
            
#             if not output_map:
#                 print("所有文件均已处理完成，无需继续运行；重新生成结果请另设文件夹，或删除已有结果")
#                 return
#         else:
#             print(f"找到 {len(input_files)} 个输入文件，开始处理...")
            
        
        
        
#         # 5. 验证路径
#         validation_checks = []
#         for input_file in input_files:
#             validation_checks.append((input_file, "输入文件", {'must_exist': True, 'must_be_file': True}))
        
#         is_valid, errors = processor.validate_paths(*validation_checks)
#         if not is_valid:
#             print("路径验证失败:")
#             for error in errors:
#                 print(f"  - {error}")
#             sys.exit(1)
        
#         # 4. 批量处理文件
#         # print(f"找到 {len(output_map)} / len(input_files)个输入文件，开始处理...")   #有多少个输出，就肯定有多少个输入，没错的，这样更合理
        
#         for i, (input_file, output_file) in enumerate(output_map.items(), 1):
#             print(f"\n[{ skipped_files + i }/{len(input_files)}] 处理: {input_file.name}")
#             print(f"  输出: {output_file}")
            
#             try:
#                 counter = FanseCounter(
#                     input_file=str(input_file),
#                     output_dir=str(output_file.parent),
#                     output_filename=output_file.name,                    
#                     # minreads=args.minreads,
#                     # rpkm=args.rpkm,
#                     gxf_file= args.gxf,
#                     level= args.level if annotation_df is not None else 'isoform',  # 没有注释时强制使用isoform水平
#                     paired_end= args.paired_end,
#                     annotation_df= annotation_df,  # 传递注释数据
#                 )
#                 count_files = counter.run()
#                 print(" 完成")
#                 print(f"生成文件: {list(count_files.keys())}")
#             except Exception as e:
#                 print(f"处理失败: {str(e)}")
        
#         print(f"\n处理完成: 总共 {len(input_files)} 个文件")
        
#     except Exception as e:
#         print(f"错误: {str(e)}")
#         sys.exit(1)


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
            print(f"Successfully loaded {len(annotation_df)} transcripts from existing refflat file")
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
        df = pd.read_csv(file_path, sep='\t', comment='#', header=None, names=columns)
    else:
        # 如果没有注释头部，使用默认列名
        default_columns = [
            "geneName", "txname", "chrom", "strand", "txStart", "txEnd",
            "cdsStart", "cdsEnd", "exonCount", "exonStarts", "exonEnds",
            "symbol", "g_biotype", "t_biotype", "description", "protein_id",
            "txLength", "cdsLength", "utr5Length", "utr3Length",
            "genelongesttxLength", "genelongestcdsLength", "geneEffectiveLength"
        ]
        df = pd.read_csv(file_path, sep='\t', header=None, names=default_columns)
    
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
    
    parser.add_argument('--gxf', required=False, help='Input GXF file (GTF or GFF3),if not provided, just give out isoform level readcounts')
    parser.add_argument('--annotation-output', help='Output refFlat file prefix (optional)')
    
    parser.add_argument('--level', choices=['gene', 'isoform', 'both'], default='gene',
                       help='Counting level')
    
    parser.add_argument('--resume', required=False, action='store_true', help='可从上次运行断掉的地方自动开始，自动检测文件夹中是否有输入文件对应的结果文件，有则跳过')

    parser.add_argument('--processes', '-p', type=int, default=1,
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
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
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
            fanse_file = r"\\fs2\D\DATA\Zhaojing\3.fanse3_result\old_s14\26.9311-Endosperm_RNC_R1_trimmed.fanse3"
            fanse_file = r'\\fs2\D\DATA\Zhaojing\3.fanse3_result\old_s14\16.9311-Root-RNC_R1_trimmed.fanse3'
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
            counts_data, total_count = counter.parse_fanse_file()
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
            gene_level_counts_unique_genes, gene_level_counts_multi_genes  = counter.aggregate_gene_level_counts()
            
            # if gene_counts_unique_genes:
            #     print("\n基因水平计数统计:")
            #     for count_type, gene_counter in gene_counts_unique_genes.items():
            #         if gene_counter:  # 只显示非空的计数器
            #             print(f"{count_type}: {len(gene_counter)} 个基因")
            #             top5_genes = gene_counter.most_common(5)
            #             print(f"  前5个基因: {top5_genes}")
            
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
                    print(f"  转录本: {transcript_ids}, reads数: {len(read_names)}")
            
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
