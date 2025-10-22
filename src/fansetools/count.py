#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fansetools count 组件 - 用于处理fanse3文件的read计数
"""

import os
import sys
import argparse
import pandas as pd
import glob
from collections import Counter
from tqdm import tqdm
import time
from pathlib import Path

# 导入新的路径处理器
from .utils.path_utils import PathProcessor
# 导入新的fanse_parser
from .parser import fanse_parser, FANSeRecord

class FanseCounter:
    """fanse3文件计数处理器"""
    
    def __init__(self, input_file, output_dir, level='isoform', minreads=0, rpkm=0, 
                 gtf_file=None,  paired_end=None, output_filename=None):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.level = level
        self.minreads = minreads
        self.rpkm = rpkm
        self.gtf_file = gtf_file
        self.paired_end = paired_end
        self.output_filename = output_filename  # 新增：支持自定义输出文件名
        
        # 存储计数结果
        self.counts_data = {}
        self.summary_stats = {}
    
    def judge_sequence_mode(self):
        """判断测序模式（单端/双端）"""
        if self.paired_end and os.path.isfile(self.paired_end):
            print('Pair-End mode detected.')
            return True
        else:
            print('Single-End mode detected.')
            return False
    
    def parse_fanse_file(self):
        """解析fanse3文件并计数"""
        print(f'Processing {self.input_file.name}')
        start_time = time.time()
        
        # 存储计数列表
        list_unique_mapping = []
        list_multi_mapping = []
        list_firstID = []
        list_raw = []
        list_multi2single = []
        
        total_count = 0
        # block_size = 1024 * 1024 * 512  # 512M块大小
        
        files_to_process = [self.input_file]
        if self.paired_end:
            files_to_process.append(Path(self.paired_end))
        
        for fanse_file in files_to_process:
            if not fanse_file.exists():
                continue
                
            print(f'Reading {fanse_file.name}')
            try:
                file_read_size = os.path.getsize(fanse_file)/500    #粗略估计500字节一个fanse记录
                with tqdm(total=file_read_size, unit='reads', mininterval=10, unit_scale=True) as pbar:
                    for record in fanse_parser(str(fanse_file)):
                        # 处理每个记录
                        if record.ref_names:
                            # 获取转录本ID（多个用逗号分隔）
                            transcript_ids = record.ref_names
                            
                            # 1. raw reads - 记录原始映射信息
                            raw_id = transcript_ids[0] if len(transcript_ids) == 1 else ','.join(transcript_ids)
                            # raw_id = ','.join(transcript_ids)
                            list_raw.append(raw_id)
                            
                            if record.is_multi is True:  # multi-mapping reads
                                # 2. multi mapping reads
                                list_multi_mapping.append(raw_id)
                                
                                # 3. first ID of reads (用于normal计数)
                                if transcript_ids:
                                    #firstID 的多重比对部分
                                    list_firstID.append(transcript_ids[0])
                                
                                # 4. multi ID split to one by one
                                list_multi2single.extend(transcript_ids)
                            elif record.is_multi is False:  # unique mapping reads
                                # 唯一映射
                                # print(11)
                                if transcript_ids:
                                    transcript_id = transcript_ids[0]
                                    list_unique_mapping.append(transcript_id)
                                    #firstID 的独立比对部分
                                    list_firstID.append(transcript_id)
                            
                            total_count += 1
                            pbar.update(1)
                            
                            # if total_count % 1000000 == 0:
                            #     print(f'Processed {total_count} reads')
                            
            except Exception as e:
                print(f"Error parsing file {fanse_file}: {str(e)}")
                continue
        
        # 统计计数
        self.counts_data = {
            'raw': Counter(list_raw),
            'multi': Counter(list_multi_mapping),
            'unique': Counter(list_unique_mapping),
            'firstID': Counter(list_firstID),
            'multi2single': Counter(list_multi2single),
            # 'combined': Counter(list_unique_mapping + list_multi_mapping)
        }
        
        self.summary_stats = {
            'total_reads': total_count,
            'unique_mapped': len(list_unique_mapping),
            'multi_mapped': len(list_multi_mapping),
            'processing_time': time.time() - start_time
        }
        
        print(f"Processing completed in {self.summary_stats['processing_time']:.2f} seconds")
        print(f"Total reads: {total_count:,}")
    
    def generate_count_files(self):
        """生成计数文件"""
        if self.output_filename:
            base_name = Path(self.output_filename).stem
        else:
            base_name = self.input_file.stem
        
        # 生成各种计数文件
        count_files = {}
        
        for count_type, counter in self.counts_data.items():
            df = pd.DataFrame(counter.items(), columns=['Accession', f'{count_type}_count'])
            filename = self.output_dir / f'{base_name}_{count_type}.counts.csv'
            df.to_csv(filename, index=False)
            count_files[count_type] = filename
        
        # 生成合并的计数文件
        if self.level in ['isoform', 'both']:
            combined_df = pd.DataFrame(self.counts_data['firstID'].items(), 
                                     columns=['Accession', 'fanse_count'])
            
            # 合并所有计数类型
            for count_type in ['raw','unique', 'multi', 'multi2single']:
                temp_df = pd.DataFrame(self.counts_data[count_type].items(),
                                    columns=['Accession', f'{count_type}_count'])
                combined_df = combined_df.merge(temp_df, on='Accession', how='outer')
            
            combined_filename = self.output_dir / f'{base_name}_combined.counts.csv'
            combined_df.to_csv(combined_filename, index=False)
            count_files['combined'] = combined_filename
        
        return count_files
    
    def filter_by_minreads(self, minreads=None):
        """根据最小reads数过滤"""
        if minreads is None:
            minreads = self.minreads
        
        if minreads > 0:
            filtered_counts = {
                k: Counter({acc: count for acc, count in v.items() if count >= minreads})
                for k, v in self.counts_data.items()
            }
            
            base_name = self.input_file.stem
            for count_type, counter in filtered_counts.items():
                df = pd.DataFrame(counter.items(), columns=['Accession', 'count'])
                filename = self.output_dir / f'{base_name}_{count_type}_min{minreads}.csv'
                df.to_csv(filename, index=False)
            
            print(f"Filtered by minreads {minreads}, remaining genes: {len(filtered_counts['normal'])}")
    
    def run(self):
        """运行完整的计数流程"""
        print("=" * 60)
        print("fansetools count - Starting processing")
        print("=" * 60)
        
        # 1. 解析fanse3文件
        self.parse_fanse_file()
        
        # 2. 生成计数文件
        count_files = self.generate_count_files()
        
        # 3. 可选过滤reads数目，此处不建议
        if self.minreads > 0:
            self.filter_by_minreads()
        
        # 4. 生成摘要报告
        self.generate_summary()
        
        print("=" * 60)
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
            f.write(f"level parameter: {self.level}\n")
            f.write(f"Min reads filter: {self.minreads}\n")
            f.write("\nStatistics:\n")
            for stat, value in self.summary_stats.items():
                f.write(f"{stat}: {value}\n")

def count_main(args):
    """使用新路径处理器的count主函数"""
    processor = PathProcessor()
    
    try:
        # 1. 解析输入路径
        input_files = processor.parse_input_paths(args.input, ['.fanse','.fanse3', '.fanse3.gz', '.fanse.gz'])
        if not input_files:
            print("错误: 未找到有效的输入文件")
            sys.exit(1)
            
        # 2. 生成输出映射
        output_map = processor.generate_output_mapping(input_files, args.output, '.counts.csv')
        
        # 3. 验证路径
        validation_checks = []
        for input_file in input_files:
            validation_checks.append((input_file, "输入文件", {'must_exist': True, 'must_be_file': True}))
        
        is_valid, errors = processor.validate_paths(*validation_checks)
        if not is_valid:
            print("路径验证失败:")
            for error in errors:
                print(f"  - {error}")
            sys.exit(1)
        
        # 4. 批量处理文件
        print(f"找到 {len(input_files)} 个输入文件，开始处理...")
        
        for i, (input_file, output_file) in enumerate(output_map.items(), 1):
            print(f"\n[{i}/{len(input_files)}] 处理: {input_file.name}")
            print(f"  输出: {output_file}")
            
            try:
                counter = FanseCounter(
                    input_file=str(input_file),
                    output_dir=str(output_file.parent),
                    output_filename=output_file.name,                    
                    minreads=args.minreads,
                    rpkm=args.rpkm,
                    gtf_file=args.gtf,
                    level=args.level,
                    paired_end=args.paired_end
                )
                counter.run()
                print("  ✓ 完成")
            except Exception as e:
                print(f"  ✗ 处理失败: {str(e)}")
        
        print(f"\n处理完成: {len(input_files)} 个文件")
        
    except Exception as e:
        print(f"错误: {str(e)}")
        sys.exit(1)


def add_count_subparser(subparsers):
    """命令行主函数"""
    parser = subparsers.add_parser(
        'count',
        help='运行FANSe to count，输出readcount',
        description='''fansetools count - Process fanse3 files for read counting
        支持批量处理：可以输入单个文件、目录或使用通配符
        can output isoform level 
                      gene level
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        使用示例:
          单个文件处理:
            fanse count -i sample.fanse3 -o results/
          
          批量处理目录中所有fanse3文件:
            fanse count -i /data/*.fanse3 -o /output/
          
          双端测序数据:
            fanse count -i R1.fanse3 -r R2.fanse3 -o results/
          
          使用过滤选项:
            fanse count -i *.fanse3 -o results/ --minreads 5 --level gene
                """
    )
    
    parser.add_argument('-i', '--input', required=True, 
                       help='Input fanse3 file,输入FANSe3文件/目录/通配符（支持批量处理）')
    parser.add_argument('-r', '--paired-end', 
                       help='Paired-end fanse3 file (optional)')
    parser.add_argument('-o', '--output', required=True,
                       help='Output directory,输出路径（文件或目录，自动检测）')
    
    parser.add_argument('--minreads', type=int, default=0,
                       help='Minimum reads threshold for filtering')
    parser.add_argument('--rpkm', type=float, default=0,
                       help='RPKM threshold for filtering')
    
    parser.add_argument('--gtf', 
                       help='GTF/GFF file for gene-level annotation')
    parser.add_argument('--level', choices=['gene', 'isoform'], default='isoform',
                       help='Counting level')
    
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
    