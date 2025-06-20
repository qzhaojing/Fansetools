#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from .parser import fanse_parser
from .sam import fanse2sam

def main():
    
    parser = argparse.ArgumentParser(
        prog='fanse',
        description='FANSe 工具集：用于处理 FANSe3 格式文件的命令行工具\n\n'
                    '支持以下子命令：',
        epilog='示例：\n'
               '  fanse parser input.fanse3      解析 FANSe3 文件\n'
               '  fanse sam input.fanse3 ref.fa  转换为 SAM 格式\n\n'
               '更多帮助请参考：https://github.com/qzhaojing/fanse',
        formatter_class=argparse.RawTextHelpFormatter  # 保留换行和格式
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # 子命令：parser  #########################################
    parser_parser = subparsers.add_parser(
        'parser',
        help='解析 FANSe3 文件',
        description='解析 FANSe3 文件并输出结构化数据'
    )
    parser_parser.add_argument('input_file', help='输入文件路径（FANSe3 格式）')

    
    # 子命令：sam   #########################################33
    sam_parser = subparsers.add_parser(
        'sam',
        help='转换为 SAM 格式',
        description='将 FANSe3 文件转换为标准 SAM 格式'
    )
    sam_parser.add_argument('-i','--input', dest='input_file', required=True, help='输入文件路径（FANSe3 格式）')
    sam_parser.add_argument('-r','--fasta' , dest='fasta_file', required=True, help='参考基因组 FASTA 文件路径')
    sam_parser.add_argument('-o', '--output', help='输出文件路径（默认：打印到终端）')

    args = parser.parse_args()
    if not any(vars(args).values()):  # 无参数时显示帮助
        parser.print_help()
    else:
        if args.command == 'parser':
            for record in fanse_parser(args.input_file):
                print(f"Header: {record.header}")
                print(f"Sequence: {record.seq[:50]}...")
                print(f"Alignment: {record.alignment}..." if record.alignment else "No alignment")
                print("-" * 50)
        
        elif args.command == 'sam':
            fanse2sam(args.input_file, args.fasta_file, args.output)

        elif args.command =='bed':
            fanse2bed(args.input_file, args.fasta_file, args.output)

        elif args.command =='fastq':
            fanse2fastq(args.input_file, args.fasta_file, args.output)

        elif args.command =='fasta':
            fanse2fasta(args.input_file, args.fasta_file, args.output)
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] 程序已被用户中断")  # 自定义简洁提示
        sys.exit(0)  # 静默退出，不显示 Traceback
