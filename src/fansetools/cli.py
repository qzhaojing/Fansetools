#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from .parser import fanse_parser
from .run import add_run_subparser
from .sam import add_sam_subparser
from .bed import add_bed_subparser
from .fastx import add_fastx_subparser
from .mpileup import add_mpileup_subparser
from .count import add_count_subparser
import sys
import os


def main():
    """FANSe 工具集主入口"""
    parser = argparse.ArgumentParser(
        prog='fanse',
        description='FANSe 工具集：用于处理 FANSe3 格式文件的命令行工具\n\n'
                    '支持以下子命令：',
        epilog='示例：\n'
        ' fanse run -i inputdir/input.fastq/fq/fastq.gz/fq.gz  -r ref.fa -o outputdir'
               '  fanse parser input.fanse3      解析 FANSe3 文件\n'
               '  fanse sam input.fanse3 ref.fa  转换为 SAM 格式\n\n'
               '  fanse bed   \n'
               '  fanse convert  \n\n'

               '更多帮助请参考：https://github.com/qzhaojing/fansetools',
        formatter_class=argparse.RawTextHelpFormatter  # 保留换行和格式
    )
    subparsers = parser.add_subparsers(
        title='可用命令',
        dest='command',
        required=True)

    add_run_subparser(subparsers)

    # 子命令：parser
    parser_parser = subparsers.add_parser(
        'parser',
        help='解析 FANSe3 文件',
        description='解析 FANSe3 文件并输出结构化数据'
    )
    parser_parser.add_argument('input_file', help='输入文件路径（FANSe3 格式）')

    # 子命令：sam
    add_sam_subparser(subparsers)

    # 子命令： bed
    add_bed_subparser(subparsers)

    # 子命令：fastx
    add_fastx_subparser(subparsers)

    # 子命令：mpileup
    add_mpileup_subparser(subparsers)
    
    # 子命令：count
    add_count_subparser(subparsers)

    # 解析参数
    args = parser.parse_args()
    # 无参数时显示帮助
    if not hasattr(args, 'func'):
        parser.print_help()
        return
    # 在cli.py的main()中添加
    if not sys.stdout.isatty():
        os.environ["COLORAMA_DISABLE"] = "true"  # 禁用colorama

    try:
        args.func(args)
    except Exception as e:
        print(f"\n错误: {str(e)}")
        sys.exit(1)

# ------------------------------------------
    #args = parser.parse_args()
    # if not any(vars(args).values()):  # 无参数时显示帮助
    #    parser.print_help()
    # else:
    #    if args.command == 'parser':
    #        for record in fanse_parser(args.input_file):
    #            print(f"Header: {record.header}")
    #            print(f"Sequence: {record.seq[:50]}...")
    #            print(f"Alignment: {record.alignment}..." if record.alignment else "No alignment")
    #            print("-" * 50)
        #
    #    elif args.command == 'sam':
    #        fanse2sam(args.input_file, args.fasta_file, args.output)
        #
    #    elif hasattr(args, 'func'):
    #        args.func(args)


if __name__ == '__main__':
    main()


#import argparse
#from .run import add_run_subparser
#from .sam import add_sam_subparser
#from .bed import add_bed_subparser
#from .fastx import add_fastx_subparser
#
# def main():
#    """FANSe 工具集主入口"""
#    parser = argparse.ArgumentParser(
#        prog='fanse',
#        description='FANSe 工具集 - 高通量测序数据分析工具',
#        epilog='更多帮助请访问: https://github.com/qzhaojing/Fansetools',
#        formatter_class=argparse.RawTextHelpFormatter
#    )
#
#    # 添加子命令
#    subparsers = parser.add_subparsers(
#        title='可用命令',
#        dest='command',
#        metavar='<command>'
#    )
#
#    # 添加各模块子命令
#    add_run_subparser(subparsers)
#    add_sam_subparser(subparsers)
#    add_bed_subparser(subparsers)
#    add_fastx_subparser(subparsers)
#
#    # 解析参数
#    args = parser.parse_args()
#
#    if not hasattr(args, 'func'):
#        parser.print_help()
#        return
#
#    try:
#        args.func(args)
#    except Exception as e:
#        print(f"\n错误: {str(e)}")
#        sys.exit(1)
#
# if __name__ == '__main__':
#    main()
