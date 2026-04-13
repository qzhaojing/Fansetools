
# -*- coding: utf-8 -*-
import argparse
import sys
from .tests.test_suite import run_comprehensive_test
from .utils.rich_help import CustomHelpFormatter, add_rich_epilog

def add_test_subparser(subparsers):
    """添加 test 子命令"""
    parser = subparsers.add_parser(
        'test',
        help='运行功能自检',
        description='运行全面的功能测试，检查各模块状态',
        formatter_class=CustomHelpFormatter
    )
    add_rich_epilog(parser, '''
[bold]说明:[/bold]
  运行内置的测试套件，检查 fansetools 各模块是否正常工作。
  
[bold]示例:[/bold]
  fanse test      [dim]# 运行所有测试[/dim]
''')
    parser.set_defaults(func=handle_test_command)

def handle_test_command(args):
    """处理 test 命令"""
    failures = run_comprehensive_test()
    if failures > 0:
        sys.exit(1)
