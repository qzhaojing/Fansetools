
# -*- coding: utf-8 -*-
import argparse
import sys
from .tests.test_suite import run_comprehensive_test

def add_test_subparser(subparsers):
    """添加 test 子命令"""
    parser = subparsers.add_parser(
        'test',
        help='运行功能自检',
        description='运行全面的功能测试，检查各模块状态'
    )
    parser.set_defaults(func=handle_test_command)

def handle_test_command(args):
    """处理 test 命令"""
    failures = run_comprehensive_test()
    if failures > 0:
        sys.exit(1)
