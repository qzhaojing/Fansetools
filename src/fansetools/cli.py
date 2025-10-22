#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
from .parser import fanse_parser
from .run import add_run_subparser
from .sam import add_sam_subparser
from .bed import add_bed_subparser
from .fastx import add_fastx_subparser
from .mpileup import add_mpileup_subparser
from .count import add_count_subparser



def main():
    """FANSe 工具集主入口"""
   # 先解析基础参数（版本相关）
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('-v', '--version', action='store_true', help='显示版本信息')
    base_parser.add_argument('--version-info', action='store_true', help='显示详细的版本和更新信息')
    
    # 只解析版本相关参数
    try:
        base_args, _ = base_parser.parse_known_args()
    except:
        base_args = argparse.Namespace(version=False, version_info=False)

    # 只有在明确要求版本信息时才检查更新
    if base_args.version or base_args.version_info:
        try:
            from .utils.version_check import DualVersionChecker, get_installation_method
            from . import __version__, __github_repo__
            
            if base_args.version:
                # 简单版本信息
                print(f"fansetools {__version__}")
                return
                
            elif base_args.version_info:
                # 详细版本信息
                show_detailed_version_info()
                return
                
        except ImportError:
            # 如果版本检查模块不可用，显示基本版本
            from . import __version__
            if base_args.version:
                print(f"fansetools {__version__}")
                return
            elif base_args.version_info:
                print(f"fansetools {__version__} (版本检查模块不可用)")
                return
    # 版本检查（在解析参数前）
    #try:
    #    from .utils.version_check import DualVersionChecker, get_installation_method
    #    from . import __version__, __github_repo__
        #    
    #    if not os.getenv('FANSETOOLS_DISABLE_VERSION_CHECK'):
    #        checker = DualVersionChecker(
    #            current_version=__version__,
    #            package_name="fansetools",
    #            github_repo=__github_repo__,
    #            check_interval_days=0,  # 每次都检查
    #            enable_check=True
    #        )
    #        checker.show_update_notification(force_check=True)
    #except ImportError:
    #    pass

    
    parser = argparse.ArgumentParser(
        prog='fanse',
        description='FANSe 工具集：用于处理 FANSe3 格式文件的命令行工具\n\n'
                    '支持以下子命令：',
        epilog='示例：\n'
        ' fanse run -i inputdir/input.fastq/fq/fastq.gz/fq.gz  -r ref.fa -o outputdir'
               '  fanse parser input.fanse3      解析 FANSe3 文件\n'
               '  fanse sam input.fanse3 ref.fa  转换为 SAM 格式\n\n'
               '  fanse bed   \n'
               '  fanse convert  \n'
               '  fanse count \n'

               '更多帮助请参考：https://github.com/qzhaojing/fansetools',
        formatter_class=argparse.RawTextHelpFormatter,  # 保留换行和格式
        add_help=False  # 禁用默认的help，我们会自己处理
    )
    
 # 添加帮助选项
    parser.add_argument('-h', '--help', action='store_true', help='显示帮助信息')
    
    subparsers = parser.add_subparsers(
        title='可用命令',
        dest='command',
        required=False)
    # 添加所有子命令   
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
    try:
        args = parser.parse_args()
    except SystemExit:
        # 当没有提供命令时显示帮助
        show_main_help(parser)
        return

    # 处理帮助请求
    if hasattr(args, 'help') and args.help:
        show_main_help(parser)
        return
        
    if not hasattr(args, 'func') or args.command is None:
        show_main_help(parser)
        return

    # 禁用colorama（非交互式终端）
    if not sys.stdout.isatty():
        os.environ["COLORAMA_DISABLE"] = "true"

    try:
        args.func(args)
    except Exception as e:
        print(f"\n错误: {str(e)}")
        sys.exit(1)


def show_main_help(parser):
    """显示主帮助信息"""
    from . import __version__
    
    print(f"fansetools {__version__} - FANSe3文件处理工具集")
    print("=" * 60)
    parser.print_help()
    print("\n快速使用:")
    print("  fanse run -i input.fq -r ref.fa -o output/    # 运行FANSe3比对")
    print("  fanse count -i *.fanse3 -o results/           # 统计reads计数")
    print("  fanse sam -i input.fanse3 -r ref.fa -o output.sam  # 转换为SAM格式")
    print("\n使用 fanse -v 查看版本，fanse --version-info 查看更新信息")


def show_detailed_version_info():
    """显示详细的版本信息"""
    from . import __version__, __github_repo__
    from .utils.version_check import DualVersionChecker, get_installation_method
    
    installation_method = get_installation_method()
    
    print("=" * 60)
    print("fansetools - 版本信息")
    print("=" * 60)
    print(f"当前版本: {__version__}")
    print(f"安装方式: {installation_method}")
    print(f"GitHub仓库: {__github_repo__}")
    print("")
    
    # 检查更新
    checker = DualVersionChecker(
        current_version=__version__,
        package_name="fansetools",
        github_repo=__github_repo__,
        check_interval_days=0
    )
    
    version_info = checker.check_version()
    
    if version_info:
        print("最新版本信息:")
        if version_info.get('pypi_latest'):
            status = "可更新" if version_info.get('pypi_update_available') else "已是最新"
            print(f"  PyPI版本: {version_info['pypi_latest']} ({status})")
        
        if version_info.get('github_latest'):
            gh_info = version_info['github_latest']
            print(f"  GitHub最新提交: {gh_info['sha']}")
            print(f"  提交信息: {gh_info['message']}")
            print(f"  提交时间: {gh_info['date'][:10]}")
        
        print("")
        print("更新命令:")
        if installation_method == 'pip':
            print("  pip install --upgrade fansetools")
        elif installation_method == 'git':
            print("  git pull origin main")
            print("  pip install -e .  # 重新安装开发版本")
        elif installation_method == 'conda':
            print("  conda update fansetools")
        else:
            print("  pip install --upgrade fansetools")
    
    print("=" * 60)


    
    
if __name__ == '__main__':
    main()


