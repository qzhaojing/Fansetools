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
from .sort import add_sort_subparser
from .utils.version_check import DualVersionChecker, get_installation_method, update_fansetools


class CustomHelpFormatter(argparse.RawTextHelpFormatter):
    """自定义帮助格式化器，提供更简洁的帮助信息"""
    
    def _format_action(self, action):
        # 简化子命令的显示格式
        if isinstance(action, argparse._SubParsersAction):
            # 只显示子命令名称和简短描述
            parts = []
            for choice, subparser in action.choices.items():
                # 获取子命令的简短描述
                help_text = subparser.description.split('\n')[0] if subparser.description else ""
                parts.append(f"  {choice:<12} {help_text}")
            
            return "\n".join(parts) + "\n"
        return super()._format_action(action)
        

def main():
    """FANSe 工具集主入口"""
    
    # 初始化变量
    remaining_args = sys.argv[1:]
    
   # 先解析基础参数（版本相关）
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('-v', '--version', action='store_true', help='显示版本信息')
    base_parser.add_argument('--version-info', action='store_true', help='显示详细的版本和更新信息')
    
    # 只解析版本相关参数
    try:
        base_args, _ = base_parser.parse_known_args()
    except:
        base_args = argparse.Namespace(version=False, version_info=False)
        remaining_args = sys.argv[1:]
        
    # 处理版本信息请求
    if base_args.version or base_args.version_info:
        try:
            from .utils.version_check import DualVersionChecker, get_installation_method
            from . import __version__, __github_repo__
            
            if base_args.version:
                # 显示当前版本和最新版本号
                checker = DualVersionChecker(
                    current_version=__version__,
                    package_name="fansetools",
                    github_repo=__github_repo__,
                    check_interval_days=0  # 强制检查
                )
                version_info = checker.check_version()
                
                if version_info and version_info.get('pypi_latest'):
                    print(f"fansetools {__version__} → {version_info['pypi_latest']} (最新)")
                else:
                    print(f"fansetools {__version__}")
                return
                
            elif base_args.version_info:
                show_detailed_version_info()
                return
                
        except ImportError:
            from . import __version__
            if base_args.version:
                print(f"fansetools {__version__}")
                return
            elif base_args.version_info:
                print(f"fansetools {__version__} (版本检查模块不可用)")
                return

    # 版本检查（在解析参数前）
    try:
        from .utils.version_check import DualVersionChecker, get_installation_method
        from . import __version__, __github_repo__
                
        if not os.getenv('FANSETOOLS_DISABLE_VERSION_CHECK'):
            checker = DualVersionChecker(
                current_version=__version__,
                package_name="fansetools",
                github_repo=__github_repo__,
                check_interval_days=7,  # 每次都检查
                enable_check=True
            )
            # 只在非update命令时显示更新通知
            if not (remaining_args and remaining_args[0] == 'update'):
                checker.show_update_notification(force_check=True)
    except ImportError:
        pass

#----------------------------------------------------------------------------

    # 创建主解析器
    parser = argparse.ArgumentParser(
        prog='fanse',
        description='FANSe 工具集 - 处理 FANSe3 格式文件的命令行工具',
        epilog='使用 "fanse <command> -h" 查看具体命令的帮助信息',
        formatter_class=CustomHelpFormatter,
        add_help=False,
        usage='fanse [-h] [-v] [--version-info] <command> [<args>]'
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

    # 子命令：count
    add_count_subparser(subparsers)
    
    # 子命令：sam
    add_sam_subparser(subparsers)

    # 子命令：sort
    add_sort_subparser(subparsers)

    # 子命令： bed
    add_bed_subparser(subparsers)

    # 子命令：fastx
    add_fastx_subparser(subparsers)

    # 子命令：mpileup
    add_mpileup_subparser(subparsers)
    

    # 子命令：update - 新增更新命令
    update_parser = subparsers.add_parser(
        'update',
        help='检查并更新 fansetools',
        description='检查最新版本并更新 fansetools'
    )
    update_parser.add_argument('-y', '--yes', action='store_true', 
                              help='自动确认更新，无需交互')
    update_parser.set_defaults(func=update_fansetools)
    
    
    # 特殊情况：直接显示子命令帮助
    if len(remaining_args) == 1 and remaining_args[0] in subparsers.choices:
        # 用户输入了 "fanse command"，显示该命令的帮助
        subparsers.choices[remaining_args[0]].print_help()
        return

    # 解析剩余参数
    try:
        args = parser.parse_args(remaining_args)
    except SystemExit:
        # 当没有提供命令时显示简洁帮助
        if not remaining_args or remaining_args[0] in ['-h', '--help']:
            show_brief_help(parser, subparsers)
        return
        
    # 处理帮助请求
    if hasattr(args, 'help') and args.help:
        show_brief_help(parser, subparsers)
        return
        
    if not hasattr(args, 'func') or args.command is None:
        show_brief_help(parser, subparsers)
        return

    # 禁用colorama（非交互式终端）
    if not sys.stdout.isatty():
        os.environ["COLORAMA_DISABLE"] = "true"

    try:
        args.func(args)
    except Exception as e:
        print(f"错误: {str(e)}")
        sys.exit(1)

#
#def show_brief_help(parser, subparsers):
#    """显示简洁的帮助信息"""
#    from . import __version__
#
#    print(f"fansetools {__version__} - FANSe3文件处理工具集")
#    print("=" * 50)
#    print("使用方法: fanse <command> [选项]")
#    print()
#    print("可用命令:")
#    
#    # 显示简化的命令列表
#    max_cmd_len = max(len(cmd) for cmd in subparsers.choices.keys())
#    for cmd, subparser in subparsers.choices.items():
#        desc = subparser.description.split('\n')[0] if subparser.description else ""
#        print(f"  {cmd:<{max_cmd_len}}  {desc}")
#    
#    print()
#    print("使用 'fanse <command> -h' 查看具体命令的详细帮助")
#    print("使用 'fanse -v' 查看版本，'fanse --version-info' 查看更新信息")
#
#
#def show_detailed_version_info():
#    """显示详细的版本信息"""
#    from . import __version__, __github_repo__
#    from .utils.version_check import DualVersionChecker, get_installation_method
#    
#    installation_method = get_installation_method()
#    
#    print("=" * 50)
#    print("fansetools - 版本信息")
#    print("=" * 50)
#    print(f"当前版本: {__version__}")
#    print(f"安装方式: {installation_method}")
#    print(f"GitHub仓库: {__github_repo__}")
#    print()
#    
#    # 检查更新
#    try:
#        checker = DualVersionChecker(
#            current_version=__version__,
#            package_name="fansetools",
#            github_repo=__github_repo__,
#            check_interval_days=0
#        )
#        
#        version_info = checker.check_version()
#        
#        if version_info:
#            print("最新版本信息:")
#            if version_info.get('pypi_latest'):
#                status = "可更新" if version_info.get('pypi_update_available') else "已是最新"
#                print(f"  PyPI版本: {version_info['pypi_latest']} ({status})")
#            
#            if version_info.get('github_latest'):
#                gh_info = version_info['github_latest']
#                print(f"  GitHub最新提交: {gh_info['sha'][:18]} - {gh_info['message'][:150]}...")
#            
#            print()
#            print("更新命令:")
#            if installation_method == 'pip':
#                print("  pip install --upgrade fansetools")
#            elif installation_method == 'git':
#                print("  git pull origin main")
#            elif installation_method == 'conda':
#                print("  conda update fansetools")
#            else:
#                print("  pip install --upgrade fansetools")
#    except Exception:
#        print("无法检查更新信息")
#    
#    print("=" * 50)
def show_brief_help(parser, subparsers):
    """显示简洁的帮助信息"""
    from . import __version__
    
    # 检查最新版本
    try:
        from .utils.version_check import DualVersionChecker
        from . import __github_repo__
        
        checker = DualVersionChecker(
            current_version=__version__,
            package_name="fansetools",
            github_repo=__github_repo__,
            check_interval_days=0
        )
        version_info = checker.check_version()
        
        if version_info and version_info.get('pypi_latest'):
            version_str = f"fansetools {__version__} → {version_info['pypi_latest']} (最新)"
        else:
            version_str = f"fansetools {__version__}"
    except:
        version_str = f"fansetools {__version__}"
    
    print(version_str + " - FANSe3文件处理工具集")
    print("=" * 50)
    print("使用方法: fanse <command> [选项]")
    print()
    print("可用命令:")
    
    # 显示简化的命令列表
    max_cmd_len = max(len(cmd) for cmd in subparsers.choices.keys())
    for cmd, subparser in subparsers.choices.items():
        desc = subparser.description.split('\n')[0] if subparser.description else ""
        print(f"  {cmd:<{max_cmd_len}}  {desc}")
    
    print()
    print("使用 'fanse <command> -h' 查看具体命令的详细帮助")
    print("使用 'fanse -v' 查看版本，'fanse --version-info' 查看更新信息")
    print("使用 'fanse update' 检查并更新到最新版本")


def show_detailed_version_info():
    """显示详细的版本信息"""
    from . import __version__, __github_repo__
    from .utils.version_check import DualVersionChecker, get_installation_method
    
    installation_method = get_installation_method()
    
    print("=" * 50)
    print("fansetools - 版本信息")
    print("=" * 50)
    print(f"当前版本: {__version__}")
    print(f"安装方式: {installation_method}")
    print(f"GitHub仓库: {__github_repo__}")
    print()
    
    # 检查更新
    try:
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
                print(f"  GitHub最新提交: {gh_info['sha'][:18]} - {gh_info['message'][:150]}...")
            
            print()
            print("更新命令:")
            if installation_method == 'pip':
                print("  pip install --upgrade fansetools")
            elif installation_method == 'git':
                print("  git pull origin main")
            elif installation_method == 'conda':
                print("  conda update fansetools")
            else:
                print("  pip install --upgrade fansetools")
                
            print("  或使用: fanse update")
    except Exception:
        print("无法检查更新信息")
    
    print("=" * 50)

def show_main_help(parser):
    """显示主帮助信息"""
    from . import __version__
    
    print(f"fansetools {__version__} - FANSe3文件处理工具集")
    print("=" * 60)
    parser.print_help()
    print("\n快速使用:")
    print("  fanse run -i input.fq -r ref.fa -o output/    # 运行FANSe3比对")
    print("  fanse count -i *.fanse3 -o results/           # 统计reads计数")
    print("  fanse sam -i input.fanse3 -r ref.fa -o output.sam  # 转换FANSe为SAM格式")
    print("  fanse sort -i input.sam -o sorted.sam --sort coord  # 排序SAM文件, 默认coord")
    print("\n使用 fanse -v 查看版本，fanse --version-info 检查更新信息")
    print("使用 fanse update 检查并更新到最新版本")

#def show_detailed_version_info():
#    """显示详细的版本信息"""
#    from . import __version__, __github_repo__
#    from .utils.version_check import DualVersionChecker, get_installation_method
#    
#    installation_method = get_installation_method()
#    
#    print("=" * 60)
#    print("fansetools - 版本信息")
#    print("=" * 60)
#    print(f"当前版本: {__version__}")
#    print(f"安装方式: {installation_method}")
#    print(f"GitHub仓库: {__github_repo__}")
#    print("")
#    
#    # 检查更新
#    checker = DualVersionChecker(
#        current_version=__version__,
#        package_name="fansetools",
#        github_repo=__github_repo__,
#        check_interval_days=0
#    )
#    
#    version_info = checker.check_version()
#    
#    if version_info:
#        print("最新版本信息:")
#        if version_info.get('pypi_latest'):
#            status = "可更新" if version_info.get('pypi_update_available') else "已是最新"
#            print(f"  PyPI版本: {version_info['pypi_latest']} ({status})")
#        
#        if version_info.get('github_latest'):
#            gh_info = version_info['github_latest']
#            print(f"  GitHub最新提交: {gh_info['sha']}")
#            print(f"  提交信息: {gh_info['message']}")
#            print(f"  提交时间: {gh_info['date'][:10]}")
#        
#        print("")
#        print("更新命令:")
#        if installation_method == 'pip':
#            print("  pip install --upgrade fansetools")
#        elif installation_method == 'git':
#            print("  git pull origin main")
#            print("  pip install -e .  # 重新安装开发版本")
#        elif installation_method == 'conda':
#            print("  conda update fansetools")
#        else:
#            print("  pip install --upgrade fansetools")
#    
#    print("=" * 60)


    
    
if __name__ == '__main__':
    main()


