#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
import platform
from pathlib import Path
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


def find_and_execute_binary(command_name, remaining_args):
    """查找并执行bin目录中的可执行文件"""
    # 获取当前平台
    current_platform = platform.system().lower()
    platform_dirs = {
        'linux': 'linux',
        'darwin': 'macos', 
        'windows': 'windows'
    }
    
    # 获取bin目录路径（相对于包安装位置）
    package_dir = Path(__file__).parent
    bin_base = package_dir / "bin"
    
    # 检查平台对应的目录是否存在
    platform_dir = platform_dirs.get(current_platform)
    if not platform_dir:
        print(f"不支持的操作系统: {current_platform}")
        return False
    
    bin_dir = bin_base / platform_dir
    if not bin_dir.exists():
        print(f"bin目录不存在: {bin_dir}")
        return False
    
    # 构建可执行文件路径
    if current_platform == 'windows':
        executable_name = f"{command_name}.exe"
    else:
        executable_name = command_name
    
    executable_path = bin_dir / executable_name
    
    # 检查可执行文件是否存在
    if not executable_path.is_file():
        return False
    
    # 检查执行权限（非Windows系统）
    if current_platform != 'windows' and not os.access(executable_path, os.X_OK):
        print(f"文件没有执行权限: {executable_path}")
        return False
    
    # 执行二进制文件
    try:
        cmd = [str(executable_path)] + remaining_args
        result = subprocess.run(cmd)
        return True  # 表示成功找到并执行了二进制文件
    except Exception as e:
        print(f"执行错误: {e}")
        return False


def list_available_binaries():
    """列出bin目录中所有可用的可执行文件"""
    current_platform = platform.system().lower()
    platform_dirs = {
        'linux': 'linux',
        'darwin': 'macos',
        'windows': 'windows'
    }
    
    package_dir = Path(__file__).parent
    bin_base = package_dir / "bin"
    
    platform_dir = platform_dirs.get(current_platform)
    if not platform_dir:
        return []
    
    bin_dir = bin_base / platform_dir
    if not bin_dir.exists():
        return []
    
    # 获取所有可执行文件
    binaries = []
    for item in bin_dir.iterdir():
        if item.is_file():
            if current_platform == 'windows':
                if item.suffix == '.exe':
                    binaries.append(item.stem)  # 去掉.exe后缀
            else:
                if os.access(item, os.X_OK):
                    binaries.append(item.name)
    
    return sorted(binaries)


def show_brief_help_with_binaries(subparsers_choices=None):
    """显示包含二进制工具的简洁帮助信息"""
    from . import __version__
    
    # 获取可用的二进制工具
    available_binaries = list_available_binaries()
    
    # 版本信息
    try:
        from .utils.version_check import DualVersionChecker
        from . import __github_repo__
        
        checker = DualVersionChecker(
            current_version=__version__,
            package_name="fansetools",
            github_repo=__github_repo__,
            check_interval_days=5
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
    
    # 显示Python子命令
    if subparsers_choices:
        print("可用命令:")
        max_cmd_len = max(len(cmd) for cmd in subparsers_choices.keys())
        for cmd, subparser in subparsers_choices.items():
            desc = subparser.description.split('\n')[0] if subparser.description else ""
            print(f"  {cmd:<{max_cmd_len}}  {desc}")
    
    # 显示二进制工具
    if available_binaries:
        if subparsers_choices:
            print()
        print("内置工具:")
        max_bin_len = max(len(bin) for bin in available_binaries) if available_binaries else 0
        for binary in available_binaries:
            print(f"  {binary:<{max_bin_len}}  ")
        print('感谢以上工具的作者。')
    
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


def create_parser():
    """创建主解析器"""
    # 延迟导入子命令模块
    from .trim import add_trim_subparser
    from .run import add_run_subparser
    from .count import add_count_subparser
    from .sam import add_sam_subparser
    from .bam import add_bam_subparser
    from .bed import add_bed_subparser
    from .fastx import add_fastx_subparser
    from .sort import add_sort_subparser
    from .mpileup import add_mpileup_subparser
    from .install import add_install_subparser, handle_install_command  # 新增导入
    # 创建主解析器
    parser = argparse.ArgumentParser(
        prog='fanse',
        description='FANSe 工具集 - 比对NGS文件，处理 FANSe3 格式文件的命令行工具',
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
        metavar='<command>'
    )
    
    # 添加所有子命令   
    add_run_subparser(subparsers)
    
    # 子命令：parser
    parser_parser = subparsers.add_parser(
        'parser',
        help='解析 FANSe3 文件',
        description='解析 FANSe3 文件并输出结构化数据'
    )
    parser_parser.add_argument('input_file', help='输入文件路径（FANSe3 格式）')
    #子命令  get()  
    #getref() 获得下载sra等数据
    #getdata() 获得基因组数据
    
    # 子命令：trim
    add_trim_subparser(subparsers)
    
    # 子命令：sam
    add_sam_subparser(subparsers)

    # 子命令：bam
    add_bam_subparser(subparsers)
    
    # 子命令：sort
    add_sort_subparser(subparsers)
    
    # 子命令：count
    add_count_subparser(subparsers)

    # 子命令：bed
    add_bed_subparser(subparsers)

    # 子命令：fastx
    add_fastx_subparser(subparsers)

    # 子命令：mpileup
    add_mpileup_subparser(subparsers)
    
    
    
    # 子命令：install (新增)
    install_parser = add_install_subparser(subparsers)
    install_parser.set_defaults(func=handle_install_command)
    
    # 子命令：update
    update_parser = subparsers.add_parser(
        'update',
        help='检查并更新 fansetools',
        description='检查最新版本并更新 fansetools'
    )
    update_parser.add_argument('-y', '--yes', action='store_true', 
                              help='自动确认更新，无需交互')
    update_parser.set_defaults(func=update_fansetools)
    
    return parser, subparsers.choices


def main():
    """FANSe 工具集主入口"""
    
    # 初始化变量
    remaining_args = sys.argv[1:]
    
    # 先解析基础参数（版本相关）
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('-h', '--help', action='store_true', help='显示帮助信息')
    base_parser.add_argument('-v', '--version', action='store_true', help='显示版本信息')
    base_parser.add_argument('--version-info', action='store_true', help='显示详细的版本和更新信息')
    
    # 只解析版本相关参数，仅当第一个参数是选项（以 '-' 开头）时才解析基础参数。这样设置可以正确解析samtools等的-help参数
    if remaining_args and remaining_args[0].startswith('-'):
        try:
            base_args, remaining_args = base_parser.parse_known_args(remaining_args)
        except SystemExit:
            return 1
        except:
            base_args = argparse.Namespace(help=False, version=False, version_info=False)
            remaining_args = sys.argv[1:]
    else:
        base_args = argparse.Namespace(help=False, version=False, version_info=False)
    
    # 处理版本信息请求（优先处理）
    #if base_args.version or base_args.version_info:
    #    try:
    #        from fansetools.utils.version_check import DualVersionChecker, get_installation_method
    #        from fansetools import __version__, __github_repo__
        #        
    #        if base_args.version:
    #             显示当前版本和最新版本号
    #            checker = DualVersionChecker(
    #                current_version=__version__,
    #                package_name="fansetools",
    #                github_repo=__github_repo__,
    #                check_interval_days=0
    #            )
    #            version_info = checker.check_version()
        #            
    #            if version_info and version_info.get('pypi_latest'):
    #                print(f"fansetools {__version__} → {version_info['pypi_latest']} (最新)")
    #            else:
    #                print(f"fansetools {__version__}")
    #            return
        #            
    #        elif base_args.version_info:
    #            show_detailed_version_info()
    #            return
        #            
    #    except ImportError:
    #        try:
    #            from fansetools import __version__
    #            if base_args.version:
    #                print(f"fansetools {__version__}")
    #                return
    #            elif base_args.version_info:
    #                print(f"fansetools {__version__} (版本检查模块不可用)")
    #                return
    #        except ImportError:
    #            print("fansetools 版本信息不可用")
    #            return
    
    # 检查是否是直接调用二进制文件
    if remaining_args and not getattr(base_args, 'help', False):
        command_name = remaining_args[0]
        
        # 先尝试执行二进制文件
        if find_and_execute_binary(command_name, remaining_args[1:]):
            return  # 如果成功执行了二进制文件，直接返回
    
    # 创建解析器
    parser, subparsers_choices = create_parser()
    
    # 处理帮助请求或没有参数的情况
    if base_args.help or not remaining_args:
        show_brief_help_with_binaries(subparsers_choices)
        return
    
    # 特殊处理：如果第一个参数是子命令但后面没有其他参数，显示该子命令的帮助
    if len(remaining_args) == 1 and remaining_args[0] in subparsers_choices:
        subparsers_choices[remaining_args[0]].print_help()
        return
    
    # 解析参数
    try:
        args = parser.parse_args(remaining_args)
    except SystemExit:
        return 1
    
    # 执行对应的函数
    if hasattr(args, 'func'):
        # 如果是install命令，直接执行
        if args.command == 'install':
            try:
                return args.func(args)
            except Exception as e:
                print(f"安装过程中发生错误: {e}")
                return 1
                
        # 如果是update命令，跳过版本检查（避免循环）
        if args.command == 'update':
            try:
                return args.func(args)
            except Exception as e:
                print(f"更新过程中发生错误: {e}")
                return 1
        else:
            # 对于其他命令，进行版本检查
            try:
                from .utils.version_check import DualVersionChecker, get_installation_method
                from . import __version__, __github_repo__
                
                if not os.getenv('FANSETOOLS_DISABLE_VERSION_CHECK'):
                    checker = DualVersionChecker(
                        current_version=__version__,
                        package_name="fansetools",
                        github_repo=__github_repo__,
                        check_interval_days=7,
                        enable_check=True
                    )
                    checker.show_update_notification(force_check=True)
            except ImportError:
                pass
            
            # 执行命令
            try:
                return args.func(args)
            except Exception as e:
                print(f"命令执行错误: {e}")
                return 1
    else:
        show_brief_help_with_binaries(subparsers_choices)
        return 1


if __name__ == '__main__':
    main()