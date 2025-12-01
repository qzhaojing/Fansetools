import os
import platform
import urllib.request
import zipfile
from pathlib import Path


def get_runtime_base():
    is_windows = platform.system() == 'Windows'
    return Path(__file__).parent / 'bin' / ('windows' if is_windows else 'linux') / 'runtime'


def get_local_java_path():
    base = get_runtime_base() / 'java'
    exe = base / ('bin/java.exe' if platform.system() == 'Windows' else 'bin/java')
    if exe.exists():
        return str(exe)
    exe2 = base / ('java.exe' if platform.system() == 'Windows' else 'java')
    return str(exe2) if exe2.exists() else None


def ensure_java():
    """返回可用的 java 命令路径；优先本地 JRE，其次系统"""
    local = get_local_java_path()
    if local:
        return local
    return 'java'


def install_java():
    """安装轻量 JRE 到 runtime/java（Windows 优先）"""
    is_windows = platform.system() == 'Windows'
    base = get_runtime_base()
    dest = base / 'java'
    base.mkdir(parents=True, exist_ok=True)

    if is_windows:
        url = 'https://github.com/adoptium/temurin17-binaries/releases/latest/download/OpenJDK17U-jre_x64_windows.zip'
        tmp_zip = base / 'jre.zip'
        try:
            print(f'下载 JRE: {url}')
            urllib.request.urlretrieve(url, str(tmp_zip))
            print('解压 JRE...')
            with zipfile.ZipFile(str(tmp_zip), 'r') as z:
                z.extractall(str(base))
            # 移动到 java 目录（解压出的目录可能带版本名）
            extracted_dirs = [p for p in base.iterdir() if p.is_dir() and 'jdk' in p.name.lower() or 'jre' in p.name.lower()]
            if extracted_dirs:
                src = extracted_dirs[0]
                if dest.exists():
                    # 清理旧目录
                    for item in dest.iterdir():
                        if item.is_dir():
                            continue
                    dest.rmdir()
                src.rename(dest)
            tmp_zip.unlink(missing_ok=True)
            print('✓ JRE 安装完成')
            return True
        except Exception as e:
            print(f'安装 JRE 失败: {e}')
            return False
    else:
        print('Linux/Mac 请使用系统包管理器安装 OpenJDK（例如 apt/yum/brew）')
        return False

def add_runtime_subparser(subparsers):
    runtime_parser = subparsers.add_parser(
        'runtime',
        help='运行时管理',
        description='安装运行时（如 Java JRE）'
    )
    runtime_sub = runtime_parser.add_subparsers(dest='rt_cmd')
    rt_java = runtime_sub.add_parser('install', help='安装 Java JRE')
    rt_java.set_defaults(func=lambda a: install_java())
    return runtime_parser


def add_java_subparser(subparsers):
    java_parser = subparsers.add_parser(
        'java',
        help='安装 Java 运行时',
        description='安装或更新本地 Java JRE 到 runtime/java'
    )
    java_parser.set_defaults(func=lambda a: install_java())
    return java_parser
