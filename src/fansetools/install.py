#!/usr/bin/env python
import os
import sys
import argparse
import urllib.request
import zipfile
import tarfile
import tempfile
import platform
import shutil
import json
from pathlib import Path
from urllib.parse import urlparse
import posixpath

# 预定义的软件包列表
REPO_URL = "https://github.com/qzhaojing/Fansetools/wiki"
PREDEFINED_PACKAGES = {
    "samtools": {
        "windows": "https://github.com/qzhaojing/Fansetools/raw/main/src/fansetools/bin/windows/samtools.exe",
        "linux": "https://github.com/samtools/samtools/releases/download/1.17/samtools-1.17.tar.bz2"
    },
    "bcftools": {
        "windows": "https://github.com/qzhaojing/Fansetools/raw/main/src/fansetools/bin/windows/bcftools.exe",
        "linux": "*"
    },
    "bwa": {
        "windows": "https://github.com/qzhaojing/Fansetools/raw/main/src/fansetools/bin/windows/bwa.exe",
        "linux": "*"
    },
    "pigz": {
        "windows": "https://kjkpub.s3.amazonaws.com/software/pigz/2.3.1-149/pigz.zip",
        "linux": "*"
    },
    "fastqc": {
        "windows": "https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.12.1.zip",
        "linux": "https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.12.1.zip"
    },
    "rush": {
        "windows": "https://github.com/shenwei356/rush/releases/download/v0.7.0/rush_windows_amd64.exe.tar.gz",
        "linux": "*"
    },
    "flash": {
        "windows": "http://ccb.jhu.edu/software/FLASH/FLASH-1.2.11-windows-bin.zip",
        "linux": "*"
    },
    "csvtk": {
        "windows": "https://github.com/shenwei356/csvtk/releases/download/v0.36.0/csvtk_windows_amd64.exe.tar.gz",
        "linux": "*"
    },
    "snpeff": {
        "windows": "https://snpeff.odsp.astrazeneca.com/versions/snpEff_latest_core.zip",
        "linux": "https://snpeff.odsp.astrazeneca.com/versions/snpEff_latest_core.zip"
    },

}

# 安装信息文件路径
INSTALL_INFO_FILE = Path(__file__).parent / "installed_packages.json"


def get_platform():
    """获取操作系统类型"""
    return "windows" if platform.system() == "Windows" else "linux"


def get_install_dir():
    """获取安装目录"""
    current_platform = get_platform()
    prefix = os.environ.get("FANSE_HOME") or os.environ.get("FANSE_PREFIX")
    if prefix:
        return os.path.join(prefix, "bin", current_platform)
    base = Path(__file__).parent / "bin" / current_platform
    return str(base)


def get_filename_from_url(url):
    """从URL中提取文件名"""
    parsed = urlparse(url)
    return posixpath.basename(parsed.path) or "download"


def download_file(url, dest_dir):
    """下载文件到指定目录"""
    filename = get_filename_from_url(url)
    dest_path = os.path.join(dest_dir, filename)

    try:
        print(f"下载: {url}")
        urllib.request.urlretrieve(url, dest_path)
        print(f"完成: {filename}")
        return dest_path
    except Exception as e:
        print(f"下载失败: {e}")
        return None


def extract_archive(archive_path, extract_dir):
    """解压文件"""
    try:
        if archive_path.endswith('.zip'):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        else:
            with tarfile.open(archive_path, 'r:*') as tar_ref:
                tar_ref.extractall(extract_dir)
        return True
    except Exception as e:
        print(f"解压失败: {e}")
        return False

# def find_jar_files(directory):
#     """查找jar文件"""
#     jar_files = []
#     for root, dirs, files in os.walk(directory):
#         for file in files:
#             if file.lower().endswith('.jar'):
#                 jar_files.append(os.path.join(root, file))
#     return jar_files

# def create_launcher(install_dir, package_name, jar_path=None):
#     """创建启动脚本"""
#     if get_platform() == "windows":
#         launcher_path = os.path.join(install_dir, f"{package_name}.bat")
#         if jar_path:
#             content = f'@echo off\njava -jar "{jar_path}" %*\n'
#         else:
#             content = f'@echo off\n"{package_name}.exe" %*\n'
#     else:
#         launcher_path = os.path.join(install_dir, package_name)
#         if jar_path:
#             content = f'#!/bin/bash\njava -jar "{jar_path}" "$@"\n'
#         else:
#             content = f'#!/bin/bash\n./{package_name} "$@"\n'

#     try:
#         with open(launcher_path, 'w') as f:
#             f.write(content)
#         if get_platform() != "windows":
#             os.chmod(launcher_path, 0o755)
#         return True
#     except Exception as e:
#         print(f"创建启动器失败: {e}")
#         return False

# def save_install_info(package_name, install_path, is_jar=False):
#     """保存安装信息"""
#     try:
#         if os.path.exists(INSTALL_INFO_FILE):
#             with open(INSTALL_INFO_FILE, 'r') as f:
#                 info = json.load(f)
#         else:
#             info = {}

#         info[package_name] = {
#             "path": install_path,
#             "is_jar": is_jar,
#             "platform": get_platform(),
#             "installed_at": str(Path(install_path).stat().st_mtime) if os.path.exists(install_path) else "unknown"
#         }

#         with open(INSTALL_INFO_FILE, 'w') as f:
#             json.dump(info, f, indent=2)
#         return True
#     except Exception as e:
#         print(f"保存安装信息失败: {e}")
#         return False


def load_install_info():
    """加载安装信息"""
    try:
        if INSTALL_INFO_FILE.exists():
            with open(INSTALL_INFO_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def install_package(package_name_or_url):
    """安装软件包 - 简化版本"""
    install_dir = get_install_dir()
    os.makedirs(install_dir, exist_ok=True)

    # 获取下载URL
    if package_name_or_url in PREDEFINED_PACKAGES:
        package_info = PREDEFINED_PACKAGES[package_name_or_url]
        if get_platform() not in package_info or package_info[get_platform()] == "*":
            print(f"包 {package_name_or_url} 不支持当前平台")
            return False
        url = package_info[get_platform()].replace("/blob/", "/raw/")
        package_name = package_name_or_url
    else:
        url = package_name_or_url
        package_name = os.path.splitext(get_filename_from_url(url))[0]

    print(f"安装: {package_name}")

    # 下载文件
    with tempfile.TemporaryDirectory() as temp_dir:
        archive_path = download_file(url, temp_dir)
        if not archive_path:
            return False

        # 如果是压缩文件，解压处理
        if any(archive_path.lower().endswith(ext) for ext in ('.zip', '.tar.gz', '.tgz', '.tar.bz2', '.tar')):
            extract_dir = os.path.join(temp_dir, "extracted")
            os.makedirs(extract_dir)

            if not extract_archive(archive_path, extract_dir):
                return False

            # 查找jar文件
            jar_files = find_jar_files(extract_dir)
            if jar_files:
                # 如果是jar应用，复制整个目录
                package_dir = os.path.join(install_dir, package_name)
                if os.path.exists(package_dir):
                    shutil.rmtree(package_dir)
                shutil.copytree(extract_dir, package_dir)

                # 优先使用厂商脚本（bat/sh）
                vendor_script = None
                for root, dirs, files in os.walk(package_dir):
                    for f in files:
                        if f.lower().endswith(('.bat', '.sh')):
                            vendor_script = os.path.join(root, f)
                            break
                    if vendor_script:
                        break

                if vendor_script:
                    rel_jars = []
                    try:
                        for j in jar_files:
                            rel_jars.append(os.path.relpath(os.path.join(package_dir, os.path.basename(j)), package_dir))
                    except Exception:
                        rel_jars = [os.path.basename(j) for j in jar_files]
                    if create_launcher(install_dir, package_name, exec_path=vendor_script):
                        save_install_info(
                            package_name,
                            package_dir,
                            is_jar=True,
                            entry_path=vendor_script,
                            extra_info={"jar_files": rel_jars, "vendor_script": vendor_script}
                        )
                        print(f"✓ Java应用安装完成（脚本）: {package_name}")
                        return True

                # 回退：使用最大的 jar
                main_jar = max(jar_files, key=lambda x: os.path.getsize(x))
                main_jar_name = os.path.basename(main_jar)
                main_jar_path = os.path.join(package_dir, main_jar_name)

                rel_jars = []
                try:
                    for j in jar_files:
                        rel_jars.append(os.path.relpath(os.path.join(package_dir, os.path.basename(j)), package_dir))
                except Exception:
                    rel_jars = [os.path.basename(j) for j in jar_files]

                if create_launcher(install_dir, package_name, jar_path=main_jar_path, exec_path=None):
                    save_install_info(
                        package_name,
                        package_dir,
                        is_jar=True,
                        entry_path=main_jar_path,
                        extra_info={"jar_files": rel_jars}
                    )
                    print(f"✓ Java应用安装完成: {package_name}")
                    return True
            else:
                # 查找可执行文件
                executables = []
                for root, dirs, files in os.walk(extract_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        if (
                            file.lower().endswith(('.exe', '.bin', '.bat', '.cmd', '.ps1', '.py', '.sh', '.r')) or
                            (get_platform() != "windows" and os.access(file_path, os.X_OK))
                        ):
                            executables.append(file_path)

                if executables:
                    # 复制可执行文件
                    copied = []
                    for exec_path in executables:
                        exec_name = os.path.basename(exec_path)
                        dest_path = os.path.join(install_dir, exec_name)
                        shutil.copy2(exec_path, dest_path)
                        copied.append(dest_path)
                        if get_platform() != "windows":
                            os.chmod(dest_path, 0o755)

                    # 创建启动脚本（使用真实入口）
                    main_exec = copied[0]
                    if create_launcher(install_dir, package_name, exec_path=main_exec):
                        save_install_info(
                            package_name, main_exec, is_jar=False, entry_path=main_exec)
                        print(f"✓ 安装完成: {package_name}")
                        return True
        else:
            # 单个文件直接复制
            dest_path = os.path.join(
                install_dir, os.path.basename(archive_path))
            shutil.copy2(archive_path, dest_path)
            if get_platform() != "windows":
                os.chmod(dest_path, 0o755)

            if create_launcher(install_dir, package_name, exec_path=dest_path):
                save_install_info(package_name, dest_path, is_jar=False, entry_path=dest_path)
                print(f"✓ 安装完成: {package_name}")
                return True

    print("安装失败")
    return False


def list_installed_packages():
    """列出已安装的包"""
    info = load_install_info()
    if not info:
        print("没有安装任何包")
        return

    print("已安装的包:")
    print("-" * 40)
    for package, details in info.items():
        status = "可用" if os.path.exists(details["path"]) else "缺失"
        package_type = "Java应用" if details.get("is_jar") else "可执行文件"
        extra = ""
        if details.get("is_jar"):
            jars = details.get("jar_files") or []
            extra = f" | JARs: {len(jars)}"
        alias = details.get("alias")
        if alias and alias != package:
            extra += f" | 别名: {alias}"
        print(f"  {package:<15} - {package_type} ({status}){extra}")
    print("-" * 40)


def list_available_packages():
    """列出可安装的包"""
    current_platform = get_platform()
    print("可安装的包:")
    print("-" * 40)

    # 已安装的包
    installed_info = load_install_info()
    installed_packages = set(installed_info.keys())

    for package_name in PREDEFINED_PACKAGES:
        if current_platform in PREDEFINED_PACKAGES[package_name]:
            status = "已安装" if package_name in installed_packages else "可安装"
            print(f"  {package_name:<15} - {status}")

    print("-" * 40)
    print("使用 'fanse install <包名>' 安装")
    print(f"更多软件与安装说明: {REPO_URL}")


def show_package_help():
    """显示包管理帮助"""
    print("fanse 包管理器")
    print("=" * 50)
    print("用法:")
    print("  fanse                    # 显示所有可用命令和包")
    print("  fanse install <包名>     # 安装包")
    print("  fanse list               # 列出可安装的包")
    print("  fanse installed          # 列出已安装的包")
    print("  fanse <包名> [参数]     # 使用已安装的包")
    print()
    print("示例:")
    print("  fanse install samtools")
    print("  fanse samtools --help")
    print("  fanse list")
    print()


def _legacy_install_command(args):
    """保留旧入口，委托到统一实现"""
    return handle_install_command(args)


def find_jar_files(directory):
    """查找jar文件 - 优化版本"""
    jar_files = []
    try:
        for root, dirs, files in os.walk(directory):
            # 使用列表推导式提高效率
            jar_files.extend(
                os.path.join(root, file)
                for file in files
                if file.lower().endswith('.jar')
            )
            # 限制搜索深度，避免无限递归
            if len(jar_files) > 10:  # 最多找10个jar文件
                break
    except Exception as e:
        print(f"搜索jar文件时出错: {e}")

    return jar_files


def _find_local_java():
    is_windows = get_platform() == "windows"
    base = Path(__file__).parent / "bin" / ("windows" if is_windows else "linux") / "runtime" / "java"
    candidates = [
        base / "bin" / ("java.exe" if is_windows else "java"),
        base / ("java.exe" if is_windows else "java"),
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None
def create_launcher(install_dir, package_name, jar_path=None, exec_path=None):
    """创建启动脚本 - 使用真实入口路径并支持多类型"""
    try:
        is_windows = get_platform() == "windows"
        launcher_path = os.path.join(install_dir, f"{package_name}.bat" if is_windows else package_name)

        if jar_path:
            java_cmd = _find_local_java() or "java"
            content = (
                f'@echo off\n"{java_cmd}" -jar "{jar_path}" %*\n' if is_windows
                else f'#!/bin/bash\n"{java_cmd}" -jar "{jar_path}" "$@"\n'
            )
        elif exec_path:
            if is_windows:
                ext = Path(exec_path).suffix.lower()
                if ext in ('.bat', '.cmd'):
                    content = f'@echo off\ncall "{exec_path}" %*\n'
                elif ext == '.ps1':
                    content = f'@echo off\npowershell -ExecutionPolicy Bypass -File "{exec_path}" %*\n'
                elif ext == '.py':
                    content = f'@echo off\npy -3 "{exec_path}" %*\n'
                elif ext == '.r':
                    content = f'@echo off\nRscript "{exec_path}" %*\n'
                elif ext == '.sh':
                    content = f'@echo off\nbash "{exec_path}" %*\n'
                else:
                    content = f'@echo off\n"{exec_path}" %*\n'
            else:
                content = f'#!/bin/bash\n"{exec_path}" "$@"\n'
        else:
            content = (
                f'@echo off\n"{package_name}.exe" %*\n' if is_windows
                else f'#!/bin/bash\n./{package_name} "$@"\n'
            )

        with open(launcher_path, 'w', encoding='utf-8') as f:
            f.write(content)

        if not is_windows:
            os.chmod(launcher_path, 0o755)

        return True
    except Exception as e:
        print(f"创建启动器失败: {e}")
        return False


def save_install_info(package_name, install_path, is_jar=False, alias=None, entry_path=None, extra_info=None):
    """保存安装信息 - 使用 Path 与入口信息"""
    try:
        install_info = load_install_info()
        entry = {
            "path": str(Path(install_path)),
            "is_jar": is_jar,
            "platform": get_platform(),
            "installed_at": str(Path(install_path).stat().st_mtime) if Path(install_path).exists() else "unknown"
        }
        if alias:
            entry["alias"] = alias
        if entry_path:
            entry["entry_path"] = str(entry_path)
        if extra_info:
            for k, v in (extra_info or {}).items():
                entry[k] = v

        install_info[package_name] = entry

        INSTALL_INFO_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(INSTALL_INFO_FILE, 'w', encoding='utf-8') as f:
            json.dump(install_info, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"保存安装信息失败: {e}")
        return False


def extract_archive_fast(archive_path, extract_dir):
    """快速解压文件 - 优化版本"""
    try:
        # 预先检查文件类型
        if archive_path.lower().endswith('.zip'):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                # 只提取必要的文件，避免解压所有文件
                file_list = zip_ref.namelist()
                # 优先查找可执行文件和jar文件
                target_files = [f for f in file_list if
                                f.lower().endswith(('.exe', '.jar', '.bin')) or
                                os.path.basename(f) in ['bin', 'lib']]

                if target_files:
                    # 只解压目标文件
                    for file in target_files:
                        zip_ref.extract(file, extract_dir)
                else:
                    # 如果没有明确目标，解压所有文件
                    zip_ref.extractall(extract_dir)
        else:
            # 对于tar文件，使用流式解压
            with tarfile.open(archive_path, 'r:*') as tar_ref:
                tar_ref.extractall(extract_dir)

        return True
    except Exception as e:
        print(f"解压失败: {e}")
        return False


def install_package_optimized(package_name_or_url):
    """安装软件包 - 优化版本"""
    install_dir = Path(get_install_dir())
    install_dir.mkdir(parents=True, exist_ok=True)

    # 缓存平台信息
    current_platform = get_platform()

    # 获取下载URL - 使用字典查找优化
    if package_name_or_url in PREDEFINED_PACKAGES:
        package_info = PREDEFINED_PACKAGES[package_name_or_url]
        if current_platform not in package_info or package_info[current_platform] == "*":
            print(f"包 {package_name_or_url} 不支持当前平台")
            return False
        url = package_info[current_platform].replace("/blob/", "/raw/")
        package_name = package_name_or_url
    else:
        url = package_name_or_url
        package_name = Path(get_filename_from_url(url)).stem  # 去掉扩展名

    print(f"安装: {package_name}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # 下载文件
        archive_path = download_file_optimized(url, temp_path)
        if not archive_path or not archive_path.exists():
            return False

        # 检查文件类型并处理
        archive_suffix = archive_path.suffix.lower()
        is_archive = archive_suffix in ('.zip', '.tar', '.gz', '.bz2', '.tgz')

        if is_archive:
            extract_dir = temp_path / "extracted"
            extract_dir.mkdir(exist_ok=True)

            if not extract_archive_fast(str(archive_path), str(extract_dir)):
                return False

            # 查找jar文件（优先）
            jar_files = find_jar_files(str(extract_dir))
            if jar_files:
                return install_java_app(str(extract_dir), str(install_dir), package_name, jar_files)

            # 查找可执行文件
            executables = find_executables_fast(str(extract_dir))
            if executables:
                return install_executable(executables[0], str(install_dir), package_name)

            print("未找到可执行文件或jar文件")
            return False
        else:
            # 单个文件直接安装
            return install_single_file_optimized(str(archive_path), str(install_dir), package_name, url)


def find_executables_fast(directory):
    """快速查找可执行文件"""
    executables = []
    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                # 使用文件扩展名快速过滤
                if (
                    file.lower().endswith(('.exe', '.bin', '.bat', '.cmd', '.ps1', '.py', '.sh')) or
                    (get_platform() != "windows" and os.access(file_path, os.X_OK))
                ):
                    executables.append(file_path)
                    if len(executables) >= 3:  # 最多找3个
                        return executables
    except Exception as e:
        print(f"查找可执行文件时出错: {e}")

    return executables


def install_java_app(extract_dir, install_dir, package_name, jar_files):
    """安装Java应用程序"""
    try:
        package_dir = Path(install_dir) / package_name
        if package_dir.exists():
            shutil.rmtree(package_dir)

        # 复制整个目录
        shutil.copytree(extract_dir, package_dir)

        # 使用最大的jar文件
        main_jar = max(jar_files, key=lambda x: os.path.getsize(x))
        main_jar_name = Path(main_jar).name

        # 创建启动器
        if create_launcher(install_dir, package_name, str(package_dir / main_jar_name)):
            save_install_info(package_name, str(package_dir), is_jar=True)
            print(f"✓ Java应用安装完成: {package_name}")
            return True
    except Exception as e:
        print(f"安装Java应用失败: {e}")

    return False


def install_executable(exec_path, install_dir, package_name):
    """安装可执行文件"""
    try:
        exec_name = Path(exec_path).name
        dest_path = Path(install_dir) / exec_name

        shutil.copy2(exec_path, dest_path)

        if get_platform() != "windows":
            os.chmod(dest_path, 0o755)
        # 创建启动器（使用真实入口）
        if create_launcher(install_dir, package_name, exec_path=str(dest_path)):
            save_install_info(package_name, str(dest_path), is_jar=False, entry_path=str(dest_path))
            print(f"✓ 安装完成: {package_name}")
            return True
    except Exception as e:
        print(f"安装可执行文件失败: {e}")

    return False


def download_file_optimized(url, dest_dir):
    """优化下载函数"""
    filename = get_filename_from_url(url)
    dest_path = dest_dir / filename

    try:
        # 使用流式下载节省内存
        with urllib.request.urlopen(url) as response, open(dest_path, 'wb') as out_file:
            file_size = int(response.headers.get('Content-Length', 0))
            downloaded = 0
            chunk_size = 8192

            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                out_file.write(chunk)
                downloaded += len(chunk)

                # 显示进度（可选）
                if file_size > 0:
                    percent = (downloaded / file_size) * 100
                    print(f"\r下载进度: {percent:.1f}%", end="", flush=True)

            print()  # 换行

        return dest_path
    except Exception as e:
        print(f"下载失败: {e}")
        return None


def main():
    """主函数 - 显示可用包和命令"""
    if len(sys.argv) == 1:
        # 没有参数时显示帮助和可用包
        show_package_help()
        list_available_packages()
        return

    # 检查是否是已安装的包命令
    if len(sys.argv) > 1 and sys.argv[1] in load_install_info():
        package_name = sys.argv[1]
        install_dir = get_install_dir()
        launcher = os.path.join(install_dir, f"{package_name}.bat" if get_platform() == "windows" else package_name)
        args = " ".join(sys.argv[2:])
        if os.path.exists(launcher):
            os.system(f'"{launcher}" {args}')
        else:
            # 回退：直接按记录的入口执行
            details = load_install_info().get(package_name, {})
            entry_path = details.get("entry_path") or details.get("path")
            if entry_path:
                os.system(f'"{entry_path}" {args}')
            else:
                print(f"未找到包入口: {package_name}")
    else:
        # 其他命令
        parser = argparse.ArgumentParser(description='FANSe 工具集')
        subparsers = parser.add_subparsers(dest='command')

        # 安装命令
        install_parser = subparsers.add_parser('install', help='安装软件包')
        install_parser.add_argument('packages', nargs='*', help='要安装的包名')

        # 列表命令
        list_parser = subparsers.add_parser('list', help='列出可安装的包')
        installed_parser = subparsers.add_parser('installed', help='列出已安装的包')
        uninstall_parser = subparsers.add_parser('uninstall', help='卸载软件包')
        uninstall_parser.add_argument('packages', nargs='*', help='要卸载的包名')

        args = parser.parse_args()

        if args.command == 'install':
            handle_install_command(args)
        elif args.command == 'list':
            list_available_packages()
        elif args.command == 'installed':
            list_installed_packages()
        elif args.command == 'uninstall':
            uninstall_packages(args.packages or [])

# install.py


def add_install_subparser(subparsers):
    """添加install子命令解析器"""
    install_parser = subparsers.add_parser(
        'install',
        help='安装软件包',
        description='安装预定义软件包或从URL安装软件包'
    )
    install_parser.add_argument(
        'packages',
        nargs='*',
        help='要安装的包名或URL（使用"list"查看可用包）'
    )
    install_parser.add_argument(
        '--name', '-n',
        dest='name',
        help='安装后使用的自定义别名（默认用包名）'
    )
    install_parser.set_defaults(func=handle_install_command)
    return install_parser


def handle_install_command(args):
    """处理install命令（统一实现）"""
    if not args.packages:
        list_available_packages()
        list_installed_packages()
        return

    if len(args.packages) == 1 and args.packages[0] == 'list':
        list_available_packages()
        return

    for package in args.packages:
        print(f"\n正在处理: {package}")
        if install_package(package):
            print(f"✓ 安装成功: {package}")
            if getattr(args, 'name', None):
                alias = args.name
                if alias and alias != package:
                    install_dir = get_install_dir()
                    is_windows = get_platform() == "windows"
                    src = os.path.join(install_dir, f"{package}.bat" if is_windows else package)
                    dst = os.path.join(install_dir, f"{alias}.bat" if is_windows else alias)
                    try:
                        shutil.copy2(src, dst)
                        info = load_install_info()
                        if package in info:
                            info[package]["alias"] = alias
                            with open(INSTALL_INFO_FILE, 'w', encoding='utf-8') as f:
                                json.dump(info, f, indent=2, ensure_ascii=False)
                        print(f"✓ 已创建别名: {alias}")
                    except Exception as e:
                        print(f"创建别名失败: {e}")
        else:
            print(f"✗ 安装失败: {package}")


def uninstall_packages(packages):
    """卸载软件包"""
    info = load_install_info()
    changed = False
    for pkg in packages:
        details = info.get(pkg)
        if not details:
            print(f"未找到已安装包: {pkg}")
            continue
        try:
            install_dir = get_install_dir()
            launcher = os.path.join(install_dir, f"{pkg}.bat" if get_platform() == "windows" else pkg)
            if os.path.exists(launcher):
                os.remove(launcher)
            path = details.get("path")
            if path and os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.remove(path)
            info.pop(pkg, None)
            changed = True
            print(f"✓ 已卸载: {pkg}")
        except Exception as e:
            print(f"卸载失败 {pkg}: {e}")
    if changed:
        try:
            INSTALL_INFO_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(INSTALL_INFO_FILE, 'w', encoding='utf-8') as f:
                json.dump(info, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"更新安装信息失败: {e}")


if __name__ == "__main__":
    # main()
    # 测试代码
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_install_subparser(subparsers)
    args = parser.parse_args()

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()
