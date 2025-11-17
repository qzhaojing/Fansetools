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

# 预定义的软件包列表
PREDEFINED_PACKAGES = {
    "samtools": {
        "windows": "https://github.com/qzhaojing/Fansetools/blob/main/src/fansetools/bin/windows/samtools.exe",
        "linux": "https://github.com/samtools/samtools/releases/download/1.17/samtools-1.17.tar.bz2"
    },
    "bcftools": {
        "windows": "https://github.com/qzhaojing/Fansetools/blob/main/src/fansetools/bin/windows/bcftools.exe",
        "linux": "*"
    },
    "bwa": {
        "windows": "https://github.com/qzhaojing/Fansetools/blob/main/src/fansetools/bin/windows/bwa.exe",
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
    
    # 可以继续添加更多软件
}

def get_platform():
    """获取操作系统类型"""
    if platform.system() == "Windows":
        return "windows"
    elif platform.system() == "Linux":
        return "linux"
    else:
        raise NotImplementedError(f"不支持的系统: {platform.system()}")

def get_install_dir():
    """获取安装目录"""
    current_platform = get_platform()
    if current_platform == "windows":
        return os.path.join("src", "fansetools", "bin", "windows")
    else:  # linux
        return "/usr/local/bin"

def download_file(url, dest_path):
    """下载文件"""
    try:
        print(f"正在下载: {url}")
        urllib.request.urlretrieve(url, dest_path)
        print(f"下载完成: {dest_path}")
        return True
    except Exception as e:
        print(f"下载失败: {e}")
        return False

import requests
import os

def download_file_from_github(url, temp_file, expected_size=None):
    """
    从GitHub仓库下载文件。
    注意：如果文件使用LFS，这可能不会下载完整文件。
    """
    # 如果需要认证，添加令牌（例如：GitHub Personal Access Token）
    headers = {}
    # headers = {"Authorization": "token YOUR_GITHUB_TOKEN"}  # 取消注释并替换为您的令牌
    
    try:
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"文件下载成功: {temp_file}")
            
            # 检查文件大小
            file_size = os.path.getsize(temp_file)
            print(f"文件大小: {file_size} bytes")
            
            if expected_size and file_size < expected_size:
                print("警告: 文件可能不完整或为LFS指针。")
                return False
                
            return True
        else:
            print(f"下载失败，状态码: {response.status_code}")
            return False
    except Exception as e:
        print(f"下载过程中发生错误: {e}")
        return False


def is_zip_file(file_path):
    """检查是否为zip文件"""
    return file_path.lower().endswith(('.zip'))

def is_tar_file(file_path):
    """检查是否为tar文件"""
    return file_path.lower().endswith(('.tar.gz', '.tar.bz2', '.tgz', '.tar'))

def is_executable(file_path):
    """检查是否为可执行文件"""
    executable_extensions = ('.exe', '.bin', '')  # Linux可执行文件通常无扩展名
    return file_path.lower().endswith(executable_extensions) or os.access(file_path, os.X_OK)

def extract_archive(file_path, extract_to):
    """解压文件"""
    try:
        if file_path.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                print(f"ZIP文件解压到: {extract_to}")
        elif file_path.endswith(('.tar.gz', '.tgz', '.tar.bz2', '.tar')):
            with tarfile.open(file_path, 'r:*') as tar_ref:
                tar_ref.extractall(extract_to)
                print(f"TAR文件解压到: {extract_to}")
        return True
    except Exception as e:
        print(f"解压失败: {e}")
        return False

def find_executables(directory):
    """在目录中查找可执行文件"""
    executables = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if is_executable(file_path):
                executables.append(file_path)
    return executables

def install_package(package_name_or_url):
    """安装软件包"""
    install_dir = get_install_dir()
    current_platform = get_platform()
    
    # 创建安装目录
    os.makedirs(install_dir, exist_ok=True)
    
    # 判断是预定义包还是URL
    if package_name_or_url in PREDEFINED_PACKAGES:
        package_info = PREDEFINED_PACKAGES[package_name_or_url]
        if current_platform not in package_info:
            print(f"包 {package_name_or_url} 不支持当前平台 {current_platform}")
            return False
        
        url = package_info[current_platform]
    else:
        url = package_name_or_url
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = os.path.join(temp_dir, "download_file")
        
        # 下载文件
        #if not download_file(url, temp_file):
            # 示例使用
        if not download_file_from_github(url, temp_file):
            return False
        
        # 处理下载的文件
        if is_zip_file(url) or is_tar_file(url):
            # 解压文件
            extract_dir = os.path.join(temp_dir, "extracted")
            os.makedirs(extract_dir)
            
            if not extract_archive(temp_file, extract_dir):
                return False
            
            # 调试：打印解压后的文件结构
            print("解压后的文件结构:")
            for root, dirs, files in os.walk(extract_dir):
                level = root.replace(extract_dir, '').count(os.sep)
                indent = ' ' * 2 * level
                print(f'{indent}{os.path.basename(root)}/')
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    print(f'{subindent}{file}')
                    
            # 查找可执行文件
            executables = find_executables(extract_dir)
            if not executables:
                print("未找到可执行文件")
                return False
            
            # 复制可执行文件到安装目录
            for exec_path in executables:
                exec_name = os.path.basename(exec_path)
                dest_path = os.path.join(install_dir, exec_name)
                
                # 如果是Linux，确保文件有执行权限
                if current_platform == "linux":
                    os.chmod(exec_path, 0o755)
                
                # 复制文件
                shutil.copy2(exec_path, dest_path)
                print(f"已安装: {exec_name} -> {dest_path}")
            if not executables:
                return False
        else:
            # 直接复制可执行文件
            exec_name = os.path.basename(url)
            dest_path = os.path.join(install_dir, exec_name)
            
            # 复制文件
            shutil.copy2(temp_file, dest_path)
            
            # 如果是Linux，设置执行权限
            if current_platform == "linux":
                os.chmod(dest_path, 0o755)
            
            print(f"已安装: {exec_name} -> {dest_path}")
    
    print(f"安装完成: {package_name_or_url}")
    return True

def list_packages():
    """列出可安装的包"""
    print("可安装的预定义软件包:")
    print("-" * 20)
    for package_name in PREDEFINED_PACKAGES:
        print(f"  {package_name}")
    print("-" * 20)
    print("** 也可以直接使用URL安装任意软件包")
    print("示例: fanse install https://example.com/software.zip")

def show_install_help():
    """显示安装帮助"""
    print("fanse install - 软件包安装工具")
    print("=" * 50)
    print("用法:")
    print("  fanse install list                    # 列出可安装的包")
    print("  fanse install <package_name>         # 安装预定义包")
    print("  fanse install <url>                  # 从URL安装包")
    print("  fanse install <package1> <package2> # 安装多个包")
    print()
    print("示例:")
    print("  fanse install list")
    print("  fanse install samtools")
    print("  fanse install https://example.com/tool.tar.gz")
    print()
    list_packages()

def handle_install_command(args):
    """处理install命令"""
    # 如果没有参数，显示完整帮助
    if not args.packages:
        show_install_help()
        return
    
    # 如果第一个参数是list，只显示包列表
    if len(args.packages) == 1 and args.packages[0] == 'list':
        list_packages()
        return
    
    packages = args.packages

    for package in packages:
        print(f"\n正在处理: {package}")
        if not install_package(package):
            print(f"安装失败: {package}")

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
    return install_parser