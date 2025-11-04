# fansetools/bin_utils.py
import os
import sys
import platform
import subprocess
from pathlib import Path

class BinaryManager:
    """管理跨平台二进制工具"""
    
    def __init__(self):
        self.system = platform.system().lower()
        self.arch = platform.machine().lower()
        self.bin_dir = Path(__file__).parent / "bin"
        
    def get_samtools_path(self):
        """获取samtools路径"""
        if self.system == "windows":
            samtools_exe = self.bin_dir / "windows" / "samtools.exe"
        elif self.system == "linux":
            samtools_exe = self.bin_dir / "linux" / "samtools"
        elif self.system == "darwin":  # macOS
            samtools_exe = self.bin_dir / "macos" / "samtools"
        else:
            # 回退到系统PATH
            return "samtools"
        
        if samtools_exe.exists():
            return str(samtools_exe)
        else:
            # 如果内置版本不存在，使用系统PATH中的版本
            return "samtools"
    
    def check_samtools(self):
        """检查samtools是否可用"""
        samtools_path = self.get_samtools_path()
        try:
            result = subprocess.run([samtools_path, "--version"], 
                                 capture_output=True, text=True)
            return result.returncode == 0
        except:
            return False
    
    def run_samtools(self, args, check=True):
        """运行samtools命令"""
        samtools_path = self.get_samtools_path()
        cmd = [samtools_path] + args
        
        try:
            result = subprocess.run(cmd, check=check, capture_output=True, text=True)
            return result
        except subprocess.CalledProcessError as e:
            print(f"Samtools error: {e.stderr}")
            raise
        except FileNotFoundError:
            raise RuntimeError(f"Samtools not found at {samtools_path}")

# 创建全局实例
bin_manager = BinaryManager()