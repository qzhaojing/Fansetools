# fansetools/run.py
import os
import sys
import glob
import time
import logging
import configparser
import multiprocessing
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Union
from collections import OrderedDict

# 配置系统和路径管理
#class ConfigManager:
#    """配置管理器，处理配置文件和路径存储"""
#    
#    def __init__(self):
#        self.config_dir = self._get_config_dir()
#        self.config_file = self.config_dir / "fanse3.ini"
#        
#        # 确保配置目录存在
#        self.config_dir.mkdir(parents=True, exist_ok=True)
#        
#        # 初始化配置文件
#        if not self.config_file.exists():
#            self.config_file.touch()  # 创建空文件
#    
#    def _get_config_dir(self) -> Path:
#        """确定配置目录位置（兼容Windows和Linux）"""
#        if os.name == 'nt':  # Windows
#            return Path(os.getenv('APPDATA', '')).resolve() / 'Fansetools'
#        else:  # Linux/macOS
#            return Path.home() / '.config' / 'fansetools'
#    
#    def load_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
#        if not self.config_file.exists():
#            return default
#        
#        config = configparser.ConfigParser()
#        try:
#            config.read(self.config_file, encoding='utf-8')
#        except:
#            return default
#        
#        try:
#            # 如果配置文件中没有DEFAULT节，使用fallback
#            if config.has_section('DEFAULT'):
#                return config.get('DEFAULT', key, fallback=default)
#            return default
#        except:
#            return default
#    
#    def save_config(self, key: str, value: str):
#        config = configparser.ConfigParser()
#        # 尝试读取现有配置，如果文件不存在或无效，则忽略
#        try:
#            if self.config_file.exists() and self.config_file.stat().st_size > 0:
#                config.read(self.config_file, encoding='utf-8')
#        except:
#            # 如果读取失败，我们创建一个空的ConfigParser
#            config = configparser.ConfigParser()
#        
#        # 确保有DEFAULT节
#        if not config.has_section('DEFAULT'):
#            config.add_section('DEFAULT')
#        
#        config.set('DEFAULT', key, value)
#        
#        # 写入文件
#        try:
#            with open(self.config_file, 'w', encoding='utf-8') as f:
#                config.write(f)
#        except Exception as e:
#            # 处理错误，例如无法写入
#            raise RuntimeError(f"无法写入配置文件: {str(e)}")
class ConfigManager:
    """更健壮的配置管理器，避免任何节名问题"""
    
    def __init__(self):
        self.config_dir = self._get_config_dir()
        self.config_file = self.config_dir / "fanse3.cfg"  # 修改为.cfg以避免INI解析问题
        
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # 仅当文件不存在时创建
        if not self.config_file.exists():
            # 创建一个简单的键值对文件
            try:
                self.config_file.write_text("# FANSe3 配置文件\n\n")
            except Exception:
                pass
    
    def _get_config_dir(self) -> Path:
        """获取配置目录，优先使用用户级目录"""
        if os.name == 'nt':  # Windows
            return Path(os.environ.get('LOCALAPPDATA', os.environ.get('APPDATA', Path.home() / 'AppData'))) / 'Fansetools'
        else:  # Linux/macOS
            return Path.home() / '.config' / 'fansetools'
    
    def load_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """加载配置项 - 使用简单键值对格式"""
        if not self.config_file.exists():
            return default
        
        try:
            # 读取文件作为简单的键值对
            config_lines = self.config_file.read_text(encoding='utf-8').splitlines()
            config_dict = {}
            
            for line in config_lines:
                line = line.strip()
                # 跳过注释行和空行
                if not line or line.startswith('#'):
                    continue
                
                # 解析键值对
                if '=' in line:
                    key_part, value_part = line.split('=', 1)
                    key_part = key_part.strip()
                    value_part = value_part.strip()
                    config_dict[key_part] = value_part
            
            return config_dict.get(key, default)
        
        except Exception:
            return default
    
    def save_config(self, key: str, value: str):
        """保存配置项 - 使用简单键值对格式"""
        # 读取现有配置
        config_lines = []
        if self.config_file.exists():
            try:
                config_lines = self.config_file.read_text(encoding='utf-8').splitlines()
            except Exception:
                pass
        
        # 创建更新后的配置内容
        config_dict = {}
        updated = False
        
        for line in config_lines:
            line = line.strip()
            if not line or line.startswith('#'):
                # 保留注释和空行
                config_dict[line] = ""
                continue
            
            if '=' in line:
                key_part, value_part = line.split('=', 1)
                key_part = key_part.strip()
                value_part = value_part.strip()
                
                if key_part == key:
                    # 更新现有键
                    config_dict[key] = f"{key} = {value}"
                    updated = True
                else:
                    # 保留其他键
                    config_dict[key_part] = f"{key_part} = {value_part}"
        
        # 添加新键（如果尚未添加）
        if not updated:
            config_dict[key] = f"{key} = {value}"
        
        # 构建文件内容
        new_content = []
        for k, v in config_dict.items():
            if v:
                new_content.append(v)
            else:
                # 处理注释行
                new_content.append(k)
        
        # 写入文件
        try:
            self.config_file.write_text("\n".join(new_content), encoding='utf-8')
        except Exception as e:
            logging.error(f"保存配置失败: {str(e)}")


class FanseRunner:
    """FANSe3 批量运行器 - 支持多种输入输出模式和交互菜单"""
    
    def __init__(self):
        # 默认参数配置
        self.default_params = {
            'L': 1000,      # 最大读长
            'E': '5',       # 错误数量
            'S': 13,        # Seed长度
            'H': 1,         # 每批次读取reads数(百万)
            'C': max(1, multiprocessing.cpu_count() - 2)  # 默认核数(总核数-2)
        }
        self.default_options = ['--indel', '--rename']
        
        # 配置管理
        self.config = ConfigManager()
        
        # 日志初始化
        self._init_logger()
    
    def _init_logger(self):
        """初始化日志系统 - 移除所有特殊字符，确保兼容性"""
        self.logger = logging.getLogger('fanse.run')
        self.logger.setLevel(logging.INFO)
        
        # 创建日志格式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # 文件处理器
        log_file = self.config.config_dir / 'fanse_run.log'
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        except Exception as e:
            self.logger.error(f"无法创建日志文件: {str(e)}")
    
    def _normalize_path(self, path: Union[str, Path]) -> Path:
        """规范化路径处理，支持网络路径"""
        path = Path(path).absolute()
        
        # 处理Windows网络路径
        if os.name == 'nt' and str(path).startswith('\\\\'):
            return Path(str(path).replace('/', '\\'))
        
        return path
    
    def get_fanse3_path(self) -> Optional[Path]:
        """获取完整的FANSe可执行文件路径 - 简化验证逻辑"""
        path_str = self.config.load_config('fanse3dir')
        if not path_str:
            return None
        
        path = self._normalize_path(path_str)
        
        # 只要文件存在且可执行，直接返回（不再验证文件名）
        if path.exists():
            return path
        
        # 如果是目录，尝试查找任何exe文件
        if path.is_dir():
            for file in path.iterdir():
                if file.is_file() and file.suffix.lower() == '.exe':
                    return file
                
            # 没有找到exe文件，尝试任何可执行文件
            for file in path.iterdir():
                if file.is_file() and os.access(file, os.X_OK):
                    return file
            
        self.logger.warning(f"配置的FANSe路径无效或没有可执行文件: {path}")
        return None
    
    def set_fanse3_path(self, path: Union[str, Path]):
        """设置FANSe3路径（接受文件或目录）"""
        path = self._normalize_path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"路径不存在: {path}")
        
        # 保存配置（始终存储绝对路径）
        self.config.save_config('fanse3dir', str(path))
        self.logger.info(f"FANSe路径配置成功: {path}")
    
    # 其他方法保持不变（parse_input, generate_output_mapping等）...




# 命令行接口
def add_run_subparser(subparsers):
    """添加run子命令到主解析器 - 修复参数依赖"""
    parser = subparsers.add_parser(
        'run',
        help='批量运行FANSe3',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # 路径配置 - 独立操作，不需要其他参数
    parser.add_argument(
        '--set-path',
        metavar='PATH',
        help='配置FANSe可执行文件路径 (文件或目录)'
    )
    
    # 运行组 - 只有当不设置路径时需要这些参数
    run_group = parser.add_argument_group('运行参数')
    run_group.add_argument(
        '-i', '--input',
        help='输入文件/目录 (支持通配符，多个用逗号分隔)'
    )
    run_group.add_argument(
        '-r', '--refseq',
        help='参考序列文件路径'
    )
    run_group.add_argument(
        '-o', '--output',
        help='输出目录 (单个或与输入一一对应的多个，用逗号分隔)'
    )
    # 其他参数...
    
    parser.set_defaults(func=run_command)

def run_command(args):
    """处理run子命令 - 完全修复验证逻辑"""
    runner = FanseRunner()
    
    # 处理路径配置 (单独操作)
    if args.set_path:
        try:
            runner.set_fanse3_path(args.set_path)
            return
        except Exception as e:
            runner.logger.error(f"配置失败: {str(e)}")
            sys.exit(1)
    
    # 检查是否提供了运行参数
    if not args.input or not args.refseq:
        runner.logger.error("运行任务需要提供 -i/--input 和 -r/--refseq 参数")
        sys.exit(1)
    
    # 检查FANSe路径
    fanse_path = runner.get_fanse3_path()
    if not fanse_path:
        runner.logger.error("FANSe路径未配置或配置无效，请使用 --set-path 配置有效路径")
        sys.exit(1)
    
    # 验证FANSe可执行文件
    if not fanse_path.exists():
        runner.logger.error(f"配置的FANSe可执行文件不存在: {fanse_path}")
        sys.exit(1)
    
    # 其他运行逻辑保持不变...
    
    runner.logger.info(f"使用FANSe路径: {fanse_path}")
    
    # 解析输入
    input_paths = runner.parse_input(args.input)
    if not input_paths:
        runner.logger.error("未找到有效的输入文件")
        sys.exit(1)
    
    # 处理输出目录
    output_dirs = None
    if args.output:
        output_dirs = [Path(d.strip()) for d in args.output.split(',') if d.strip()]
    
    # 生成文件映射
    path_map = runner.generate_output_mapping(input_paths, output_dirs)
    
    # 构建并执行命令
    try:
        for input_path, output_path in path_map.items():
            # 构建命令
            cmd = runner.build_command(input_path, output_path, args.refseq)
            
            # 执行命令
            runner.logger.info(f"执行: {cmd}")
            os.system(cmd)
            
    except Exception as e:
        runner.logger.error(f"运行失败: {str(e)}")
        sys.exit(1)