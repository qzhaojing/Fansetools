# -*- coding: utf-8 -*-
"""
FANSe3 批量运行模块

功能：
1. 首次使用时设置 FANSe3 可执行文件路径
2. 使用相同参数批量运行多个文件夹中的 fastq 文件
3. 支持将结果保存到指定文件夹或 fastq 所在文件夹
4. 支持生成批处理文件或直接运行

使用方法：
1. 首次使用前调用 set_fanse3_path() 设置路径
2. 使用 FanseRunner 类创建运行实例并执行

示例：
>>> from fanse_run import FanseRunner
>>> runner = FanseRunner(
...     fastq_dirs=["path/to/fastq1", "path/to/fastq2"],
...     refseq="path/to/reference.fa",
...     output_dir="path/to/output",
...     params={"L": 1000, "E": "5", "S": 13, "H": 1},
...     options=["--indel", "--rename"]
... )
>>> runner.run()  # 直接运行
>>> runner.generate_bat()  # 生成批处理文件
"""

# fansetools/run.py
import os
import argparse
import sys
import glob
import time
import logging
import configparser
import multiprocessing
from pathlib import Path
from typing import List, Dict, Optional, Union
from collections import OrderedDict

# 配置文件和日志路径
CONFIG_DIR = Path(__file__).parent / "config"
CONFIG_FILE = CONFIG_DIR / "fanse3.ini"
LOG_FILE = CONFIG_DIR / "fanse3.log"

# 确保配置目录存在
CONFIG_DIR.mkdir(exist_ok=True)

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ConfigManager:
    """FANSe3配置管理器"""
    
    @staticmethod
    def get_config_path() -> Path:
        """获取配置文件路径"""
        return CONFIG_FILE
    
    @staticmethod
    def save_config(key: str, value: str):
        """保存配置项"""
        config = configparser.ConfigParser()
        if CONFIG_FILE.exists():
            config.read(CONFIG_FILE)
        
        if not config.has_section('DEFAULT'):
            config.add_section('DEFAULT')
        
        config.set('DEFAULT', key, value)
        
        with open(CONFIG_FILE, 'w') as f:
            config.write(f)
    
    @staticmethod
    def load_config(key: str, default: Optional[str] = None) -> Optional[str]:
        """加载配置项"""
        if not CONFIG_FILE.exists():
            return default
        
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        
        return config.get('DEFAULT', key, fallback=default)

class FanseRunner:
    """增强版FANSe3运行器"""
    
    def __init__(self):
        # 默认参数配置
        self.default_params = {
            'L': 1000,  # 最大读长
            'E': '5',   # 错误数量
            'S': 13,    # Seed长度
            'H': 1,     # 每批次读取reads数(百万)
            'C': max(1, multiprocessing.cpu_count() - 2)  # 默认核数(总核数-2)
        }
        self.default_options = ['--indel', '--rename']
        
        # 初始化配置
        self._init_config()
    
    def _init_config(self):
        """初始化配置"""
        if not CONFIG_FILE.exists():
            logger.info("初始化FANSe3配置文件")
            CONFIG_FILE.touch()
    
    #def _normalize_path(self, path: Union[str, Path]) -> Path:
    #    """规范化路径处理"""
    #    path = Path(path).absolute()
        #    
    #    # 处理网络路径(Windows)
    #    if os.name == 'nt' and str(path).startswith('\\\\'):
    #        return Path(str(path).replace('/', '\\'))
        #    
    #    return path
    
    def _normalize_path(self, path: Union[str, Path]) -> Path:
        path = Path(path).absolute()
        # 统一处理网络路径
        if os.name == 'nt' and str(path).startswith('\\\\'):
            return Path(str(path).replace('/', '\\'))
        return path
    
    def set_fanse3_path(self, exe_path: Union[str, Path]):
        """设置FANSe3路径（直接接受可执行文件路径）"""
        exe_path = self._normalize_path(exe_path)
        
        # 验证可执行文件是否存在
        if not exe_path.exists():
            raise FileNotFoundError(f"文件不存在: {exe_path}")
        
        # 检查是否是有效的可执行文件名
        valid_names = ['fanse3', 'fanse3g', 'fanse']
        if os.name == 'nt':
            valid_names = [name + '.exe' for name in valid_names]
        
        if exe_path.name not in valid_names:
            raise ValueError(
                f"无效的可执行文件名，应为: {', '.join(valid_names)}"
            )
        
        # 保存配置（存储所在目录）
        ConfigManager.save_config('fanse3dir', str(exe_path.parent))
        ConfigManager.save_config('fanse3exe', str(exe_path.name))
        
        print(f"✓ 配置成功: 找到 {exe_path.name} 可执行文件")
        print(f"  路径: {exe_path}")

    #def get_fanse3_path(self) -> Optional[Path]:
    #    """获取FANSe3可执行文件完整路径"""
    #    dir_path = ConfigManager.load_config('fanse3dir')
    #    exe_name = ConfigManager.load_config('fanse3exe')
        #    
    #    if not dir_path or not exe_name:
    #        return None
        #    
    #    path = self._normalize_path(dir_path) / exe_name
    #    if not path.exists():
    #        logger.warning(f"配置的FANSe3可执行文件不存在: {path}")
    #        return None
        #    
    #    return path
    
    def get_fanse3_path(self) -> Optional[Path]:
        """获取完整的FANSe可执行文件路径"""
        dir_path = ConfigManager.load_config('fanse3dir')
        exe_name = ConfigManager.load_config('fanse3exe', 'fanse3')  # 默认值
        
        if not dir_path:
            return None
        
        path = self._normalize_path(dir_path) / exe_name
        if not path.exists():
            logger.warning(f"配置的FANSe可执行文件不存在: {path}")
            return None
        
        return path
    
    def parse_input(self, input_str: str) -> List[Path]:
        """解析输入路径"""
        input_paths = []
        
        for item in input_str.split(','):
            item = item.strip()
            if not item:
                continue
            
            try:
                # 处理通配符
                if '*' in item or '?' in item:
                    matched = [self._normalize_path(p) for p in glob.glob(item)]
                    if not matched:
                        logger.warning(f"未找到匹配的文件: {item}")
                        continue
                    input_paths.extend(matched)
                else:
                    path = self._normalize_path(item)
                    if path.exists():
                        input_paths.append(path)
                    else:
                        logger.warning(f"路径不存在: {item}")
            except Exception as e:
                logger.error(f"解析输入路径失败: {item} - {str(e)}")
        
        return input_paths
    
    def generate_output_mapping(self, input_paths: List[Path], 
                              output_dirs: Optional[List[Path]] = None) -> Dict[Path, Path]:
        """生成输入输出路径映射"""
        path_map = OrderedDict()
        
        # 处理输出目录
        if not output_dirs:
            # 无输出目录，使用输入文件所在目录
            for path in input_paths:
                path_map[path] = path.parent
        elif len(output_dirs) == 1:
            # 单个输出目录
            output_dir = output_dirs[0]
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for path in input_paths:
                if path.is_file():
                    path_map[path] = output_dir
                else:
                    # 对于目录，保持相对路径结构
                    rel_path = path.relative_to(path.parent)
                    path_map[path] = output_dir / rel_path
        else:
            # 多个输出目录，必须与输入一一对应
            if len(input_paths) != len(output_dirs):
                raise ValueError("输入路径和输出目录数量不匹配")
            
            for input_path, output_dir in zip(input_paths, output_dirs):
                output_dir.mkdir(parents=True, exist_ok=True)
                path_map[input_path] = output_dir
        
        return path_map
    
    #def build_command(self, input_path: Path, output_path: Path, 
    #                 refseq: Path, params: Dict[str, Union[int, str]], 
    #                 options: List[str]) -> str:
    #    """构建FANSe命令（使用配置的可执行文件名）"""
    #    # 获取配置的可执行文件名
    #    exe_name = ConfigManager.load_config('fanse3exe', 'fanse3')  # 默认为fanse3
    #    exe_path = self.get_fanse3_path()
        #    
    #    if not exe_path:
    #        raise RuntimeError("未配置FANSe可执行文件路径，请先使用 --set-path 配置")
        #    
    #    # 构建基础命令
    #    cmd = f'"{exe_path}" -R"{refseq}" -D"{input_path}" -O"{output_path}"'
        #    
    #    # 添加参数
    #    for param, value in params.items():
    #        cmd += f" -{param}{value}"
        #    
    #    # 添加选项
    #    for option in options:
    #        cmd += f" {option}"
        #    
    #    return cmd
    
    def build_command(self, input_path: Path, output_path: Path, 
                     refseq: Path, params: Dict[str, Union[int, str]], 
                     options: List[str]) -> str:
        exe_path = self.get_fanse3_path()
        if not exe_path:
            raise RuntimeError("FANSe3路径未配置")
        
        if not refseq.exists():
            raise FileNotFoundError(f"参考序列文件不存在: {refseq}")
        
        # 跨平台路径处理
        cmd_parts = [
            f'"{exe_path}"',
            f'-R"{refseq}"',
            f'-D"{input_path}"',
            f'-O"{output_path}"'
        ]
        
        # 添加参数和选项
        cmd_parts.extend(f"-{param}{value}" for param, value in params.items())
        cmd_parts.extend(options)
        
        return " ".join(cmd_parts)
    
    def run_batch(self, file_map: Dict[Path, Path], refseq: Path,
                 params: Optional[Dict[str, Union[int, str]]] = None,
                 options: Optional[List[str]] = None):
        """批量运行FANSe3"""
        if not refseq.exists():
            raise FileNotFoundError(f"参考序列文件不存在: {refseq}")
        # 合并参数和选项
        final_params = {**self.default_params, **(params or {})}
        final_options = [*self.default_options, *(options or [])]
        
        # 显示配置信息
        logger.info("\nFANSe3 批量运行配置:")
        logger.info(f"  参考序列: {refseq}")
        logger.info(f"  输入文件: {len(file_map)} 个")
        logger.info(f"  参数: {final_params}")
        logger.info(f"  选项: {final_options}")
        
        # 执行运行
        total = len(file_map)
        success = 0
        
        for i, (input_path, output_dir) in enumerate(file_map.items(), 1):
            output_file = output_dir / f"{input_path.stem}.fanse3"
            
            logger.info(f"\n[{i}/{total}] 正在处理: {input_path.name}")
            logger.info(f"  输出文件: {output_file}")
            
            # 构建命令
            cmd = self.build_command(input_path, output_file, refseq, final_params, final_options)
            logger.debug(f"  执行命令: {cmd}")
            
            # 执行命令
            start_time = time.time()
            try:
                ret = os.system(cmd)
                elapsed = time.time() - start_time
                
                if ret == 0:
                    success += 1
                    logger.info(f"✓ 完成! 耗时: {elapsed:.2f}秒")
                else:
                    logger.error(f"✗ 失败! 耗时: {elapsed:.2f}秒")
            except Exception as e:
                logger.error(f"执行失败: {str(e)}")
        
        logger.info(f"\n处理完成: {success}/{total} 成功")

    def generate_bat(self, file_map: Dict[Path, Path], refseq: Path,
                    params: Optional[Dict] = None, options: Optional[List] = None,
                    bat_path: Optional[Path] = None):
        """生成批处理文件"""
        bat_path = bat_path or Path("run_fanse3.bat")
        
        with open(bat_path, 'w') as f:
            f.write("@echo off\n")
            for input_path, output_dir in file_map.items():
                cmd = self.build_command(input_path, output_dir, refseq, params, options)
                f.write(f"{cmd}\n")
        
        logger.info(f"批处理文件已生成: {bat_path}")

def add_run_subparser(subparsers):
    """添加run子命令到主解析器"""
    runner = FanseRunner()
    fanse3_path = runner.get_fanse3_path()
    
    parser = subparsers.add_parser(
        'run',
        help='批量运行FANSe3',
        formatter_class=argparse.RawTextHelpFormatter,
        description=f'''FANSe3 批量运行工具
当前FANSe路径: {fanse3_path or "未配置"}

使用示例:
  1. 单个文件
    fanse run -i sample.fastq -r reference.fa

  2. 多个文件
    fanse run -i sample1.fastq,sample2.fastq -r reference.fa

  3. 目录处理
    fanse run -i data/ -r reference.fa

  4. 多个目录
    fanse run -i data1/,data2/ -r reference.fa

  5. 通配符
    fanse run -i "data/*.fastq" -r reference.fa

  6. 指定单个输出目录
    fanse run -i data/ -o results/ -r reference.fa

  7. 指定多个输出目录
    fanse run -i data1/,data2/ -o results1/,results2/ -r reference.fa

  8. 自定义参数
    fanse run -i data/ -r reference.fa -L 1500 -E 3 -S 11 -C 8

  9. 添加选项
    fanse run -i data/ -r reference.fa --all --showalign

配置FANSe路径:
  fanse run --set-path /path/to/fanse3
'''
    )
    
    # 配置参数
    parser.add_argument(
        '--set-path',
        metavar='EXE_PATH',
        help='设置FANSe3可执行文件完整路径（如/path/to/fanse3.exe）'
    )
    
    # 必需参数
    required = parser.add_argument_group('必需参数')
    required.add_argument(
        '-i', '--input',
        required=True,
        help='输入文件/目录(支持通配符，多个用逗号分隔)'
    )
    required.add_argument(
        '-r', '--refseq',
        required=True,
        help='参考序列文件路径'
    )
    
    # 可选参数
    optional = parser.add_argument_group('可选参数')
    optional.add_argument(
        '-o', '--output',
        help='输出目录(单个或与输入一一对应的多个，用逗号分隔)'
    )
    
    # FANSe3参数
    params = parser.add_argument_group('FANSe3参数')
    params.add_argument(
        '-L', type=int,
        help='最大读长(默认: 1000)'
    )
    params.add_argument(
        '-E', type=str,
        help='错误数量(默认: 5)'
    )
    params.add_argument(
        '-S', type=int,
        help='Seed长度(默认: 13)'
    )
    params.add_argument(
        '-H', type=int,
        help='每批次读取reads数(百万)(默认: 1)'
    )
    params.add_argument(
        '-C', type=int,
        help=f'并行核数(默认: CPU核数-2)'
    )
    
    # FANSe3选项
    options = parser.add_argument_group('FANSe3选项')
    options.add_argument(
        '--all',
        action='store_true',
        help='输出所有最佳的mapping位点'
    )
    options.add_argument(
        '--unique',
        action='store_true',
        help='将unique和multi mapped reads分别输出'
    )
    options.add_argument(
        '--showalign',
        action='store_true',
        help='在结果中输出比对结果'
    )
    options.add_argument(
        '--test',
        action='store_true',
        help='以单线程模式运行'
    )
    
    parser.set_defaults(func=run_command)


def run_command(args):
    """处理run子命令"""
    runner = FanseRunner()
    
    try:
        # 处理路径配置
        if args.set_path:
            runner.set_fanse3_path(args.set_path)
            return
        
        # 检查FANSe3路径
        if not runner.get_fanse3_path():
            logger.error("错误: 未配置FANSe3路径，请先使用 --set-path 配置")
            return
        
        # 解析输入路径
        input_paths = runner.parse_input(args.input)
        if not input_paths:
            logger.error("错误: 未找到有效的输入路径")
            return
        
        # 解析输出目录(如果有)
        output_dirs = None
        if args.output:
            output_dirs = [Path(d.strip()) for d in args.output.split(',') if d.strip()]
        
        # 生成路径映射
        path_map = runner.generate_output_mapping(input_paths, output_dirs)
        
        # 准备参数
        params = {}
        if args.L is not None:
            params['L'] = args.L
        if args.E is not None:
            params['E'] = args.E
        if args.S is not None:
            params['S'] = args.S
        if args.H is not None:
            params['H'] = args.H
        if args.C is not None:
            params['C'] = args.C
        
        # 准备选项
        options = []
        if args.all:
            options.append('--all')
        if args.unique:
            options.append('--unique')
        if args.showalign:
            options.append('--showalign')
        if args.test:
            options.append('--test')
        
        # 显示交互菜单
        show_interactive_menu(runner, path_map, Path(args.refseq), params, options)
        
    except Exception as e:
        logger.error(f"运行失败: {str(e)}")
        sys.exit(1)

def show_interactive_menu(runner: FanseRunner, file_map: Dict[Path, Path], 
                         refseq: Path, params: Dict, options: List[str]):
    """显示交互式操作菜单"""
    while True:
        print("\n" + "="*50)
        print("FANSe3 操作菜单")
        print("="*50)
        print("当前配置:")
        print(f"  参考序列: {refseq}")
        print(f"  输入文件: {len(file_map)} 个")
        print(f"  参数: {params}")
        print(f"  选项: {options}")
        print("\n请选择操作:")
        print("1. 生成批处理文件")
        print("2. 直接运行处理")
        print("3. 重置 FANSe3 路径")
        print("4. 退出")
        
        choice = input("\n请输入选项 (1-4): ").strip()
        
        try:
            if choice == "1":
                output_dir = input("输入批处理文件保存目录(回车使用当前目录): ").strip()
                bat_path = Path(output_dir) / "run_fanse3.bat" if output_dir else None
                runner.generate_bat(file_map, refseq, params, options, bat_path)
                input("\n按Enter键继续...")
            elif choice == "2":
                print("\n即将开始处理，请确认配置:")
                print(f"  参考序列: {refseq}")
                print(f"  输入文件: {len(file_map)} 个")
                print(f"  参数: {params}")
                print(f"  选项: {options}")
                
                confirm = input("\n是否开始处理? (y/N): ").lower()
                if confirm == 'y':
                    runner.run_batch(file_map, refseq, params, options)
                    input("\n按Enter键返回菜单...")
            elif choice == "3":
                new_path = input("输入新的FANSe3可执行文件路径: ").strip()
                if new_path:
                    runner.set_fanse3_path(new_path)
                    input("\n按Enter键继续...")
            elif choice == "4":
                print("退出操作菜单")
                break
            else:
                print("无效选项，请重新输入")
        except Exception as e:
            print(f"\n操作失败: {str(e)}")
            input("按Enter键继续...")
            
#def run_command(args):
#    """处理run子命令"""
#    runner = FanseRunner()
#    
#    try:
#        # 处理路径配置
#        if args.set_path:
#            runner.set_fanse3_path(args.set_path)
#            return
#        
#        # 检查FANSe3路径
#        if not runner.get_fanse3_path():
#            logger.error("错误: 未配置FANSe3路径，请先使用 --set-path 配置")
#            return
#        
#        # 解析输入路径
#        input_paths = runner.parse_input(args.input)
#        if not input_paths:
#            logger.error("错误: 未找到有效的输入路径")
#            return
#        
#        # 解析输出目录(如果有)
#        output_dirs = None
#        if args.output:
#            output_dirs = [Path(d.strip()) for d in args.output.split(',') if d.strip()]
#        
#        # 生成路径映射
#        path_map = runner.generate_output_mapping(input_paths, output_dirs)
#        
#        # 准备参数
#        params = {}
#        if args.L is not None:
#            params['L'] = args.L
#        if args.E is not None:
#            params['E'] = args.E
#        if args.S is not None:
#            params['S'] = args.S
#        if args.H is not None:
#            params['H'] = args.H
#        if args.C is not None:
#            params['C'] = args.C
#        
#        # 准备选项
#        options = []
#        if args.all:
#            options.append('--all')
#        if args.unique:
#            options.append('--unique')
#        if args.showalign:
#            options.append('--showalign')
#        if args.test:
#            options.append('--test')
#        
#        # 运行批量处理
#        runner.run_batch(
#            file_map=path_map,
#            refseq=Path(args.refseq),
#            params=params,
#            options=options
#        )
#    except Exception as e:
#        logger.error(f"运行失败: {str(e)}")
#        sys.exit(1)

# 单元测试
if __name__ == '__main__':
    import unittest
    
    class TestFanseRunner(unittest.TestCase):
        def setUp(self):
            self.runner = FanseRunner()
            self.test_dir = Path(__file__).parent / "test_data"
            self.test_dir.mkdir(exist_ok=True)
            
            # 创建测试文件
            (self.test_dir / "test1.fastq").touch()
            (self.test_dir / "test2.fastq").touch()
        
        def test_path_normalization(self):
            path = self.runner._normalize_path("test/path")
            self.assertTrue(isinstance(path, Path))
        
        def test_parse_input(self):
            paths = self.runner.parse_input(f"{self.test_dir}/test1.fastq,{self.test_dir}/test2.fastq")
            self.assertEqual(len(paths), 2)
        
        def tearDown(self):
            # 清理测试文件
            for f in self.test_dir.glob("*"):
                f.unlink()
            self.test_dir.rmdir()
    
    unittest.main()