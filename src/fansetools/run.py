import os
import sys
import glob
import time
import logging
# import multiprocessing
import argparse
from .utils.rich_help import CustomHelpFormatter
from .distribute import distribute_command
from .utils.path_utils import PathProcessor
import gzip
import shutil
import tempfile
# from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple
from collections import OrderedDict

# pip install colorama
try:
    from colorama import init, Fore, Style
    init()  # Windows下启用颜色支持，没装也没关系，黑白显示就好了
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False
    if not HAS_COLORAMA:
        print("提示: 安装 colorama 可获得更好的彩色输出体验 (pip install colorama)")
# 在命令行添加 --debug 参数即可启用验证模式：


class ConfigManager:
    """配置管理器，使用自定义键值对格式存储配置"""

    def __init__(self):
        self.config_dir = self._get_config_dir()
        self.config_file = self.config_dir / "fanse3.cfg"

        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)

    def _get_config_dir(self) -> Path:
        """获取配置目录位置（兼容Windows和Linux）"""
        if os.name == 'nt':  # Windows
            # 使用LOCALAPPDATA或APPDATA
            appdata = os.environ.get('LOCALAPPDATA') or os.environ.get(
                'APPDATA') or os.path.expanduser("~")
            return Path(appdata) / 'Fansetools'
        else:  # Linux/macOS
            return Path.home() / '.config' / 'fansetools'

    def load_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """从配置文件加载配置项"""
        if not self.config_file.exists():
            return default

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception:
            return default

        config_dict = {}
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                k = k.strip()
                v = v.strip()
                config_dict[k] = v

        return config_dict.get(key, default)

    def save_config(self, key: str, value: str):
        """保存配置项到配置文件"""
        # 读取现有配置
        config_lines = []
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            except Exception:
                lines = []

            # 处理注释和空行
            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith('#'):
                    config_lines.append(line.rstrip())  # 保留原样
                else:
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        k = parts[0].strip()
                        v = parts[1].strip()
                        config_lines.append(f"{k} = {v}")

        # 更新或添加新的配置项
        updated = False
        new_config_lines = []
        for line in config_lines:
            if '=' in line:
                k, v = line.split('=', 1)
                k = k.strip()
                if k == key:
                    new_config_lines.append(f"{key} = {value}")
                    updated = True
                else:
                    new_config_lines.append(line)
            else:
                new_config_lines.append(line)

        if not updated:
            new_config_lines.append(f"{key} = {value}")

        # 写入文件
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(new_config_lines) + "\n")
        except Exception as e:
            print(f"保存配置失败: {str(e)}", file=sys.stderr)

    def _parse_ssh_path(self, path_str: str) -> Dict[str, str]:
        """解析SSH路径格式: user@host:/path/to/fanse3.exe"""
        import re
        pattern = r'^(?P<user>[^@]+)@(?P<host>[^:]+):(?P<path>.+)$'
        match = re.match(pattern, path_str)
        if match:
            return match.groupdict()
        return None

    def save_ssh_config(self, ssh_path: str, ssh_key: str = None, password: str = None):
        """保存SSH连接配置"""
        ssh_info = self._parse_ssh_path(ssh_path)
        if not ssh_info:
            raise ValueError(f"无效的SSH路径格式: {ssh_path}")

        self.save_config('fanse3_ssh_user', ssh_info['user'])
        self.save_config('fanse3_ssh_host', ssh_info['host'])
        self.save_config('fanse3_ssh_path', ssh_info['path'])

        if ssh_key:
            self.save_config('fanse3_ssh_key', ssh_key)
        if password:
            # 注意：密码存储需要加密，这里简化处理
            self.save_config('fanse3_ssh_password', password)

    def load_ssh_config(self) -> Optional[Dict[str, str]]:
        """加载SSH配置"""
        user = self.load_config('fanse3_ssh_user')
        host = self.load_config('fanse3_ssh_host')
        path = self.load_config('fanse3_ssh_path')

        if all([user, host, path]):
            return {
                'user': user,
                'host': host,
                'path': path,
                'key': self.load_config('fanse3_ssh_key'),
                'password': self.load_config('fanse3_ssh_password')
            }
        return None


# 替换现有的 SSHConnectionManager 类
class SSHConnectionManager:
    """SSH连接管理器"""

    def __init__(self, logger):
        self.logger = logger
        self.connection = None
        self.sftp = None

    def connect(self, ssh_config: Dict[str, str]) -> bool:
        """建立SSH连接"""
        try:
            import warnings
            from cryptography.utils import CryptographyDeprecationWarning
            warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
            import paramiko

            self.logger.info(
                f"正在连接SSH: {ssh_config['user']}@{ssh_config['host']}")

            # 创建SSH客户端
            self.connection = paramiko.SSHClient()
            self.connection.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())

            # 简化认证逻辑
            connect_kwargs = {
                'hostname': ssh_config['host'],
                'username': ssh_config['user'],
                'timeout': 30
            }

            # 优先尝试密码认证
            if ssh_config.get('password'):
                connect_kwargs['password'] = ssh_config['password']
            # 其次尝试密钥
            elif ssh_config.get('key'):
                key_file = Path(ssh_config['key']).expanduser()
                if key_file.exists():
                    private_key = paramiko.RSAKey.from_private_key_file(
                        str(key_file))
                    connect_kwargs['pkey'] = private_key

            self.connection.connect(**connect_kwargs)
            self.sftp = self.connection.open_sftp()

            self.logger.info(" SSH连接成功")
            return True

        except Exception as e:
            self.logger.error(f" SSH连接失败: {str(e)}")
            return False

    def execute_command(self, command: str) -> Tuple[bool, str]:
        """执行远程命令"""
        if not self.connection:
            return False, "SSH未连接"

        try:
            stdin, stdout, stderr = self.connection.exec_command(
                command, timeout=3600)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8', errors='ignore')
            error_output = stderr.read().decode('utf-8', errors='ignore')

            full_output = output + \
                ("\n" + error_output if error_output else "")

            if exit_status == 0:
                return True, full_output.strip()
            else:
                return False, f"Exit {exit_status}: {full_output}"

        except Exception as e:
            return False, f"命令执行失败: {str(e)}"

    def close(self):
        """关闭连接"""
        if self.sftp:
            self.sftp.close()
        if self.connection:
            self.connection.close()


class FanseRunner:
    """FANSe3 批量运行器 - 支持多种输入输出模式和交互菜单"""

    FANSE_EXECUTABLES = [
        "FANSe3g.exe", "FANSe3.exe", "FANSe3g", "FANSe3", "Fanse",
        "fanse3g.exe", "fanse3.exe", "fanse3g", "fanse3", "fanse",
    ]

    def _validate_output_intent(self, input_paths: List[Path], output_paths: Optional[List[Path]]) -> None:
        """验证输出路径意图并提供用户提示"""
        if not output_paths:
            return

        if len(output_paths) == 1 and len(input_paths) == 1:
            output_path = self._normalize_path(output_paths[0])

            # 检查是否是明显的文件路径（有.fanse3扩展名）
            if output_path.suffix == '.fanse3':
                self.logger.info(f"检测到文件输出模式: {output_path}")
            elif output_path.exists() and output_path.is_file():
                self.logger.info(f"使用现有文件作为输出: {output_path}")
            else:
                self.logger.info(f"检测到目录输出模式，将在目录内创建文件")

        elif len(output_paths) > 1:
            self.logger.info(f"多文件输出模式: {len(output_paths)} 个输出路径")

    def __init__(self, debug=False, log_path: Optional[Path] = None, show_progress: bool = True):
        # 如果没有colorama，提示

        # 默认参数配置
        # self.default_params = {
        #     'L': 1000,      # 最大读长
        #     'E': '5',       # 错误数量
        #     'S': 13,        # Seed长度
        #     'H': 1,         # 每批次读取reads数(百万)
        #     'C': max(1, multiprocessing.cpu_count() / 2)  # 默认核数(总核数/2)
        # }
        # self.default_options = ['--indel', '--rename']

        self.default_params = {
            # 'L': 1000,      # 最大读长
            # 'E': '5',       # 错误数量
            # 'S': 13,        # Seed长度
            # 'H': 1,         # 每批次读取reads数(百万)
            # 'C': int(max(1, multiprocessing.cpu_count() / 2)),  # 默认核数(总核数/2)
        }
        self.default_options = []

        # 配置管理
        self.config = ConfigManager()

        # 日志初始化
        # self._init_logger()
        self._init_logger(log_path)
        self.debug = debug  # 存储为实例属性
        
        # 路径处理器
        self.path_processor = PathProcessor(self.logger)

        # 处理工作目录
        self.temp_files: List[Path] = []  # 添加临时文件跟踪
        self.work_dir: Optional[Path] = None  # 添加work_dir属性
        self.ssh_manager = SSHConnectionManager(self.logger)
        self.remote_mode = False  # 新增远程模式标志
        self.show_progress = show_progress  # 新增参数控制是否显示进度条


# =============================================================================
# 配置集群SSH相关路径
# =============================================================================

    def set_remote_fanse3_path(self, ssh_path: str, ssh_key: str = None,
                               password: str = None, port: int = 22):
        """设置远程FANSe3路径（改进错误处理）"""
        try:
            # 验证SSH路径格式
            if not ssh_path or '@' not in ssh_path or ':' not in ssh_path:
                raise ValueError(
                    f"SSH路径格式不正确。正确格式: user@host:/path/to/fanse3.exe")

            # 记录认证方式
            auth_method = "密码" if password else ("密钥" if ssh_key else "自动检测")
            self.logger.info(f"尝试使用{auth_method}认证连接SSH...")

            # 保存SSH配置
            self.config.save_ssh_config(ssh_path, ssh_key, password)

            # 测试连接
            ssh_config = self.config.load_ssh_config()
            if not ssh_config:
                raise ValueError("SSH配置保存失败")

            if self.ssh_manager.connect(ssh_config):
                self.remote_mode = True

                # 验证远程FANSe3可执行文件
                remote_path = ssh_config['path']
                self.logger.info(f"验证远程FANSe3可执行文件: {remote_path}")

                success, output = self.ssh_manager.execute_command(
                    f'"{remote_path}" --version')

                if success:
                    version_info = output.strip() if output else "版本信息不可用"
                    self.logger.info(f"✅ 远程FANSe3验证成功: {version_info}")
                else:
                    self.logger.warning(f"⚠️ 远程FANSe3版本检查失败: {output}")
                    # 不阻止继续，可能版本命令不支持

                self.logger.info(f"✅ 远程FANSe3模式已启用: {ssh_path}")

            else:
                raise ConnectionError("SSH连接测试失败")

        except Exception as e:
            self.logger.error(f"❌ 设置远程路径失败: {str(e)}")
            # 提供详细的错误解决建议
            self._provide_ssh_troubleshooting(ssh_path, e)
            raise

    def _provide_ssh_troubleshooting(self, ssh_path: str, error: Exception):
        """提供SSH连接故障排除建议"""
        self.logger.info("\n🔧 SSH连接故障排除建议:")
        self.logger.info("1. 检查网络连接和服务器地址")
        self.logger.info("2. 验证用户名和密码/密钥是否正确")
        self.logger.info("3. 确保服务器SSH服务正在运行")
        self.logger.info("4. 检查防火墙设置")
        self.logger.info("5. 尝试手动SSH连接测试:")
        self.logger.info(f"   ssh {ssh_path.split(':')[0]}")

    def build_remote_command(self, input_file: Path, output_file: Path,
                             refseq: Path, params: Dict[str, Union[int, str]],
                             options: List[str]) -> str:
        """构建远程执行命令"""
        ssh_config = self.config.load_ssh_config()
        if not ssh_config:
            raise RuntimeError("未配置远程SSH路径")

        remote_path = ssh_config['path']

        # 处理路径映射（本地路径到远程路径）
        # 这里需要根据您的mount配置来映射路径
        remote_input = self._map_local_to_remote_path(input_file)
        remote_output = self._map_local_to_remote_path(output_file)
        remote_refseq = self._map_local_to_remote_path(refseq)

        # 构建远程命令
        cmd_parts = [
            f'"{remote_path}"',
            f'-R"{remote_refseq}"',
            f'-D"{remote_input}"',
            f'-O"{remote_output}"'
        ]

        # 添加参数和选项
        for param, value in params.items():
            cmd_parts.append(f"-{param}{value}")
        cmd_parts.extend(options)

        remote_command = " ".join(cmd_parts)

        # 记录调试信息
        self.logger.debug(f"远程命令: {remote_command}")
        return remote_command

    def _map_local_to_remote_path(self, local_path: Path) -> str:
        """将本地路径映射到远程路径"""
        local_str = str(local_path)

        # 添加您的具体映射规则
        mapping_rules = [
            # (本地路径前缀, 远程路径前缀)
            (r"\\fs2\D\DATA", "C:\\data"),  # 示例：将网络路径映射到C盘
            (r"/mnt/fs2/D", "/data"),        # Linux路径映射
        ]

        for local_prefix, remote_prefix in mapping_rules:
            if local_str.startswith(local_prefix):
                remaining = local_str[len(local_prefix):]
                remote_path = remote_prefix + remaining.replace('/', '\\')
                self.logger.info(f"路径映射: {local_str} -> {remote_path}")
                return remote_path

        # 如果没有匹配的规则，返回原路径
        self.logger.warning(f"没有找到路径映射规则，使用原路径: {local_str}")
        return local_str

    def run_remote_command(self, command: str) -> Tuple[bool, str, float]:
        """执行远程命令"""
        start_time = time.time()
        success, output = self.ssh_manager.execute_command(command)
        elapsed = time.time() - start_time
        return success, output, elapsed
# =============================================================================
# 配置工作目录tmp_dir
# =============================================================================

    def set_work_dir(self, work_dir: Optional[str]):
        """设置工作目录"""
        if not work_dir:
            self.work_dir = None
            return

        try:
            self.work_dir = self._prepare_work_dir(work_dir)
        except Exception as e:
            self.logger.error(f"设置工作目录失败: {str(e)}")
            self.work_dir = None

    def _prepare_work_dir(self, work_dir: Optional[str]) -> Optional[Path]:
        """准备并验证工作目录"""
        if not work_dir:
            return None

        # 转换为Path对象并创建目录
        work_path = Path(work_dir).resolve()

        # 验证路径
        if not work_path.exists():
            work_path.mkdir(parents=True, exist_ok=True)
        elif not work_path.is_dir():
            raise ValueError(f"指定路径不是目录: {work_path}")

        # 检查可写权限
        test_file = work_path / "write_test.tmp"
        try:
            test_file.touch()
            test_file.unlink()
        except OSError as e:
            raise PermissionError(f"无法写入指定目录 {work_path}: {str(e)}")

        self.logger.info(f"使用工作目录: {work_path}")
        return work_path

    def _cleanup(self):
        """清理所有临时文件"""
        for file in self.temp_files:
            try:
                if file.exists():
                    file.unlink()
                    self.logger.debug(f"已清理临时文件: {file}")
            except Exception as e:
                self.logger.warning(f"清理临时文件失败 {file}: {str(e)}")
        self.temp_files = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

# =============================================================================
# 配置日志
# =============================================================================
    def _init_logger(self, custom_log_path: Optional[Path] = None):
        """初始化日志系统"""
        self.logger = logging.getLogger('fanse.run')
        self.logger.setLevel(logging.INFO)

        # 创建日志格式 - 时间到秒（无毫秒）
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'  # 新增datefmt参数指定到秒
        )

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 确定日志文件路径
        if custom_log_path:
            # 使用自定义路径
            log_file = custom_log_path
        else:
            # 默认路径：配置目录下的 fanse_run.log
            log_file = self.config.config_dir / 'fanse_run.log'

        # 文件处理器
        try:
            # 确保日志目录存在
            log_file.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.info(f"日志文件: {log_file}")
        except Exception as e:
            self.logger.error(f"无法创建日志文件: {str(e)}")

    def _normalize_path(self, path: Union[str, Path]) -> Path:
        """规范化路径处理，完全支持UNC和所有Windows路径（添加引号处理）"""
        return self.path_processor._normalize_path(path)
# =============================================================================
# set the FANSe3 folder position
# =============================================================================

    def find_fanse_executable(self, directory: Path) -> Optional[Path]:
        """在目录中查找FANSe可执行文件"""
        for root, _, files in os.walk(directory):
            for file in files:
                if file in self.FANSE_EXECUTABLES:
                    return Path(root) / file
        return None

    def get_fanse3_path(self) -> Optional[Path]:
        """获取完整的FANSe可执行文件路径（修正目录处理）"""
        path_str = self.config.load_config('fanse3dir')
        if not path_str:
            return None

        path = self._normalize_path(path_str)

        # 如果是文件，直接返回
        if path.is_file():
            return path

        # 如果是目录，查找目录中的可执行文件
        if path.is_dir():
            executable = self.find_fanse_executable(path)
            if executable:
                return executable
            else:
                self.logger.warning(f"在目录中未找到FANSe可执行文件: {path}")
                return None

        # 路径不存在
        self.logger.warning(
            f"配置的FANSe路径不存在: {path}，\n请输入 'dir {path}' 检查文件是否存在，或路径是否可访问")
        return None

    def set_fanse3_path(self, path: Union[str, Path]):
        """设置FANSe3路径（自动查找可执行文件）"""
        path = self._normalize_path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"路径不存在: {path}，\n请输入 'dir {path}' 检查文件是否存在，或路径是否可访问")

        # 如果是目录，查找可执行文件
        if path.is_dir():
            executable = self.find_fanse_executable(path)
            if not executable:
                raise FileNotFoundError(f"目录中未找到FANSe可执行文件: {path}")
            path = executable

        # 保存配置
        self.config.save_config('fanse3dir', str(path))
        self.logger.info(f"FANSe路径配置成功: {path}")
# =============================================================================
# Generate the input and output file and folder
# =============================================================================

    def parse_input(self, input_str: str) -> List[Path]:
        """解析输入路径字符串，支持多种格式（修正Windows路径处理）"""
        self.logger.debug(f"原始输入字符串: {repr(input_str)}")  # 添加调试信息
        
        # 使用统一的 PathProcessor 处理
        paths = self.path_processor.parse_input_paths(input_str, self.path_processor.FASTQ_EXTENSIONS)
        
        self.logger.debug(f"最终解析的路径: {[str(p) for p in paths]}")
        return paths

# %% gzip and pigz

    def _handle_gzipped_input(self, input_file: Path) -> Tuple[Path, Optional[Path]]:
        """使用并行工具加速gzip解压缩"""
        if input_file.suffix != '.gz' and not (len(input_file.suffixes) > 1 and input_file.suffixes[-1] == '.gz'):
            return input_file, None

        try:
            # 检查系统是否安装并行解压工具
            if self._check_pigz_available():
                return self._decompress_with_pigz(input_file)
            else:
                # 回退到标准gzip
                return self._decompress_with_standard_gzip(input_file)

        except Exception as e:
            self.logger.error(f"解压文件失败: {input_file} - {str(e)}")
            raise

    def _handle_gzipped_input_with_cache(self, input_file: Path) -> Tuple[Path, Optional[Path]]:
        """带缓存机制的gzip解压"""
        if input_file.suffix != '.gz':
            return input_file, None

        # 计算文件哈希作为缓存标识
        file_hash = self._get_file_hash(input_file)
        cache_dir = self.work_dir / \
            "cache" if self.work_dir else Path(
                tempfile.gettempdir()) / "fanse_cache"
        cache_file = cache_dir / f"{file_hash}_{input_file.stem}.fastq"

        # 检查缓存是否存在且有效
        if cache_file.exists() and self._is_cache_valid(input_file, cache_file):
            self.logger.info(f"使用缓存文件: {cache_file}")
            return cache_file, None

        # 解压并缓存
        result_path, temp_path = self._decompress_with_pigz(
            input_file)  # 或标准gzip

        # 将解压结果保存到缓存
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(result_path, cache_file)
            self.logger.info(f"缓存已更新: {cache_file}")
        except Exception as e:
            self.logger.warning(f"缓存保存失败: {str(e)}")

        return result_path, temp_path

    # def _check_pigz_available(self) -> bool:
    #    """检查系统是否安装pigz（并行gzip工具）"""
    #    try:
    #        # 方法1: 使用shutil.which（更高效，无需创建子进程）
    #        import shutil
    #        if shutil.which('pigz') is not None:
    #            print('pigz is available')
    #            self._pigz_available_cache = True
    #            return True
    #    #except:
    #        import subprocess
    #        result = subprocess.run(['fanse', 'pigz'], capture_output=True, text=True)
    #        print('use pigz to unzip')
    #        return result.returncode == 0
    #    except:
    #        return False

    def _check_pigz_available(self) -> bool:
        """检查系统是否安装pigz（并行gzip工具）- 优先检查fanse pigz命令"""
        try:
            import subprocess
            import shutil

            # 方法0: 优先检查fanse pigz命令（新增）
            try:
                # 使用fanse pigz --version检查
                result = subprocess.run(
                    ['fanse', 'pigz', '--version'],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore',
                    timeout=5
                )
                if result.returncode == 0:
                    self.logger.info("✅ 检测到 fanse pigz 命令可用")
                    return True
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass  # 继续尝试其他方法

            # 方法1: 使用shutil.which检查系统pigz
            if shutil.which('pigz') is not None:
                return True

            # 方法2: 直接运行pigz --version（备用方案）
            result = subprocess.run(
                ['pigz', '--version'],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore',
                timeout=5
            )
            return result.returncode == 0

        except (FileNotFoundError, subprocess.TimeoutExpired, UnicodeDecodeError):
            # 只捕获特定异常
            return False
        except Exception:
            # 其他异常也返回False
            return False

    def _decompress_with_standard_gzip(self, input_file: Path) -> Tuple[Path, Optional[Path]]:
        """标准gzip解压 - 添加进度条版本"""
        custom_temp_dir = self.work_dir if self.work_dir else None
        if custom_temp_dir:
            custom_temp_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            prefix=f"{input_file.stem}_",
            suffix=".fastq",
            dir=custom_temp_dir,
            delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)

        self.logger.info(f"使用gzip解压: {input_file} -> {temp_path}")

        try:
            # 获取输入文件大小用于进度条
            total_size = input_file.stat().st_size

            # 使用tqdm进度条
            try:
                from tqdm import tqdm
            except ImportError:
                self.logger.warning("未安装tqdm，无法显示进度条")
                # 回退到无进度条版本
                with gzip.open(input_file, 'rb') as f_in:
                    with open(temp_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                # 使用tqdm显示进度条
                with gzip.open(input_file, 'rb') as f_in:
                    with open(temp_path, 'wb') as f_out:
                        with tqdm(total=total_size*5, unit='B', unit_scale=True,
                                  desc=f"解压 {input_file.name}", ncols=80) as pbar:
                            # 分块读取和写入，每块更新进度条
                            chunk_size = 1024 * 1024  # 1MB
                            while True:
                                chunk = f_in.read(chunk_size)
                                if not chunk:
                                    break
                                f_out.write(chunk)
                                pbar.update(len(chunk))

            # 验证解压结果
            if not temp_path.exists() or temp_path.stat().st_size == 0:
                raise ValueError("gzip解压失败")

            self.temp_files.append(temp_path)
            self.logger.info(f"✅ gzip解压成功")
            return temp_path, temp_path

        except Exception as e:
            self.logger.error(f"❌ gzip解压失败: {str(e)}")
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except:
                pass
            raise

    def _decompress_with_pigz(self, input_file: Path) -> Tuple[Path, Optional[Path]]:
        """使用pigz并行解压 - 添加进度条版本, 预估压缩比为5倍，gz文件大小*5，尝试"""
        custom_temp_dir = self.work_dir if self.work_dir else None
        if custom_temp_dir:
            custom_temp_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            prefix=f"{input_file.stem}_",
            suffix=".fastq",
            dir=custom_temp_dir,
            delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)

        try:
            import subprocess
            self.logger.info(f"使用pigz并行解压: {input_file} -> {temp_path}")

            # 使用fanse pigz命令
            cpu_count = min(os.cpu_count(), 8)
            cmd = ['fanse', 'pigz', '-d', '-c', '-p',
                   str(cpu_count), str(input_file)]

            # 获取输入文件大小用于进度条（进度可能不准确，但提供视觉反馈）
            total_size = input_file.stat().st_size

            try:
                from tqdm import tqdm
            except ImportError:
                self.logger.warning("未安装tqdm，无法显示进度条")
                # 回退到无进度条版本
                with open(temp_path, 'wb') as f_out:
                    result = subprocess.run(
                        cmd, stdout=f_out, check=True, timeout=3600)
            else:
                # 使用tqdm显示进度条
                with open(temp_path, 'wb') as f_out:
                    # 启动进程
                    process = subprocess.Popen(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                    with tqdm(total=total_size*5, unit='B', unit_scale=True,
                              desc=f"pigz解压 {input_file.name}", ncols=80) as pbar:
                        # 分块读取输出并更新进度条
                        chunk_size = 1024 * 1024  # 1MB
                        while True:
                            chunk = process.stdout.read(chunk_size)
                            if not chunk:
                                break
                            f_out.write(chunk)
                            pbar.update(len(chunk))

                    # 等待进程完成并检查返回值
                    stdout, stderr = process.communicate()
                    if process.returncode != 0:
                        raise subprocess.CalledProcessError(
                            process.returncode, cmd, stdout, stderr)

            # 验证解压结果
            if not temp_path.exists() or temp_path.stat().st_size == 0:
                raise ValueError("pigz解压失败")

            self.temp_files.append(temp_path)
            self.logger.info(f"✅ pigz解压成功")
            return temp_path, temp_path

        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ pigz解压失败，返回码: {e.returncode}")
            if e.stderr:
                self.logger.error(f"错误输出: {e.stderr.decode()}")
            return self._decompress_with_standard_gzip(input_file)
        except subprocess.TimeoutExpired:
            self.logger.error("❌ pigz解压超时")
            return self._decompress_with_standard_gzip(input_file)
        except Exception as e:
            self.logger.error(f"❌ pigz解压异常: {str(e)}")
            return self._decompress_with_standard_gzip(input_file)


# 解压不带进度条，带的是速度指示
    # def _decompress_with_standard_gzip(self, input_file: Path) -> Tuple[Path, Optional[Path]]:
    #    """标准gzip解压 - 使用不确定进度条"""
    #    custom_temp_dir = self.work_dir if self.work_dir else None
    #    if custom_temp_dir:
    #        custom_temp_dir.mkdir(parents=True, exist_ok=True)
        #
    #    with tempfile.NamedTemporaryFile(
    #        prefix=f"{input_file.stem}_",
    #        suffix=".fastq",
    #        dir=custom_temp_dir,
    #        delete=False
    #    ) as temp_file:
    #        temp_path = Path(temp_file.name)
        #
    #    self.logger.info(f"使用gzip解压: {input_file} -> {temp_path}")
        #
    #    try:
    #        # 使用不确定进度条（不显示百分比）
    #        try:
    #            from tqdm import tqdm
    #        except ImportError:
    #            self.logger.warning("未安装tqdm，无法显示进度条")
    #            # 回退到无进度条版本
    #            with gzip.open(input_file, 'rb') as f_in:
    #                with open(temp_path, 'wb') as f_out:
    #                    shutil.copyfileobj(f_in, f_out)
    #        else:
    #            # 使用不确定进度条
    #            with gzip.open(input_file, 'rb') as f_in:
    #                with open(temp_path, 'wb') as f_out:
    #                    with tqdm(total=None, unit='B', unit_scale=True,
    #                             desc=f"解压 {input_file.name}", ncols=80) as pbar:
    #                        # 分块读取和写入，更新进度条但不显示百分比
    #                        chunk_size = 1024 * 1024  # 1MB
    #                        while True:
    #                            chunk = f_in.read(chunk_size)
    #                            if not chunk:
    #                                break
    #                            f_out.write(chunk)
    #                            pbar.update(len(chunk))
        #
    #        # 验证解压结果
    #        if not temp_path.exists() or temp_path.stat().st_size == 0:
    #            raise ValueError("gzip解压失败")
        #
    #        self.temp_files.append(temp_path)
    #        self.logger.info(f"✅ gzip解压成功，解压后大小: {temp_path.stat().st_size} 字节")
    #        return temp_path, temp_path
        #
    #    except Exception as e:
    #        self.logger.error(f"❌❌ gzip解压失败: {str(e)}")
    #        try:
    #            if temp_path.exists():
    #                temp_path.unlink()
    #        except:
    #            pass
    #        raise
    #
    # def _decompress_with_pigz(self, input_file: Path) -> Tuple[Path, Optional[Path]]:
    #    """使用pigz并行解压 - 使用不确定进度条"""
    #    custom_temp_dir = self.work_dir if self.work_dir else None
    #    if custom_temp_dir:
    #        custom_temp_dir.mkdir(parents=True, exist_ok=True)
        #
    #    with tempfile.NamedTemporaryFile(
    #        prefix=f"{input_file.stem}_",
    #        suffix=".fastq",
    #        dir=custom_temp_dir,
    #        delete=False
    #    ) as temp_file:
    #        temp_path = Path(temp_file.name)
        #
    #    try:
    #        import subprocess
    #        self.logger.info(f"使用pigz并行解压: {input_file} -> {temp_path}")
        #
    #        # 使用fanse pigz命令
    #        cpu_count = min(os.cpu_count(), 50)
    #        cmd = ['fanse', 'pigz', '-d', '-c', '-p', str(cpu_count), str(input_file)]
        #
    #        try:
    #            from tqdm import tqdm
    #        except ImportError:
    #            self.logger.warning("未安装tqdm，无法显示进度条")
    #            # 回退到无进度条版本
    #            with open(temp_path, 'wb') as f_out:
    #                result = subprocess.run(cmd, stdout=f_out, check=True, timeout=3600)
    #        else:
    #            # 使用不确定进度条
    #            with open(temp_path, 'wb') as f_out:
    #                # 启动进程
    #                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #
    #                with tqdm(total=None, unit='B', unit_scale=True,
    #                         desc=f"pigz解压 {input_file.name}", ncols=80) as pbar:
    #                    # 分块读取输出并更新进度条（不确定模式）
    #                    chunk_size = 1024 * 1024  # 1MB
    #                    while True:
    #                        chunk = process.stdout.read(chunk_size)
    #                        if not chunk:
    #                            break
    #                        f_out.write(chunk)
    #                        pbar.update(len(chunk))
        #
    #                # 等待进程完成并检查返回值
    #                stdout, stderr = process.communicate()
    #                if process.returncode != 0:
    #                    raise subprocess.CalledProcessError(process.returncode, cmd, stdout, stderr)
        #
    #        # 验证解压结果
    #        if not temp_path.exists() or temp_path.stat().st_size == 0:
    #            raise ValueError("pigz解压失败")
        #
    #        self.temp_files.append(temp_path)
    #        self.logger.info(f"✅ pigz解压成功，解压后大小: {temp_path.stat().st_size} 字节")
    #        return temp_path, temp_path
        #
    #    except subprocess.CalledProcessError as e:
    #        self.logger.error(f"❌❌ pigz解压失败，返回码: {e.returncode}")
    #        if e.stderr:
    #            self.logger.error(f"错误输出: {e.stderr.decode()}")
    #        return self._decompress_with_standard_gzip(input_file)
    #    except subprocess.TimeoutExpired:
    #        self.logger.error("❌❌ pigz解压超时")
    #        return self._decompress_with_standard_gzip(input_file)
    #    except Exception as e:
    #        self.logger.error(f"❌❌ pigz解压异常: {str(e)}")
    #        return self._decompress_with_standard_gzip(input_file)

    def _validate_decompressed_file(self, file_path: Path) -> bool:
        """验证解压后的文件是否有效"""
        try:
            if not file_path.exists():
                self.logger.error("解压文件不存在")
                return False

            file_size = file_path.stat().st_size
            if file_size == 0:
                self.logger.error("解压文件为空")
                return False

            self.logger.info(f"✅ 解压文件验证通过，大小: {file_size} 字节")
            return True

        except Exception as e:
            self.logger.error(f"文件验证失败: {str(e)}")
            return False

    def _is_likely_fastq(self, data: bytes) -> bool:
        """检查数据是否可能是FASTQ格式"""
        try:
            text = data.decode('utf-8', errors='ignore')
            # 简单的FASTQ格式检查
            if '@' in text and '+' in text:
                return True
            return False
        except:
            return False

    def _get_file_hash(self, file_path: Path) -> str:
        """计算文件哈希值"""
        import hashlib
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _is_cache_valid(self, original_file: Path, cache_file: Path) -> bool:
        """检查缓存是否有效（基于文件修改时间）"""
        try:
            original_mtime = original_file.stat().st_mtime
            cache_mtime = cache_file.stat().st_mtime
            return cache_mtime > original_mtime
        except:
            return False

    def generate_output_mapping(self, input_paths: List[Path],
                                output_paths: Optional[List[Path]] = None) -> Dict[Path, Path]:
        """        
        生成输入输出路径映射（支持文件和文件夹输入）

        参数:
            input_paths: 输入路径列表（可以是文件或文件夹）
            output_paths: 可选输出路径列表

        返回:
            输入路径到输出路径的映射字典
        """
        # 验证输出意图
        # self._validate_output_intent(input_paths, output_paths)

        path_map = OrderedDict()

        # 展开所有输入路径（处理文件夹情况）
        expanded_inputs = []
        for path in input_paths:
            if path.is_file():
                expanded_inputs.append(path)
            elif path.is_dir():
                # 收集文件夹下所有文件（不递归）
                expanded_inputs.extend(
                    [f for f in path.iterdir() if f.is_file()])
            else:
                raise ValueError(f"路径既不是文件也不是文件夹: {path}")

        # 辅助函数：智能生成输出文件名
        def get_output_filename(input_file: Path) -> str:
            """根据输入文件名生成输出文件名，处理压缩文件扩展名"""
            stem = input_file.stem

            # 处理常见的压缩文件扩展名
            compress_exts = ['.gz', '.bz2', '.zip']
            for ext in compress_exts:
                if stem.endswith(ext):
                    stem = stem[:-len(ext)]

            # 处理常见的测序文件扩展名
            seq_exts = ['.fastq', '.fq', '.fa', '.fna', '.fasta']
            for ext in seq_exts:
                if stem.endswith(ext):
                    stem = stem[:-len(ext)]

            return f"{stem}.fanse3"

        # 智能识别输出路径类型
        if output_paths is None:
            # 没有指定输出路径，使用输入文件所在目录
            for path in expanded_inputs:
                output_file = path.with_name(get_output_filename(path))
                path_map[path] = output_file

        elif len(output_paths) == 1:
            # 单个输出路径 - 需要智能识别是文件还是文件夹
            output_path = self._normalize_path(output_paths[0])

            # 检查路径是否已存在
            if output_path.exists():
                if output_path.is_file():
                    # 如果输出路径是已存在的文件
                    if len(expanded_inputs) == 1:
                        # 单个输入对应单个文件输出
                        path_map[expanded_inputs[0]] = output_path
                    else:
                        # 多个输入不能输出到单个文件
                        raise ValueError(f"多个输入文件不能输出到单个文件: {output_path}")
                else:
                    # 输出路径是目录
                    for path in expanded_inputs:
                        output_file = output_path / get_output_filename(path)
                        path_map[path] = output_file
            else:
                # 路径不存在，通过扩展名判断意图
                if output_path.suffix == '.fanse3' and len(expanded_inputs) == 1:
                    # 有.fanse3扩展名且单个输入 - 视为文件输出
                    # 确保父目录存在
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    path_map[expanded_inputs[0]] = output_path
                else:
                    # 没有.fanse3扩展名或多个输入 - 视为目录
                    output_path.mkdir(parents=True, exist_ok=True)
                    for path in expanded_inputs:
                        output_file = output_path / get_output_filename(path)
                        path_map[path] = output_file

        else:
            # 多个输出路径（必须与输入数量匹配）
            if len(expanded_inputs) != len(output_paths):
                raise ValueError(
                    f"输入路径({len(expanded_inputs)})和输出路径({len(output_paths)})数量不匹配")

            for input_path, output_path in zip(expanded_inputs, output_paths):
                output_path = self._normalize_path(output_path)

                if output_path.exists() and output_path.is_file():
                    # 直接使用指定的文件路径
                    path_map[input_path] = output_path
                else:
                    # 视为目录或创建文件
                    if output_path.suffix == '.fanse3':
                        # 有.fanse3扩展名 - 视为文件
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        path_map[input_path] = output_path
                    else:
                        # 没有扩展名 - 视为目录
                        output_path.mkdir(parents=True, exist_ok=True)
                        output_file = output_path / \
                            get_output_filename(input_path)
                        path_map[input_path] = output_file

        # # 添加调试信息，帮助用户理解路径映射
        # self.logger.info("输出路径映射:")
        # for input_path, output_path in path_map.items():
        #     self.logger.info(f"  {input_path} -> {output_path}")

        #     # 如果输出路径是目录而不是文件，发出警告
        #     if output_path.exists() and output_path.is_dir():
        #         self.logger.warning(f"警告: 输出路径是目录而不是文件: {output_path}")
        #         self.logger.warning(f"      将在目录内创建: {get_output_filename(input_path)}")

        return path_map

# =============================================================================
# Start to integrate the paras  to single cmd
# =============================================================================

    def build_command(self, input_file: Path, output_file: Path,
                      refseq: Path, params: Dict[str, Union[int, str]],
                      options: List[str], fanse_path_override: str = None) -> str:
        """构建FANSe3命令 - 保证路径的引号使用，避免出错"""
        if fanse_path_override:
            fanse_path = fanse_path_override
        else:
            fanse_path = self.get_fanse3_path()
            if not fanse_path:
                raise RuntimeError(
                    "未配置FANSe路径，请使用fanse run --set-path /path    添加fanse.exe路径或所在文件夹")

        # 验证路径存在
        if not input_file.exists():
            raise FileNotFoundError(f"输入文件没找到: {input_file}")
        if not refseq.exists():
            raise FileNotFoundError(f"参考序列文件没找到: {refseq}")

        # 确保输出文件的父目录存在
        if not output_file.parent.exists():
            print(f"结果输出文件夹不存在，将新建: {refseq}")
            output_file.parent.mkdir(parents=True, exist_ok=True)

        cmd_fanseparts = [
            str(fanse_path),  # 直接使用字符串路径
            f'-R{str(refseq)}',    # 参数值直接拼接
            f'-D{str(input_file)}',
            f'-O{str(output_file)}'
        ]

        # 添加参数（确保格式一致）
        for param, value in params.items():
            # fanse参数和值之间千万不要添加空格，排查要死人的
            cmd_fanseparts.append(f"-{param}{value}")

        # 添加选项
        cmd_fanseparts.extend(options)

        # 记录最终命令用于调试
        final_cmd = " ".join(cmd_fanseparts)
        self.logger.debug(f"最终命令: {final_cmd}")
        return final_cmd

    def _print_task_info(self, task_info: str):
        """专用方法处理控制台的任务信息打印"""
        # 同时打印彩色（示例，假设我们有彩色支持）
        try:
            print(Fore.CYAN + task_info + Style.RESET_ALL)
        except ImportError:
            print(task_info)

    def log_path_diagnostics(self, path_name, path):
        """记录路径诊断信息"""
        self.logger.debug(
            f"生成命令路径格式 - 系统类型: {'Windows' if os.name == 'nt' else 'Linux'}")
        # self.logger.debug(f"可执行文件路径: {self._format_path_for_system(fanse_path)}")
        self.logger.debug(f"{path_name}: {path}")
        self.logger.debug(f"  绝对路径: {path.absolute()}")
        self.logger.debug(f"  真实路径: {path.resolve()}")
        self.logger.debug(f"  是否存在: {path.exists()}")

        if path.exists():
            self.logger.debug(f"  是文件: {path.is_file()}")
            self.logger.debug(f"  是目录: {path.is_dir()}")
        self.logger.debug(f"  父目录: {path.parent}")
        self.logger.debug(f"  父目录是否存在: {path.parent.exists()}")

    def validate_paths(self, path: Path, name: str,
                       is_file: bool = False, is_dir: bool = False
                       ) -> Tuple[bool, List[str]]:
        """集中验证路径，返回验证状态与错误信息"""
        errors = []

        # 1. 存在性检查
        if not path.exists():
            errors.append(f"{name}不存在: {path}")
            return False, errors

        # 2. 类型检查
        if is_file and not path.is_file():
            errors.append(f"{name}不是文件: {path}")
        if is_dir and not path.is_dir():
            errors.append(f"{name}不是目录: {path}")

        # 3. 路径长度检查（Windows限制）
        path_str = str(path.resolve())
        if len(path_str) > 150:  # 预警阈值
            errors.append(f"{name}路径过长（{len(path_str)}字符）: {path}")

        # 4. 可访问性检查（针对输出目录）
        if is_dir:
            test_file = path / "fanse_debug_test.tmp"
            try:
                test_file.touch()
                test_file.unlink()
            except PermissionError:
                errors.append(f"{name}目录不可写: {path}")

        return len(errors) == 0, errors

    def _prepare_reference_in_memory(self, refseq: Path) -> Path:
        """
        Check if reference should be cached in memory and perform caching.
        Returns the path to the cached reference or original reference.
        """
        # 1. Check if caching is feasible (size < 1GB)
        try:
            ref_size = refseq.stat().st_size
            if ref_size > 1024 * 1024 * 1024:  # > 1GB
                self.logger.info(f"参考序列过大 ({ref_size/1024/1024:.2f} MB > 1GB)，跳过内存缓存")
                return refseq
        except Exception as e:
            self.logger.warning(f"无法获取参考序列大小: {e}，跳过内存缓存")
            return refseq

        # 2. Determine memory cache directory
        import platform
        if platform.system() == "Linux":
            mem_base = Path("/dev/shm")
        else:
            # Windows or others: use temp dir (not true RAM disk unless configured, but faster than network)
            # User specifically asked for Windows solution. 
            # If no RAM disk, OS file caching usually handles this well, but we can copy to local temp
            # to avoid network IO if the original ref is on NFS/SMB.
            import tempfile
            mem_base = Path(tempfile.gettempdir())

        cache_dir = mem_base / "fanse_refs"
        
        # 3. Check available memory (need psutil)
        try:
            import psutil
            vm = psutil.virtual_memory()
            # Require at least 2x ref size available to be safe
            if vm.available < ref_size * 2:
                self.logger.warning(f"系统可用内存不足 ({vm.available/1024/1024:.2f} MB)，跳过参考序列内存缓存")
                return refseq
        except ImportError:
            self.logger.warning("未安装 psutil，无法检测内存，跳过内存缓存检查 (建议 pip install psutil)")
            # If psutil missing, proceed with caution or skip? 
            # User requirement: "如果检测到内存不够用，那么不采用这个方式"
            # So if we can't check, maybe we should skip to be safe, or just try if small enough.
            # Let's skip to be safe and avoid crashing.
            return refseq
        except Exception as e:
            self.logger.warning(f"内存检查失败: {e}，跳过内存缓存")
            return refseq

        # 4. Prepare cache path
        # Use hash of file path + mtime to ensure uniqueness and freshness
        ref_hash = self._get_file_hash(refseq) # Reuse existing hash method if suitable or just name
        # Actually _get_file_hash reads whole file, might be slow for 1GB. 
        # Use path + mtime hash is faster for identification.
        import hashlib
        identifier = f"{refseq.absolute()}_{refseq.stat().st_mtime}"
        name_hash = hashlib.md5(identifier.encode()).hexdigest()[:8]
        cache_file = cache_dir / f"{refseq.stem}_{name_hash}{refseq.suffix}"

        # 5. Copy if not exists
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            if not cache_file.exists():
                self.logger.info(f"正在将参考序列加载到内存/本地缓存: {cache_file} ...")
                import shutil
                shutil.copy2(refseq, cache_file)
                self.logger.info("参考序列缓存完成")
            else:
                self.logger.info(f"使用现有的参考序列缓存: {cache_file}")
                
            return cache_file
        except Exception as e:
            self.logger.warning(f"参考序列缓存失败: {e}，将使用原始路径")
            return refseq

    def run_batch(self, file_map: Dict[Path, Path], refseq: Path,
                  params: Optional[Dict[str, Union[int, str]]] = None,
                  options: Optional[List[str]] = None,
                  debug: bool = False,
                  yes: bool = False,  # 新增-y选项
                  resume: bool = False  # 新增-r选项
                  ):
        """批量运行FANSe3（添加执行确认选项）"""
        """批量运行FANSe3 - 支持远程模式"""
        # 合并参数和选项
        final_params = {**self.default_params, **(params or {})}
        final_options = [*self.default_options, *(options or [])]

        # 验证参考序列存在
        if not refseq.exists():
            raise FileNotFoundError(f"参考序列文件不存在: {refseq}")

        # 显示配置信息
        mode_info = " 远程模式" if self.remote_mode else " 本地模式"
        self.logger.info("\n" + "="*50)
        self.logger.info(f"FANSe3 运行配置- {mode_info}")
        self.logger.info(f"  参考序列: {refseq}")
        # self.logger.info(f"  输入文件夹: {len(file_map)} 个")
        self.logger.info(f"  输入文件: {len(file_map)} 个")
        self.logger.info(f"  参数: {final_params}")
        self.logger.info(f"  选项: {final_options}")
        self.logger.info("="*50)

        # 统计处理进度
        total = len(file_map)
        success = 0
        skipped = 0  # 新增：记录跳过的任务数
        failed = []

        # 执行模式控制
        run_mode = "confirm" if not yes else "auto"  # 如果指定了-y，则自动进入自动模式

        print("\n执行模式说明：")
        print(" - [y] 执行当前任务并继续")
        print(" - [a] 执行当前任务并切换到自动模式（执行所有剩余任务）")
        print(" - [n] 跳过当前任务，继续下一个")
        print(" - [q] 退出整个批处理")

        # 如果指定了--resume选项，则过滤掉已存在的输出文件
        if resume:
            filtered_map = OrderedDict()
            for input_path, output_path in file_map.items():
                if output_path.exists():
                    self.logger.info(f"跳过已存在输出文件: {output_path}")
                    skipped += 1
                else:
                    filtered_map[input_path] = output_path
            file_map = filtered_map
            total = len(file_map)
            self.logger.info(f"断点续运行模式: 跳过 {skipped} 个已完成任务，剩余 {total} 个任务")

        if debug:
            run_mode = "auto"
            self.logger.info("调试模式激活，进入自动执行模式")

        # 开始处理
        start_time = time.time()
        with self:
            for i, (original_input_file, output_file) in enumerate(file_map.items(), 1):
                # 构建命令

                temp_file = None

                # try:
                #     # 处理可能的gzipped输入
                #     input_file, temp_file = self._handle_gzipped_input(
                #         original_input_file)
                # except:
                #     # 如果检测不是gzfile，则还是input_file   (*.fastq)
                #     input_file = original_input_file

                # cmd = self.build_command(
                #     original_input_file, output_file, refseq, final_params, final_options)

                # 准备任务信息
                task_info = f"""
                            {'='*50}
                            任务 {i}/{total}: {original_input_file.name}
                            {'='*50}
                            原始输入文件: {original_input_file}
                            输出文件: {output_file}
                            参考序列: {refseq}
                            参数: {final_params}
                            选项: {final_options}
                            {'-'*50}
                            """
                # 命令: {cmd}
                # {'临时文件: ' + str(temp_file) if temp_file else 'None'}
                # 实际输入文件: {input_file}

                # 显示任务信息（调试模式下简化输出）
                if not debug:
                    # self.logger.info(task_info)
                    self._print_task_info(task_info)  # 专门处理控制台打印
                    # self.logger.info(task_info)       # 同时记录到日志
                    if temp_file and temp_file.exists():
                        try:
                            temp_file.unlink()
                            self.logger.info(f"已清理临时文件: {temp_file}")
                        except:
                            pass
                    if temp_file:
                        self.logger.info(f"临时文件将在完成后删除: {temp_file}")
                else:
                    print(task_info)  # 正常模式直接打印到控制台

                # =============== 新增路径验证逻辑 ===============
                if debug:
                    self.logger.info("调试模式激活 - 开始路径验证")
                    all_errors = []

                    # 统一调用验证方法
                    for path, name, check_type in [
                        (original_input_file, "输入文件", {"is_file": True}),
                        (refseq, "参考序列", {"is_file": True}),
                        # (output_file, "输出文件", {"is_file": True})
                    ]:
                        is_valid, errors = self.validate_paths(
                            path, name, **check_type)
                        all_errors.extend(errors)

                    # 输出验证报告
                    if not all_errors:
                        self.logger.info("✅ 所有路径验证通过")
                    else:
                        self.logger.error("🚫 路径验证失败：")
                        for error in all_errors:
                            self.logger.error(f"   - {error}")
                    return

                # =============== 调试逻辑结束 ===============

                # 模式处理逻辑（在非调试模式下）
                user_action = None
                if run_mode == "confirm":
                    # 只有在确认模式下才需要用户输入
                    response = ""
                    while response not in ['y', 'a', 'n', 'q']:
                        response = input(
                            "请选择操作 [y]自动执行所有/[a]执行本条/[n]跳过本条/[q]退出: ").strip().lower()
                        user_action = response

                    # 处理用户响应
                    if user_action == 'y':
                        self.logger.info("切换到自动模式，执行所有剩余任务")
                        run_mode = "auto"
                    elif response == 'a':
                        self.logger.info("用户选择执行此单条任务")
                    elif user_action == 'q':
                        self.logger.info("用户选择退出批处理")
                        break
                    elif user_action == 'n':
                        self.logger.info(f"跳过任务: {original_input_file.name}")
                        continue

                # 只有在需要执行任务时才处理文件
                if user_action in (None, 'y', 'a'):
                    try:
                        # 处理gzipped输入
                        input_file, temp_file = self._handle_gzipped_input(
                            original_input_file)

                        if self.remote_mode:
                            # 🌐🌐🌐🌐 远程模式执行 - 修复：这里必须实际执行远程命令
                            self.logger.info("🚀🚀 进入远程执行模式")

                            # 构建远程命令
                            remote_cmd = self.build_remote_command(
                                input_file, output_file, refseq, final_params, final_options
                            )

                            self.logger.info(f"🌐 远程命令: {remote_cmd}")

                            # 执行远程命令
                            success_flag, output, elapsed = self.run_remote_command(
                                remote_cmd)

                            if success_flag:
                                success += 1
                                self.logger.info(
                                    f"✅ 远程任务完成! 耗时: {elapsed:.2f}秒")
                                if output:
                                    self.logger.debug(f"远程输出: {output}")
                            else:
                                failed.append(original_input_file.name)
                                self.logger.error(
                                    f"❌❌ 远程任务失败! 错误: {output}, 耗时: {elapsed:.2f}秒")

                        else:
                            # 💻💻 本地模式执行
                            cmd = self.build_command(
                                input_file, output_file, refseq, final_params, final_options)
                            cmd_info = f"命令: {cmd}"
                            self.logger.info(cmd_info)

                            self.logger.info("开始执行命令...")
                            cmd_start_time = time.time()
                            ret = os.system(cmd)
                            elapsed = time.time() - cmd_start_time

                            if ret == 0:
                                success += 1
                                self.logger.info(
                                    f"✅ 本地任务完成! 耗时: {elapsed:.2f}秒")
                            else:
                                failed.append(original_input_file.name)
                                self.logger.error(
                                    f"❌❌ 本地任务失败! 返回码: {ret}, 耗时: {elapsed:.2f}秒")
                    except Exception as e:
                        failed.append(original_input_file.name)
                        self.logger.error(f"  处理异常: {str(e)}")
                    finally:
                        # 清理临时文件（如果创建了）
                        if temp_file and temp_file.exists():
                            try:
                                temp_file.unlink()
                                self.logger.info(f"已清理临时文件: {temp_file}")
                            except Exception as e:
                                self.logger.error(
                                    f"清理临时文件失败: {temp_file} - {str(e)}")

        # 汇总统计（美化显示）
        total_elapsed = time.time() - start_time
        if not resume:
            summary = f"\n{'='*50}\n处理完成: {success} 成功, {len(failed)} 失败\n总耗时: {total_elapsed:.2f}秒\n"
        elif resume:
            summary = f"\n{'='*50}\n处理完成: {success} 成功, {len(failed)} 失败, {skipped} 跳过\n总耗时: {total_elapsed:.2f}秒\n"

        self.logger.info(summary)
        if HAS_COLORAMA:
            print(Fore.CYAN + summary + Style.RESET_ALL)
        else:
            print(summary)

        if failed:
            self.logger.info("失败文件列表:")
            if HAS_COLORAMA:
                print(Fore.RED + "失败文件列表:" + Style.RESET_ALL)
            else:
                print("失败文件列表:")
            for name in failed:
                self.logger.info(f"  - {name}")
                if HAS_COLORAMA:
                    print(Fore.RED + f"  - {name}" + Style.RESET_ALL)
                else:
                    print(f"  - {name}")

# 在 FanseRunner 类中添加远程命令执行方法


def build_remote_command(self, input_file: Path, output_file: Path,
                         refseq: Path, params: Dict[str, Union[int, str]],
                         options: List[str]) -> str:
    """构建远程执行命令"""
    ssh_config = self.config.load_ssh_config()
    if not ssh_config:
        raise RuntimeError("未配置远程SSH路径")

    remote_path = ssh_config['path']

    # 构建远程命令
    cmd_parts = [
        f'"{remote_path}"',
        f'-R"{refseq}"',
        f'-D"{input_file}"',
        f'-O"{output_file}"'
    ]

    # 添加参数和选项
    for param, value in params.items():
        cmd_parts.append(f"-{param}{value}")
    cmd_parts.extend(options)

    remote_command = " ".join(cmd_parts)
    self.logger.info(f"🌐 远程命令: {remote_command}")
    return remote_command


def run_remote_command(self, command: str) -> Tuple[bool, str, float]:
    """执行远程命令"""
    start_time = time.time()
    success, output = self.ssh_manager.execute_command(command)
    elapsed = time.time() - start_time
    return success, output, elapsed

# 修改 run_batch 方法，添加远程执行逻辑


def run_batch(self, file_map: Dict[Path, Path], refseq: Path,
                  params: Optional[Dict[str, Union[int, str]]] = None,
                  options: Optional[List[str]] = None,
                  debug: bool = False,
                  yes: bool = False,
                  resume: bool = False):
    # 修正：缩进错误，将文档字符串的缩进从5个空格改为4个空格，与函数体保持一致
    """批量运行FANSe3 - 支持远程模式"""
    # 尝试将参考序列加载到内存（仅在非远程模式下，或者是通过distribute分发到节点本地执行时）
    # 如果是远程模式(self.remote_mode=True)，refseq已经是远程路径或者映射路径，
    # 且我们无法直接控制远程节点的内存加载（除非我们重写远程逻辑），这里暂只针对本地执行优化
    if not self.remote_mode:
        refseq = self._prepare_reference_in_memory(refseq)

    # 合并参数和选项
    final_params = {**self.default_params, **(params or {})}
    final_options = [*self.default_options, *(options or [])]
    

    # 验证参考序列存在
    if not refseq.exists():
        raise FileNotFoundError(f"参考序列文件不存在: {refseq}")

    # 显示配置信息
    mode_info = "🌐 远程模式" if self.remote_mode else "💻 本地模式"
    self.logger.info("\n" + "="*50)
    self.logger.info(f"FANSe3 运行配置 - {mode_info}")
    self.logger.info(f"  参考序列: {refseq}")
    self.logger.info(f"  输入文件: {len(file_map)} 个")
    self.logger.info(f"  参数: {final_params}")
    self.logger.info(f"  选项: {final_options}")
    self.logger.info("="*50)

    # 统计处理进度
    total = len(file_map)
    success = 0
    skipped = 0
    failed = []

    # 执行模式控制
    run_mode = "confirm" if not yes else "auto"

    if not debug:
        print("\n执行模式说明：")
        print(" - [y] 执行当前任务并继续")
        print(" - [a] 执行当前任务并切换到自动模式（执行所有剩余任务）")
        print(" - [n] 跳过当前任务，继续下一个")
        print(" - [q] 退出整个批处理")

    # 断点续运行模式
    if resume:
        filtered_map = OrderedDict()
        for input_path, output_path in file_map.items():
            if output_path.exists():
                self.logger.info(f"跳过已存在输出文件: {output_path}")
                skipped += 1
            else:
                filtered_map[input_path] = output_path
        file_map = filtered_map
        total = len(file_map)
        self.logger.info(f"断点续运行模式: 跳过 {skipped} 个已完成任务，剩余 {total} 个任务")

    if debug:
        run_mode = "auto"
        self.logger.info("调试模式激活，进入自动执行模式")

    # 开始处理
    start_time = time.time()
    with self:
        for i, (original_input_file, output_file) in enumerate(file_map.items(), 1):
            # 准备任务信息
            task_info = f"""
                        {'='*50}
                        任务 {i}/{total}: {original_input_file.name}
                        {'='*50}
                        输入文件: {original_input_file}
                        输出文件: {output_file}
                        参考序列: {refseq}
                        参数: {final_params}
                        选项: {final_options}
                        {'-'*50}
                        """

            # 显示任务信息
            if not debug:
                self._print_task_info(task_info)
            else:
                print(task_info)

            # 调试模式路径验证
            if debug:
                self.logger.info("调试模式激活 - 开始路径验证")
                all_errors = []
                for path, name, check_type in [
                    (original_input_file, "输入文件", {"is_file": True}),
                    (refseq, "参考序列", {"is_file": True}),
                ]:
                    is_valid, errors = self.validate_paths(
                        path, name, **check_type)
                    all_errors.extend(errors)

                if not all_errors:
                    self.logger.info("✅ 所有路径验证通过")
                else:
                    self.logger.error("🚫 路径验证失败：")
                    for error in all_errors:
                        self.logger.error(f"   - {error}")
                return

            # 模式处理逻辑
            user_action = None
            if run_mode == "confirm":
                response = ""
                while response not in ['y', 'a', 'n', 'q']:
                    response = input(
                        "请选择操作 [y]自动执行所有/[a]执行本条/[n]跳过本条/[q]退出: ").strip().lower()
                    user_action = response

                if user_action == 'y':
                    self.logger.info("切换到自动模式，执行所有剩余任务")
                    run_mode = "auto"
                elif response == 'a':
                    self.logger.info("用户选择执行此单条任务")
                elif user_action == 'q':
                    self.logger.info("用户选择退出批处理")
                    break
                elif user_action == 'n':
                    self.logger.info(f"跳过任务: {original_input_file.name}")
                    continue

            # 执行任务
            if user_action in (None, 'y', 'a'):
                try:
                    # 处理gzipped输入
                    input_file, temp_file = self._handle_gzipped_input(
                        original_input_file)

                    if self.remote_mode:
                        # 🌐🌐 远程模式执行
                        remote_cmd = self.build_remote_command(
                            input_file, output_file, refseq, final_params, final_options
                        )
                        self.logger.info(f"🚀 开始远程执行...")

                        success_flag, output, elapsed = self.run_remote_command(
                            remote_cmd)

                        if success_flag:
                            success += 1
                            self.logger.info(f"✅ 远程任务完成! 耗时: {elapsed:.2f}秒")
                            if output:
                                self.logger.debug(f"远程输出: {output}")
                        else:
                            failed.append(original_input_file.name)
                            self.logger.error(
                                f"❌ 远程任务失败! 错误: {output}, 耗时: {elapsed:.2f}秒")

                    else:
                        # 💻 本地模式执行
                        cmd = self.build_command(
                            input_file, output_file, refseq, final_params, final_options)
                        cmd_info = f"命令: {cmd}"
                        self.logger.info(cmd_info)

                        self.logger.info("开始执行命令...")
                        cmd_start_time = time.time()
                        ret = os.system(cmd)
                        elapsed = time.time() - cmd_start_time

                        if ret == 0:
                            success += 1
                            self.logger.info(f"✅ 本地任务完成! 耗时: {elapsed:.2f}秒")
                        else:
                            failed.append(original_input_file.name)
                            self.logger.error(
                                f"❌ 本地任务失败! 返回码: {ret}, 耗时: {elapsed:.2f}秒")

                except Exception as e:
                    failed.append(original_input_file.name)
                    self.logger.error(f"❌ 处理异常: {str(e)}")
                finally:
                    # 清理临时文件
                    if 'temp_file' in locals() and temp_file and temp_file.exists():
                        try:
                            temp_file.unlink()
                            self.logger.info(f"已清理临时文件: {temp_file}")
                        except Exception as e:
                            self.logger.error(
                                f"清理临时文件失败: {temp_file} - {str(e)}")

    # 汇总统计
    total_elapsed = time.time() - start_time
    summary = f"\n{'='*50}\n处理完成: {success} 成功, {len(failed)} 失败"
    if resume:
        summary += f", {skipped} 跳过"
    summary += f"\n总耗时: {total_elapsed:.2f}秒\n"

    self.logger.info(summary)
    if HAS_COLORAMA:
        print(Fore.CYAN + summary + Style.RESET_ALL)
    else:
        print(summary)

    if failed:
        self.logger.info("失败文件列表:")
        for name in failed:
            self.logger.info(f"  - {name}")


class PathMapper:
    """路径映射器 - 处理本地与远程路径的转换"""

    def __init__(self, mapping_rules: List[Tuple[str, str]] = None):
        self.mapping_rules = mapping_rules or []

    def add_mapping(self, local_prefix: str, remote_prefix: str):
        """添加路径映射规则"""
        self.mapping_rules.append((local_prefix, remote_prefix))

    def local_to_remote(self, local_path: Union[str, Path]) -> str:
        """本地路径转远程路径"""
        local_str = str(local_path)

        for local_prefix, remote_prefix in self.mapping_rules:
            if local_str.startswith(local_prefix):
                remaining = local_str[len(local_prefix):]
                # 处理路径分隔符转换
                if remote_prefix.upper().startswith('C:'):
                    # Windows路径
                    remote_path = remote_prefix + remaining.replace('/', '\\')
                else:
                    # Linux路径
                    remote_path = remote_prefix + remaining
                return remote_path

        return local_str  # 默认返回原路径

    def remote_to_local(self, remote_path: str) -> Path:
        """远程路径转本地路径"""
        for local_prefix, remote_prefix in self.mapping_rules:
            if remote_path.startswith(remote_prefix):
                remaining = remote_path[len(remote_prefix):]
                if remote_prefix.upper().startswith('C:'):
                    # 从Windows路径转换
                    local_path = local_prefix + remaining.replace('\\', '/')
                else:
                    local_path = local_prefix + remaining
                return Path(local_path)

        return Path(remote_path)  # 默认返回原路径


# %% 命令行接口
def add_run_subparser(subparsers):
    """添加run子命令到主解析器"""
    parser = subparsers.add_parser(
        'run',
        help='批量运行FANSe3',
        description='''[bold]FANSe3 批量运行工具[/bold]

支持多种输入输出模式:  单个文件与目录形式均可，可批量运行

  [cyan]-i sample.fq 文件:[/cyan]
      直接处理单个或多个文件。支持 .gz 读取，会先解压到本地/服务器临时目录后输入fanse3比对。
      可输入多个文件，用逗号隔开。
      [dim]例如: /path/sample.fastq;/path/sample.fq.gz[/dim]

  [cyan]-i /path/ 目录:[/cyan]
      如输入目录，则处理目录下所有 fastq/fq/fq.gz/fastq.gz。
      可同时输入多个目录，用逗号隔开。

  [cyan]-i /*_R1.fq 通配符:[/cyan]
      使用通配符选择文件，为高效筛选目录中所需文件，可使用*号进行筛选。
      [dim]例如: /path/*R1.fastq.gz[/dim]

[bold]输出目录控制:[/bold]
  不指定: 输出到输入文件所在目录
  单目录: 所有输出保存到同一目录
  多目录: 与输入一一对应的输出目录

  [yellow]如多目录，最好文本文件记录好命令再运行。[/yellow]
''',
        formatter_class=CustomHelpFormatter
    )

    #parser = subparsers.add_parser('run', help='批量运行FANSe3')

    # 添加work_dir配置 (新增)
    parser.add_argument(
        '-w', '--work-dir',
        type=str,
        default=None,
        help="配置临时工作目录，用于存放解压等操作产生的临时文件"
    )

    # 必需参数（当不设置路径时）
    parser.add_argument(
        '-i', '--input',
        required=False,  # 改为非必需
        help='输入文件/目录 (支持通配符，多个用逗号分隔)'
    )
    parser.add_argument(
        '-r', '--refseq',
        required=False,  # 改为非必需
        help='参考序列文件路径'
    )

    # 可选参数
    parser.add_argument(
        '-o', '--output',
        help='输出目录 (单个或与输入一一对应的多个，用逗号分隔).输出文件或目录（如果是目录会自动添加 input.fanse3）'
    )

    # 新增日志配置参数
    parser.add_argument(
        '--log',
        help='指定日志文件路径（默认保存在配置目录）'
    )
    # 在命令行解析代码中（例如 main.py 或 cli.py）
    parser.add_argument(
        '--debug', action='store_true',
        help='启用调试模式：验证各个路径是否正确，但不运行命令')

    # FANSe3参数
    parser.add_argument(
        '-O', type=str, metavar='output',
        help='结果输出文件夹 (不指定：输入文件夹)'
    )
    parser.add_argument(
        '-L', type=int, metavar='LENGTH',
        help='最大读长 (默认: 1000)'
    )
    parser.add_argument(
        '-E', type=str, metavar='MISMATCH',
        help='错误数量 (默认: 5)'
    )
    parser.add_argument(
        '-S', type=int, metavar='min_LENGTH',
        help='Seed长度 (默认: 13)，不建议设置低于10，速度很慢'
    )
    parser.add_argument(
        '-H', type=float, metavar='MILLION READS',
        help='比对时每批次读取fastq的reads数(百万) (默认: 1)，可以为小数，例如0.01'
    )
    parser.add_argument(
        '-C', type=int, metavar='CORES',
        help='并行核数 (默认: 现有CPU总核数-2)'
    )
    parser.add_argument(
        '-T', type=str, metavar='TRIM READS',
        help='对read进行预处理，切除不用的区域。从第start位开始切割，向后保留length长度：START,LENGTH (默认: 0,150)'
    )
    parser.add_argument(
        '-I', type=int, metavar='INDEL 0,1',
        help='不开启0,开启1(默认: 0)'
    )
    # FANSe3选项
    parser.add_argument(
        '--all',
        action='store_true',
        help='输出每条read的所有最佳的mapping位点'
    )
    parser.add_argument(
        '--unique',
        action='store_true',
        help='将unique和multi mapped reads分别输出'
    )
    parser.add_argument(
        '--showalign',
        action='store_true',
        help='在结果中输出比对结果'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='以单线程模式运行'
    )
    parser.add_argument(
        '--rename',
        action='store_true',
        help='启用reads改名，改为1，2，3，4……，减小结果文件大小'
    )
    parser.add_argument(
        '--indel',
        action='store_true',
        help='启用indel比对，结果更精细，耗时加倍'
    )

    parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='无需确认直接执行所有任务'
    )

    parser.add_argument(
        '--resume',
        dest='resume',
        action='store_true',
        help='断点续运行模式（跳过已存在的输出文件）'
    )

    # 集群运行参数
    cluster_group = parser.add_argument_group('集群运行参数，通过fanse cluster list查看，设置')
    cluster_group.add_argument(
        '--cluster',
        action='store_true',
        help='开启集群模式：将任务分发到配置的计算节点运行'
    )
    cluster_group.add_argument(
        '-n', '--nodes',
        metavar='NODES',
        help='指定运行节点（逗号分隔），默认使用所有可用节点'
    )
    cluster_group.add_argument(
        '--timeout',
        type=int,
        default=60,
        help='任务超时时间（默认60秒），0表示无超时'
    )

    # 新增SSH相关参数============================================================

    # 创建互斥组，确保不同模式不冲突
    path_mode_group = parser.add_mutually_exclusive_group()

    # 本地路径配置
    path_mode_group.add_argument(
        '--set-path',
        metavar='PATH',
        help='配置本地FANSe可执行文件路径'
    )


    # ==========================
    # SSH认证参数组
    ssh_auth_group = parser.add_argument_group('SSH相关参数')

    # SSH路径配置
    ssh_auth_group.add_argument(
        '--set-ssh-path',
        metavar='USER@HOST:PATH',
        help='配置远程FANSe3路径 (格式: user@host:/path/to/fanse3.exe)，亦可用于linux下通过SSH配置win系统的fanse3.exe路径实现调用'
    )
    ssh_auth_group.add_argument(
        '--ssh-key',
        metavar='PATH',
        help='SSH私钥文件路径（优先使用密钥认证）'
    )

    ssh_auth_group.add_argument(
        '--ssh-password',
        help='SSH密码（如果提供密钥，则忽略密码）'
    )

    ssh_auth_group.add_argument(
        '--ssh-port',
        type=int,
        default=22,
        help='SSH端口 (默认: 22)'
    )
    ssh_auth_group.add_argument(
        '--ssh',
        dest='ssh',
        action='store_true',
        help='采用远程fanse调用模式（不好用，使用场景不对）'
    )
    # 新增 --remote-ssh 参数，用于显式启用远程模式
    ssh_auth_group.add_argument(
        '--remote-ssh',
        action='store_true',
        help='强制启用远程SSH模式，跳过本地FANSe路径检查（等同于 --ssh，但意图更明确）'
    )
    # 路径配置
    # parser.add_argument(
    #    '--set-path',
    #    metavar='PATH',
    #    help='配置FANSe可执行文件路径 (文件或目录)'
    # )

    # 修改现有的set-path处理逻辑
    def run_command(args):
        # 处理日志路径
        log_path = Path(args.log) if args.log else None
        if args.log:
            try:
                log_path = Path(args.log)
                if log_path.is_dir():
                    log_path = log_path / 'fanse_run.log'
            except Exception as e:
                print(f"警告: 指定的日志路径无效 - {str(e)}")

        # 创建运行器实例 - 移到函数内部
        runner = FanseRunner(log_path=log_path, debug=args.debug)
        try:
            # ========== 第一步：处理路径配置 ==========
            # 处理SSH路径配置
            if args.set_ssh_path:
                runner.set_remote_fanse3_path(
                    args.set_ssh_path,
                    args.ssh_key,
                    args.ssh_password,
                    args.ssh_port
                )
                # return

            # 处理本地路径配置（原有逻辑）
            if args.set_path:
                runner.set_fanse3_path(args.set_path)
                # return

            # # ========== 第二步：检查运行模式 ==========
            ssh_config = runner.config.load_ssh_config()
            # if ssh_config:
            ssh_mode = args.ssh
            if ssh_mode and not args.set_path:  # 只有当没有设置本地路径时才使用远程模式
                runner.remote_mode = True
                runner.logger.info("🌐 使用远程FANSe3模式")

                # 建立SSH连接
                if runner.ssh_manager.connect(ssh_config):
                    runner.logger.info("✅ SSH连接就绪")
                else:
                    runner.logger.warning("⚠️ SSH连接失败，切换到本地模式")
                    runner.remote_mode = False
            else:
                # 检查本地FANSe路径
                fanse_path = runner.get_fanse3_path()
                if fanse_path:
                    runner.logger.info(f"💻 使用本地FANSe3模式: {fanse_path}")
                else:
                    runner.logger.error("❌ 未找到可用的FANSe3路径")
                    sys.exit(1)
                    # ========== 第三步：必须的运行参数检查 ==========
            if not args.input or not args.refseq:
                runner.logger.error("❌ 必须提供 -i/--input 和 -r/--refseq 参数")
                sys.exit(1)
                # ========== 第四步：处理工作目录 ==========
            if args.work_dir:
                runner.set_work_dir(args.work_dir)

            # ========== 第五步：解析输入输出路径 ==========
            # 解析输入数据的路径
            input_paths = runner.parse_input(args.input)
            if not input_paths:
                runner.logger.error("未找到有效的输入文件")
                sys.exit(1)

            # 检查是否指定了结果输出目录
            output_paths = None
            if args.output:
                # 支持多种分隔符处理：逗号/分号/空格
                separators = [',', ';', ' ']
                output_list = args.output

                # 尝试找到最适合的分隔符
                for sep in separators:
                    if sep in args.output:
                        output_list = [d.strip()
                                       for d in args.output.split(sep) if d.strip()]
                        break
                else:  # 没有分隔符时视为单个路径
                    output_list = [args.output.strip()]

                output_paths = [Path(d) for d in output_list]

            # 生成路径映射
            path_map = runner.generate_output_mapping(
                input_paths, output_paths)

            # ========== 第六步：准备参数和选项 ==========
            # 准备参数
            params = {
                key: value for key, value in [
                    ('O', args.O),
                    ('L', args.L),
                    ('E', args.E),
                    ('S', args.S),
                    ('H', args.H),
                    ('C', args.C),
                    ('T', args.T),
                ] if value is not None
            }

            # 准备选项
            options = [
                opt for opt, flag in [
                    ('--all', args.all),
                    ('--unique', args.unique),
                    ('--showalign', args.showalign),
                    ('--test', args.test),
                    ('--indel', args.indel),
                    ('--rename', args.rename)
                ] if flag
            ]

            # ========== 第七步：执行比对 ==========
            runner.logger.info("🚀 开始执行FANSe3比对...")

            runner.run_batch(
                file_map=path_map,
                refseq=Path(args.refseq),
                params=params,
                options=options,
                debug=args.debug,
                yes=args.yes,
                resume=args.resume
            )

        except Exception as e:
            runner.logger.error(f"运行失败: {str(e)}")
            if args.debug:
                import traceback
                traceback.print_exc()
            sys.exit(1)
        finally:
            runner._cleanup()

    parser.set_defaults(func=run_command)


def run_command(args):
    """处理run子命令"""
    # 处理日志路径
    log_path = Path(args.log) if args.log else None
    if args.log:
        try:
            log_path = Path(args.log)
            if log_path.is_dir():
                log_path = log_path / 'fanse_run.log'
        except Exception as e:
            print(f"警告: 指定的日志路径无效 - {str(e)}")

    # 创建运行器实例
    runner = FanseRunner(log_path=log_path, debug=args.debug)

    try:
        # ========== 第一步：处理路径配置 ==========

        # 1. 处理SSH路径配置（需要先创建runner实例）
        if args.set_ssh_path:
            runner.set_remote_fanse3_path(
                args.set_ssh_path,
                args.ssh_key,
                args.ssh_password,
                args.ssh_port
            )
            runner.logger.info("✅ SSH路径配置完成")
            return

        # 2. 处理本地路径配置（原有逻辑）
        if args.set_path:
            runner.set_fanse3_path(args.set_path)
            runner.logger.info("✅ 本地路径配置完成")
            return

        # ========== 第二步：处理工作目录 ==========
        if args.work_dir:
            runner.set_work_dir(args.work_dir)

        # 如果设置了工作目录，记录日志
        if args.work_dir:
            work_dir = Path(args.work_dir)
            if not work_dir.exists():
                runner.logger.info(f"创建工作目录: {work_dir}")
                work_dir.mkdir(parents=True, exist_ok=True)
            runner.work_dir = work_dir
            runner.logger.info(f"设置工作目录: {runner.work_dir}")

        # ========== 第三步：检查运行模式 ==========

        # 检查是否配置了SSH（用于后续运行）

        ssh_config = runner.config.load_ssh_config()
        # 更新 ssh_mode 逻辑：支持 --ssh 或 --remote-ssh
        ssh_mode = args.ssh or getattr(args, 'remote_ssh', False)

        if ssh_mode:
            runner.remote_mode = True
            runner.logger.info("🌐 使用远程FANSe3模式")

            # 建立SSH连接
            if not runner.ssh_manager.connect(ssh_config):
                runner.logger.error("SSH连接失败，回退到本地模式")
                runner.remote_mode = False
        else:
            # 检查本地FANSe路径 (非集群模式下必需)
            # 修改：只要指定了 nodes (-n)，也视为集群模式，跳过本地检查
            is_cluster = getattr(args, 'cluster', False) or (getattr(args, 'nodes', None) is not None)

            fanse_path = runner.get_fanse3_path()
            
            if fanse_path:
                runner.logger.info(f"💻 使用本地FANSe3模式: {fanse_path}")
            elif not is_cluster:
                # 只有在非集群模式下，本地路径缺失才是错误
                runner.logger.error(
                    "未配置FANSe路径，请先使用 --set-path 或 --set-ssh-path 配置")
                sys.exit(1)
            else:
                # 集群模式下，本地路径缺失是允许的
                runner.logger.info("🚀 集群模式/多节点模式: 跳过本地FANSe路径检查")

        # ========== 第四步：检查运行参数 ==========

        # 检查是否提供了够运行的最少运行参数
        if not args.input or not args.refseq:
            runner.logger.error("需至少提供 -i/--input 和 -r/--refseq 参数")
            sys.exit(1)

        # 解析输入数据的路径
        input_paths = runner.parse_input(args.input)
        if not input_paths:
            runner.logger.error("未找到有效的输入文件")
            sys.exit(1)

        # 检查是否指定了结果输出目录
        output_paths = None
        if args.output:
            # 支持多种分隔符处理：逗号/分号/空格
            separators = [',', ';', ' ']
            output_list = args.output

            # 尝试找到最适合的分隔符
            for sep in separators:
                if sep in args.output:
                    output_list = [d.strip()
                                   for d in args.output.split(sep) if d.strip()]
                    break
            else:  # 没有分隔符时视为单个路径
                output_list = [args.output.strip()]

            output_paths = [Path(d) for d in output_list]

        # 生成路径映射
        path_map = runner.generate_output_mapping(input_paths, output_paths)

        # 准备参数
        params = {
            key: value for key, value in [
                ('O', args.O), ('L', args.L), ('E', args.E), ('S', args.S),
                ('H', args.H), ('C', args.C), ('T', args.T),
            ] if value is not None
        }

        # 准备选项
        options = [
            opt for opt, flag in [
                ('--all', args.all), ('--unique', args.unique),
                ('--showalign', args.showalign), ('--test', args.test),
                ('--indel', args.indel), ('--rename', args.rename)
            ] if flag
        ]

        # ========== 第五步：选择运行模式并执行 ==========

        # 修改：只要指定了 nodes (-n)，也视为集群模式
        is_cluster_mode = getattr(args, 'cluster', False) or (getattr(args, 'nodes', None) is not None)

        if is_cluster_mode:
            # 集群模式
            runner.logger.info("🚀 准备集群分发任务...")
            commands = []
            
            # 准备通用参数
            final_params = {**runner.default_params, **params}
            final_options = [*runner.default_options, *options]
            
            # 1. GZIP检测与节点限制
            has_gzip = any(f.suffix == '.gz' for f in path_map.keys())
            if has_gzip:
                args.require_fansetools = True
                runner.logger.info("📦 检测到GZIP文件，将只使用安装了FANSeTools的节点运行")

            # 2. 参考序列文件处理 (传输到远程)
            args.required_files = []
            ref_path = Path(args.refseq)
            # 定义远程参考序列存放路径 (使用相对路径，相对于用户主目录)
            remote_ref_dir = "fansetools_work/refs"
            remote_ref_path = f"{remote_ref_dir}/{ref_path.name}"
            
            # 添加到传输列表
            args.required_files.append((str(ref_path), remote_ref_path))
            runner.logger.info(f"📄 将传输参考序列文件到集群节点: {remote_ref_path}")

            for input_file, output_file in path_map.items():
                # Resume逻辑
                if args.resume and output_file.exists():
                    runner.logger.info(f"跳过已存在输出: {output_file}")
                    continue
                
                # 检查gzip
                curr_input = input_file
                has_gzip_file = curr_input.suffix == '.gz'
                
                if has_gzip_file:
                    # 使用 fanse run 命令 (远程节点需安装fansetools)
                    # 构建 fanse run 命令
                    cmd_parts = ["fanse", "run"]
                    cmd_parts.append(f"-i {curr_input}")
                    cmd_parts.append(f"-r {remote_ref_path}")
                    cmd_parts.append(f"-o {output_file}")
                    
                    # 添加参数
                    for k, v in final_params.items():
                        cmd_parts.append(f"-{k} {v}")
                    
                    # 添加选项
                    cmd_parts.extend(final_options)
                    
                    # 确保非交互模式
                    if '-y' not in final_options and '--yes' not in final_options:
                        cmd_parts.append('-y')
                        
                    cmd = " ".join(cmd_parts)
                    runner.logger.debug(f"构建GZIP集群命令: {cmd}")
                else:
                    # 构建命令 - 使用远程参考序列路径
                    # 注意：这里假设输入文件路径在远程也是可访问的（如共享存储）
                    # 使用 {{FANSE_PATH}} 占位符，由 distribute 模块根据节点配置替换
                    cmd = runner.build_command(
                        curr_input, output_file, Path(remote_ref_path), final_params, final_options,
                        fanse_path_override="{{FANSE_PATH}}"
                    )
                
                commands.append(cmd)
            
            if not commands:
                 runner.logger.info("没有需要执行的任务")
            else:
                 runner.logger.info(f"提交 {len(commands)} 个任务到集群")
                 if distribute_command(commands, args):
                     runner.logger.info("✅ 集群任务执行完成")
                 else:
                     runner.logger.error("❌ 集群任务执行失败")
                     sys.exit(1)
            return

        if runner.remote_mode:
            # 远程模式运行
            # 这里需要实现远程运行逻辑
            runner.logger.info("🚀 开始远程FANSe3运行...")
            # 暂时回退到本地模式
            runner.run_batch(
                file_map=path_map,
                refseq=Path(args.refseq),
                params=params,
                options=options,
                debug=args.debug,
                yes=args.yes,
                resume=args.resume
            )
        else:
            # 本地模式运行
            runner.run_batch(
                file_map=path_map,
                refseq=Path(args.refseq),
                params=params,
                options=options,
                debug=args.debug,
                yes=args.yes,
                resume=args.resume
            )

    except Exception as e:
        runner.logger.error(f"运行失败: {str(e)}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        runner._cleanup()


# 如果独立运行，则测试
if __name__ == "__main__":
    # 测试配置
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_run_subparser(subparsers)

    # 模拟参数
    args = parser.parse_args(['run', '--set-path', '.'])
    args.func(args)
