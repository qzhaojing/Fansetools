import json
import os
import argparse
from .utils.rich_help import CustomHelpFormatter
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import paramiko
import base64  # 新增：用于 PowerShell 脚本编码
import gzip
import shutil
import tempfile
import subprocess
from dataclasses import dataclass

# 修正：Windows下支持ESC键检测用于中断watch
try:
    import msvcrt  # Windows 控制台按键检测
    _HAS_MSVCRT = True
except Exception:
    _HAS_MSVCRT = False
import socket
import time
import re
import queue  # 新增：用于动态任务队列
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from .utils.path_utils import PathProcessor

@dataclass
class ClusterNode:
    """集群节点配置"""
    name: str
    host: str
    user: str
    fanse_path: Optional[str] = None  # 修正：Linux节点可不设置fanse可执行路径
    key_path: Optional[str] = None
    password: Optional[str] = None
    port: int = 22
    max_jobs: int = 1
    enabled: bool = True
    work_dir: Optional[str] = None  # 修正：预留工作目录字段，便于后续 -w 更新

class OptimizedClusterManager:
    """优化后的集群管理器"""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.cluster_file = config_dir / "cluster.json"
        self.status_file = config_dir / "cluster_status.json"  # 修正：缓存最近一次检查结果供 list 离线展示
        self.nodes: Dict[str, ClusterNode] = {}
        self._connection_pool: Dict[str, paramiko.SSHClient] = {}
        self._load_cluster_config()
    
    def _load_cluster_config(self):
        """加载集群配置"""
        if self.cluster_file.exists():
            try:
                with open(self.cluster_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for node_data in data.get('nodes', []):
                        node = ClusterNode(**node_data)
                        self.nodes[node.name] = node
            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️ 配置文件损坏: {e}，将创建新的配置")
    
    def _save_cluster_config(self):
        """保存集群配置"""
        try:
            data = {'nodes': [vars(node) for node in self.nodes.values()]}
            self.config_dir.mkdir(parents=True, exist_ok=True)
            with open(self.cluster_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ 保存配置失败: {e}")
    
    def _test_network_connectivity(self, host: str, port: int, timeout: int = 2) -> bool:
        """优化的网络连通性测试"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception:
            return False
    
    def _create_ssh_connection(self, node: ClusterNode, timeout: int = 3) -> Optional[paramiko.SSHClient]:
        """创建SSH连接（带详细错误处理）"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_kwargs = {
                'hostname': node.host,
                'username': node.user,
                'port': node.port,
                'timeout': timeout,
                'banner_timeout': timeout,
                'auth_timeout': timeout
            }
            
            # 认证配置
            if node.key_path and os.path.exists(node.key_path):
                try:
                    key = paramiko.RSAKey.from_private_key_file(node.key_path)
                    connect_kwargs['pkey'] = key
                except Exception as e:
                    print(f"❌ 密钥加载失败: {e}")
                    return None
            elif node.password:
                connect_kwargs['password'] = node.password
            else:
                print("❌ 未提供认证信息")
                return None
            
            ssh.connect(**connect_kwargs)
            return ssh
            
        except paramiko.AuthenticationException as e:
            print(f"❌ SSH认证失败: {e}")
        except paramiko.SSHException as e:
            print(f"❌ SSH连接错误: {e}")
        except Exception as e:
            print(f"❌ 连接创建失败: {e}")
        
        return None
    
    def _execute_remote_command(self, ssh: paramiko.SSHClient, command: str, timeout: int = 10) -> Tuple[bool, str, str]:
        """执行远程命令并返回结果"""
        try:
            stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8', errors='ignore').strip()
            error = stderr.read().decode('utf-8', errors='ignore').strip()
            return exit_status == 0, output, error
        except Exception as e:
            return False, "", str(e)
    
    def _is_windows_system(self, ssh: paramiko.SSHClient) -> bool:
        """检测远程系统是否为Windows"""
        # 尝试执行Windows和Linux命令来检测系统类型
        success, output, error = self._execute_remote_command(ssh, "echo %OS%")
        if success and "Windows" in output:
            return True
        
        success, output, error = self._execute_remote_command(ssh, "uname -s")
        if success and "Linux" in output:
            return False
        
        # 默认假设为Windows（基于路径格式）
        return True
    
    def _test_windows_path(self, ssh: paramiko.SSHClient, path: str) -> bool:
        """专门测试Windows路径存在性"""
        # 多种Windows路径验证方法
        commands = [
            f'if exist "{path}" echo EXISTS',
            f'dir "{path}" >nul 2>&1 && echo EXISTS',
            f'powershell -Command "Test-Path \\"{path}\\" -PathType Leaf"'
        ]
        
        for command in commands:
            success, output, error = self._execute_remote_command(ssh, command)
            if success and ("EXISTS" in output or "True" in output):
                return True
            time.sleep(0.5)  # 短暂延迟
        
        return False
    
    def _test_linux_path(self, ssh: paramiko.SSHClient, path: str) -> bool:
        """测试Linux路径存在性"""
        commands = [
            f'test -f "{path}" && echo EXISTS',
            f'ls "{path}" >/dev/null 2>&1 && echo EXISTS'
        ]
        
        for command in commands:
            success, output, error = self._execute_remote_command(ssh, command)
            if success and "EXISTS" in output:
                return True
        
        return False
    
    def test_node_connection(self, node: ClusterNode, verbose: bool = True) -> bool:
        """优化的节点连接测试"""
        if verbose:
            print(f"🔍 测试节点连接: {node.name} ({node.user}@{node.host}:{node.port})")
        
        # 1. 测试网络连通性
        if verbose:
            print("  📡 测试网络连通性...")
        if not self._test_network_connectivity(node.host, node.port):
            if verbose:
                print("  ❌ 网络连接失败")
            return False
        if verbose:
            print("  ✅ 网络连通性正常")
        
        # 2. 建立SSH连接
        if verbose:
            print("  🔌 建立SSH连接...")
        ssh = self._create_ssh_connection(node)
        if not ssh:
            if verbose:
                print("  ❌ SSH连接失败")
            return False
        if verbose:
            print("  ✅ SSH连接成功")
        
        try:
            # 3. 检测操作系统类型
            if verbose:
                print("  💻 检测操作系统...")
            is_windows = self._is_windows_system(ssh)
            if verbose:
                print(f"  ✅ 检测为: {'Windows' if is_windows else 'Linux'}")
            
            # 4. 验证路径存在性（修正：Windows/Linux 节点路径非必填，若提供则尝试验证）
            if is_windows:
                if node.fanse_path:
                    if verbose:
                        print(f"  📁 验证路径: {node.fanse_path}")
                    path_ok = self._test_windows_path(ssh, node.fanse_path)
                    if verbose:
                        print("  ✅ 路径验证成功" if path_ok else "  ⚠️ 路径不可访问（可稍后更新）")
                if verbose:
                    print("  ✅ Windows 节点连接通过")
                return True
            else:
                # Linux 节点：若未提供路径，直接认为连接成功；若提供路径，则尝试验证但失败不阻断
                if node.fanse_path:
                    if verbose:
                        print(f"  📁 验证路径: {node.fanse_path}")
                    _ = self._test_linux_path(ssh, node.fanse_path)
                if verbose:
                    print("  ✅ Linux 节点连接与环境检测通过")
                return True
                
        finally:
            ssh.close()
    
    def add_node(self, name: str, host: str, user: str, fanse_path: Optional[str] = None, 
                 key_path: Optional[str] = None, password: Optional[str] = None, port: int = 22) -> bool:
        """优化的添加节点方法"""
        if name in self.nodes:
            raise ValueError(f"节点 '{name}' 已存在")
        
        node = ClusterNode(
            name=name, host=host, user=user, fanse_path=fanse_path,
            key_path=key_path, password=password, port=port
        )
        
        print("=" * 60)
        print(f"🔧 添加节点: {name}")
        print("=" * 60)
        
        # 分步测试并提供详细反馈
        steps = [
            ("网络连通性", self._test_network_connectivity, (host, port)),
            ("SSH连接", lambda: bool(self._create_ssh_connection(node)), ()),
            ("环境检测", self.test_node_connection, (node, False))
        ]
        
        for step_name, test_func, test_args in steps:
            print(f"🔍 测试{step_name}...", end=" ")
            try:
                if test_func(*test_args):
                    print("✅")
                else:
                    print("❌")
                    raise Exception(f"{step_name}测试失败")
            except Exception as e:
                print(f"❌ ({e})")
                return False
        # 修正：添加阶段不再强制部署FANSe3，后续可通过 update 命令更新路径
        
        # 保存节点配置
        self.nodes[name] = node
        self._save_cluster_config()
        
        print("=" * 60)
        print(f"✅ 节点 '{name}' 添加成功!")
        print(f"   地址: {node.user}@{node.host}:{node.port}")
        print(f"   路径: {node.fanse_path if node.fanse_path else '-'}")
        print("=" * 60)
        return True
    
    def _deploy_fanse_to_remote(self, node: ClusterNode, ssh: paramiko.SSHClient) -> bool:
        """自动部署FANSe3到远程节点"""
        try:
            # 1. 查找本地FANSe3可执行文件
            local_fanse = self._find_local_fanse_executable()
            if not local_fanse:
                print("  ❌❌ 未找到本地FANSe3可执行文件")
                return False
                
            # 2. 通过SFTP上传文件
            sftp = ssh.open_sftp()
            remote_dir = os.path.dirname(node.fanse_path)
            
            # 3. 确保远程目录存在
            self._ensure_remote_directory(sftp, remote_dir)
            
            # 4. 上传文件
            sftp.put(str(local_fanse), node.fanse_path)
            
            # 5. 设置执行权限（Linux系统）
            if not self._is_windows_system(ssh):
                ssh.exec_command(f'chmod +x "{node.fanse_path}"')
                
            sftp.close()
            return True
            
        except Exception as e:
            print(f"  ❌❌ 部署失败: {e}")
            return False

    def _find_local_fanse_executable(self) -> Optional[Path]:
        """查找本地FANSe3可执行文件"""
        # 搜索常见位置
        search_paths = [
            Path.cwd(),
            Path.home() / 'fanse',
            Path.home() / 'FANSe3',
            Path('/opt/fanse'),
            Path('/usr/local/fanse')
        ]
        
        for path in search_paths:
            if path.exists():
                for executable in ['FANSe3g.exe', 'FANSe3.exe', 'FANSe3g', 'FANSe3']:
                    exe_path = path / executable
                    if exe_path.exists():
                        return exe_path
        return None
    
    
    def install_node_software(self, node: ClusterNode, install_conda: bool, install_fansetools: bool, pip_mirror: str) -> bool:
        """在节点上安装软件（Conda/Miniforge、git、fansetools）
        修正：读取本地 utils 安装脚本并在远端执行，统一Windows/Linux行为，避免复杂引号问题；并修复 Windows 安装器路径引号问题
        """
        print(f"🔧 正在节点 '{node.name}' 上执行安装任务...")
        ssh = self._create_ssh_connection(node)
        if not ssh:
            print(f"❌ 无法连接到节点 '{node.name}'")
            return False

        try:
            is_windows = self._is_windows_system(ssh)
            utils_dir = Path(__file__).resolve().parent / 'utils'
            cmd = ""

            if is_windows:
                # 修正：读取 PowerShell 安装脚本并附加调用参数
                ps_path = utils_dir / 'install_win.ps1'
                if not ps_path.exists():
                    raise FileNotFoundError(str(ps_path))
                with open(ps_path, 'r', encoding='utf-8') as f:
                    script_body = f.read()
                inv = [
                    'Invoke-FansetoolsInstall',
                    f'-InstallConda:{"$true" if install_conda else "$false"}',
                    f'-InstallFansetools:{"$true" if install_fansetools else "$false"}',
                    f'-PipMirror "{pip_mirror}"'
                ]
                full_script = script_body + "\n" + " ".join(inv)
                import base64
                encoded_cmd = base64.b64encode(full_script.encode('utf-16le')).decode('utf-8')
                cmd = f'powershell -NoProfile -ExecutionPolicy Bypass -EncodedCommand {encoded_cmd}'
            else:
                # 修正：读取 Bash 安装脚本并附加调用参数
                sh_path = utils_dir / 'install_linux.sh'
                if not sh_path.exists():
                    raise FileNotFoundError(str(sh_path))
                with open(sh_path, 'r', encoding='utf-8') as f:
                    script_body = f.read()
                inv = [
                    'fansetools_install',
                    f'--conda {"true" if install_conda else "false"}',
                    f'--fansetools {"true" if install_fansetools else "false"}',
                    f'--pip-mirror "{pip_mirror}"'
                ]
                full_script = script_body + "\n" + " ".join(inv)
                full_script_escaped = full_script.replace('"', '\\"')
                cmd = f'bash -c "{full_script_escaped}"'

            print(f"🚀 发送指令到 '{node.name}'...")
            stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
            for line in iter(stdout.readline, ""):
                print(f"  [{node.name}] {line.strip()}")
            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0:
                print(f"✅ 节点 '{node.name}' 任务成功")
                return True
            else:
                print(f"❌ 节点 '{node.name}' 任务失败 (Code {exit_status})")
                return False
        except Exception as e:
            print(f"❌ 安装异常: {e}")
            return False
        finally:
            ssh.close()

    def remove_node(self, name: str):
        """移除节点"""
        if name not in self.nodes:
            raise ValueError(f"节点 '{name}' 不存在")
        del self.nodes[name]
        self._save_cluster_config()
    
    def list_nodes(self) -> List[ClusterNode]:
        """列出所有节点"""
        return list(self.nodes.values())
    
    def check_all_nodes_parallel(self, max_workers: int = 3, detail: bool = False) -> Dict[str, Dict[str, any]]:
        """并行检查所有节点状态，返回详细信息
        修正说明：此函数返回 {node_name: info_dict}，不再返回布尔值。
        适配调用方时需使用 info['online'] 判断在线状态。
        """
        def _collect_node_info(node: ClusterNode) -> Dict[str, any]:
            """收集单个节点的完整信息"""
            info = {
                'online': False,
                'response_time': None,
                'cpu_cores': None,
                'cpu_usage': None,
                'cpu_model': None,   # 修正：新增CPU型号
                'cpu_freq_mhz': None,  # 修正：新增CPU当前频率
                'memory_usage': None,
                'disk_usage': None,
                'load_avg': None,
                'net_rx_mbps': None,
                'net_tx_mbps': None,
                'kernel_version': None,  # 修正：detail模式下新增Linux内核版本
                # 修正：新增环境与路径检查结果，用于列表与筛选
                'conda_ok': None,
                'conda_version': None,
                'fansetools_ok': None,
                'fansetools_version': None,
                'fanse_path_ok': None,
                'temp_folder_ok': None
            }
            
            # 1. 网络连通性与响应时间
            start = time.time()
            if not self._test_network_connectivity(node.host, node.port, timeout=2):
                return info
            info['response_time'] = round((time.time() - start) * 1000, 2)  # ms
            
            # 2. SSH连接
            ssh = self._create_ssh_connection(node, timeout=3)
            if not ssh:
                return info
            info['online'] = True
            
            try:
                is_windows = self._is_windows_system(ssh)
                
                # 3. CPU核数
                if is_windows:
                    cmd = 'wmic cpu get NumberOfCores /value'
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success and 'NumberOfCores=' in out:
                        info['cpu_cores'] = int(out.split('NumberOfCores=')[1].strip())
                else:
                    cmd = 'nproc'
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success and out.isdigit():
                        info['cpu_cores'] = int(out)
                
                # 4. CPU使用率
                if is_windows:
                    cmd = 'wmic cpu get loadpercentage /value'
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success and 'LoadPercentage=' in out:
                        info['cpu_usage'] = f"{out.split('LoadPercentage=')[1].strip()}%"
                    
                    # 修正：采集CPU型号与频率
                    cmd = 'wmic cpu get Name,CurrentClockSpeed /value'
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success:
                        m_name = re.search(r'Name=(.+)', out)
                        m_freq = re.search(r'CurrentClockSpeed=(\d+)', out)
                        if m_name:
                            info['cpu_model'] = m_name.group(1).strip()
                        if m_freq:
                            info['cpu_freq_mhz'] = int(m_freq.group(1))
                else:
                    cmd = "top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | cut -d'%' -f1"
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success:
                        info['cpu_usage'] = f"{float(out):.1f}%"
                    
                    # 修正：采集CPU型号与频率（Linux）
                    # 型号
                    cmd = "lscpu | sed -n 's/Model name:\\s*//p'"
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success and out:
                        info['cpu_model'] = out.strip()
                    else:
                        cmd = "awk -F: '/model name/ {print $2; exit}' /proc/cpuinfo"
                        success, out, _ = self._execute_remote_command(ssh, cmd)
                        if success and out:
                            info['cpu_model'] = out.strip()
                    
                    # 频率（取平均MHz）
                    cmd = "awk -F: '/cpu MHz/ {sum+=$2; cnt++} END {if(cnt>0) printf \"%.0f\", sum/cnt}' /proc/cpuinfo"
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success and out:
                        try:
                            info['cpu_freq_mhz'] = int(float(out))
                        except:
                            pass
                
                # 5. 内存使用率
                if is_windows:
                    cmd = 'wmic OS get TotalVisibleMemorySize,FreePhysicalMemory /value'
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success:
                        total = round(int(re.search(r'TotalVisibleMemorySize=(\d+)', out).group(1))/1e6, 1)
                        free  = round(int(re.search(r'FreePhysicalMemory=(\d+)', out).group(1))/1e6, 1)
                        used_percent = (total - free) / total * 100
                        info['memory_usage'] = f"{(total - free):.1f}/{total:.1f} GB, {used_percent:.1f}%"
                else:
                    
                    # 修正：显示已用/总量（GB）和百分比
                    cmd = "free -b | awk '/Mem:/ {printf \"%.1f/%.1f GB, %.1f%%\", $3/1e9, $2/1e9, ($3/$2)*100}'"
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success and out:
                        info['memory_usage'] = out.strip()
                
                # 6. 本地硬盘使用情况（取根分区）
                if is_windows:
                    cmd = 'wmic logicaldisk get size,freespace,caption | findstr "^C:"'
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success:
                        parts = out.split()
                        free  = round(int(parts[1])/1e9, 1)
                        total = round(int(parts[2])/1e9, 1)
                        used_percent = (total - free) / total * 100
                        info['disk_usage'] = f"C: {(total - free):.1f}/{total:.1f} GB, {used_percent:.1f}%"
                else:
                    # 修正：显示已用/总量与百分比
                    cmd = "df -B1 / | tail -1 | awk '{printf \"%.1f/%.1f GB, %s\", $3/1e9, $2/1e9, $5}'"
                    success, out, _ = self._execute_remote_command(ssh, cmd)
                    if success:
                        info['disk_usage'] = f"/ {out.strip()}"

                # 修正：环境与路径检查（Conda/Fansetools/FANSe路径/工作目录）
                try:
                    if is_windows:
                        # Conda 检查：优先检测 Miniforge/Miniconda 安装目录与版本
                        cmd = '$c = Get-Command conda -ErrorAction SilentlyContinue; if ($c) { conda -V }'
                        success, out, _ = self._execute_remote_command(ssh, f'powershell -NoProfile -Command "{cmd}"')
                        if success and out:
                            info['conda_ok'] = True
                            info['conda_version'] = out.strip()
                        else:
                            # 路径存在性回退检测
                            success, out, _ = self._execute_remote_command(ssh, 'if exist "%USERPROFILE%\\miniforge3\\python.exe" echo YES')
                            if not success:
                                success, out, _ = self._execute_remote_command(ssh, 'if exist "%USERPROFILE%\\miniconda3\\python.exe" echo YES')
                            info['conda_ok'] = True if (success and 'YES' in out) else False
                            info['conda_version'] = None

                        # Fansetools 检查：优先使用 conda python
                        py_candidates = [
                            '%USERPROFILE%\\miniforge3\\python.exe',
                            '%USERPROFILE%\\miniconda3\\python.exe',
                            'python'
                        ]
                        fans_ok = False
                        fans_ver = None
                        for py in py_candidates:
                            cmd = f'"{py}" -c "import fansetools, sys; sys.stdout.write(getattr(fansetools,\"__version__\",\"\"))"'
                            success, out, _ = self._execute_remote_command(ssh, cmd)
                            if success:
                                fans_ok = True
                                fans_ver = out.strip() or None
                                break
                        info['fansetools_ok'] = fans_ok
                        info['fansetools_version'] = fans_ver

                        # FANSe 路径检查
                        if node.fanse_path:
                            info['fanse_path_ok'] = self._test_windows_path(ssh, node.fanse_path)
                        else:
                            info['fanse_path_ok'] = None

                        # 工作目录检查（作为临时目录）
                        if node.work_dir:
                            cmd = f'powershell -NoProfile -Command "Test-Path \"{node.work_dir}\" -PathType Container"'
                            success, out, _ = self._execute_remote_command(ssh, cmd)
                            info['temp_folder_ok'] = True if (success and ('True' in out)) else False
                        else:
                            info['temp_folder_ok'] = None
                    else:
                        # Linux Conda 检查
                        success, out, _ = self._execute_remote_command(ssh, 'bash -lc "conda -V"')
                        if success and out:
                            info['conda_ok'] = True
                            info['conda_version'] = out.strip()
                        else:
                            success, out, _ = self._execute_remote_command(ssh, 'test -d "$HOME/miniforge3" && echo YES || test -d "$HOME/miniconda3" && echo YES')
                            info['conda_ok'] = True if (success and 'YES' in out) else False
                            info['conda_version'] = None

                        # Fansetools 检查
                        fans_ok = False
                        fans_ver = None
                        for py in ['python3', 'python']:
                            success, out, _ = self._execute_remote_command(ssh, f'bash -lc "{py} -c \"import fansetools, sys; sys.stdout.write(getattr(fansetools,\\\"__version__\\\",\\\"\\\"))\""')
                            if success:
                                fans_ok = True
                                fans_ver = out.strip() or None
                                break
                        info['fansetools_ok'] = fans_ok
                        info['fansetools_version'] = fans_ver

                        # FANSe 路径检查
                        if node.fanse_path:
                            info['fanse_path_ok'] = self._test_linux_path(ssh, node.fanse_path)
                        else:
                            info['fanse_path_ok'] = None

                        # 工作目录检查
                        if node.work_dir:
                            success, out, _ = self._execute_remote_command(ssh, f'bash -lc "test -d \"{node.work_dir}\" && echo YES"')
                            info['temp_folder_ok'] = True if (success and 'YES' in out) else False
                        else:
                            info['temp_folder_ok'] = None
                except Exception:
                    pass

                # 7. 负载均值 & 网络带宽（detail模式）
                if detail:
                    if is_windows:
                        # Windows 无标准loadavg，网络带宽尝试获取，每秒采样一次
                        # 负载均值使用CPU百分比近似或置为'-'
                        info['load_avg'] = '-'
                        cmd = 'wmic path Win32_PerfFormattedData_Tcpip_NetworkInterface get BytesReceivedPersec,BytesSentPersec /value'
                        success1, out1, _ = self._execute_remote_command(ssh, cmd)
                        time.sleep(1)
                        success2, out2, _ = self._execute_remote_command(ssh, cmd)
                        if success1 and success2:
                            try:
                                r1 = sum(int(x) for x in re.findall(r'BytesReceivedPersec=(\d+)', out1))
                                s1 = sum(int(x) for x in re.findall(r'BytesSentPersec=(\d+)', out1))
                                r2 = sum(int(x) for x in re.findall(r'BytesReceivedPersec=(\d+)', out2))
                                s2 = sum(int(x) for x in re.findall(r'BytesSentPersec=(\d+)', out2))
                                rx_bps = max(0, r2 - r1)
                                tx_bps = max(0, s2 - s1)
                                info['net_rx_mbps'] = round(rx_bps * 8 / 1e6, 1)
                                info['net_tx_mbps'] = round(tx_bps * 8 / 1e6, 1)
                            except:
                                pass
                    else:
                        # Linux 负载均值
                        cmd = "cat /proc/loadavg | awk '{printf \"%s,%s,%s\", $1,$2,$3}'"
                        success, out, _ = self._execute_remote_command(ssh, cmd)
                        if success and out:
                            info['load_avg'] = out.strip()
                        # 修正：Linux 内核版本（uname -r）
                        cmd = 'uname -r'
                        success, out, _ = self._execute_remote_command(ssh, cmd)
                        if success and out:
                            info['kernel_version'] = out.strip()
                        # Linux 网络带宽，采样两次 /proc/net/dev
                        success, out1, _ = self._execute_remote_command(ssh, 'cat /proc/net/dev')
                        time.sleep(1)
                        success2, out2, _ = self._execute_remote_command(ssh, 'cat /proc/net/dev')
                        if success and success2:
                            def parse_netdev(text):
                                stats = {}
                                for line in text.splitlines():
                                    if ':' in line:
                                        name, data = line.split(':', 1)
                                        name = name.strip()
                                        parts = [p for p in data.strip().split() if p]
                                        if len(parts) >= 10:
                                            rx = int(parts[0])
                                            tx = int(parts[8])
                                            stats[name] = (rx, tx)
                                return stats
                            s1 = parse_netdev(out1)
                            s2 = parse_netdev(out2)
                            best_iface = None
                            best_delta = -1
                            for iface in s1:
                                if iface in s2:
                                    drx = s2[iface][0] - s1[iface][0]
                                    dtx = s2[iface][1] - s1[iface][1]
                                    delta = drx + dtx
                                    if delta > best_delta and not iface.startswith(('lo',)):
                                        best_delta = delta
                                        best_iface = (drx, dtx)
                            if best_iface:
                                info['net_rx_mbps'] = round(best_iface[0] * 8 / 1e6, 1)
                                info['net_tx_mbps'] = round(best_iface[1] * 8 / 1e6, 1)
                        
            except Exception as e:
                # 静默忽略细节错误，保证主流程
                pass
            finally:
                ssh.close()
            
            return info
        
        # 并行收集
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_node = {
                executor.submit(_collect_node_info, node): node.name
                for node in self.nodes.values()
            }
            
            results = {}
            for future in as_completed(future_to_node):
                node_name = future_to_node[future]
                try:
                    results[node_name] = future.result()
                except Exception:
                    results[node_name] = {
                        'online': False,
                        'response_time': None,
                        'cpu_cores': None,
                        'cpu_usage': None,
                        'memory_usage': None,
                        'disk_usage': None
                    }
            
            # 修正：将最近一次检查结果写入本地缓存，供 list 离线展示
            try:
                cache = {
                    'timestamp': time.time(),
                    'results': results
                }
                self.config_dir.mkdir(parents=True, exist_ok=True)
                with open(self.status_file, 'w', encoding='utf-8') as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
            return results

    # 在OptimizedClusterManager中添加以下方法
    def execute_with_monitoring(self, node_name: str, command: str) -> bool:
        """带实时监控的远程命令执行"""
        return self.monitor_node_execution(node_name, command)

    def deploy_to_node(self, node_name: str) -> bool:
        """部署FANSe3到指定节点"""
        node = self.nodes.get(node_name)
        ssh = self._create_ssh_connection(node)
        return self._deploy_fanse_to_remote(node, ssh)

    def monitor_node_execution(self, node_name: str, command: str, quiet: bool = False, log_file: Optional[str] = None, prefix: Optional[str] = None, idle_timeout: Optional[int] = None, hard_timeout: Optional[int] = None, heartbeat_sec: int = 0, stop_event: Optional[any] = None):
        """实时监控远程节点执行（支持静默、日志、心跳与超时）
        修改说明：
        - 增加 idle_timeout：长时间无输出判定假死并主动结束
        - 增加 hard_timeout：总时长限制，超时后主动结束
        - 增加 heartbeat_sec：启用SSH keepalive，避免长连接被断开
        - 增加 stop_event：控制端触发中止时立即结束远端执行
        """
        node = self.nodes.get(node_name)
        if not node:
            raise ValueError(f"节点不存在: {node_name}")
        
        ssh = self._create_ssh_connection(node)
        if not ssh:
            return False
        
        try:
            # 创建交互式会话
            transport = ssh.get_transport()
            # 修正：开启SSH心跳，防止长时间运行被网络设备中断
            try:
                if heartbeat_sec and heartbeat_sec > 0:
                    transport.set_keepalive(heartbeat_sec)
            except Exception:
                pass
            channel = transport.open_session()
            
            # 设置伪终端以获得实时输出
            channel.get_pty()
            channel.exec_command(command)
            
            # 实时读取输出（修正：稳健解码，避免UTF-8解码错误；支持静默、写日志、超时与心跳）
            lf = None
            if log_file:
                try:
                    os.makedirs(os.path.dirname(log_file), exist_ok=True)
                    lf = open(log_file, 'a', encoding='utf-8', errors='ignore')
                except Exception:
                    lf = None
            start_time = time.time()
            last_activity = start_time
            while True:
                # 修正：支持控制端中止（Ctrl+C触发的 stop_event）
                if stop_event is not None and getattr(stop_event, 'is_set', None) and stop_event.is_set():
                    try:
                        if not quiet:
                            print(f"{prefix or ''} 🔴 控制端请求终止，关闭远端会话")
                        channel.close()
                    except Exception:
                        pass
                    try:
                        self.kill_remote_fanse_processes(node_name)
                    except Exception:
                        pass
                    return False
                if channel.recv_ready():
                    raw = channel.recv(4096)
                    try:
                        data = raw.decode('utf-8', errors='ignore')
                    except Exception:
                        try:
                            data = raw.decode('gbk', errors='ignore')
                        except Exception:
                            data = ''
                    if data:
                        last_activity = time.time()
                        if lf:
                            lf.write(data)
                        if not quiet:
                            if prefix:
                                print(f"{prefix} {data}", end='', flush=True)
                            else:
                                print(data, end='', flush=True)
                if channel.recv_stderr_ready():
                    raw_err = channel.recv_stderr(4096)
                    try:
                        data_err = raw_err.decode('utf-8', errors='ignore')
                    except Exception:
                        try:
                            data_err = raw_err.decode('gbk', errors='ignore')
                        except Exception:
                            data_err = ''
                    if data_err:
                        last_activity = time.time()
                        if lf:
                            lf.write(data_err)
                        if not quiet:
                            if prefix:
                                print(f"{prefix} [STDERR] {data_err}", end='', flush=True)
                            else:
                                print(f"[STDERR] {data_err}", end='', flush=True)
                if channel.exit_status_ready():
                    break
                # 修正：假死与超时检测
                now = time.time()
                if hard_timeout and hard_timeout > 0 and (now - start_time) > hard_timeout:
                    try:
                        if not quiet:
                            print(f"{prefix or ''} ⚠️ 超过总时长限制({hard_timeout}s)，主动终止远程进程")
                        channel.close()
                    except Exception:
                        pass
                    try:
                        self.kill_remote_fanse_processes(node_name)
                    except Exception:
                        pass
                    return False
                if idle_timeout and idle_timeout > 0 and (now - last_activity) > idle_timeout:
                    try:
                        if not quiet:
                            print(f"{prefix or ''} ⚠️ 长时间无输出({idle_timeout}s)，判定远端假死，主动结束")
                        channel.close()
                    except Exception:
                        pass
                    try:
                        self.kill_remote_fanse_processes(node_name)
                    except Exception:
                        pass
                    return False
                time.sleep(0.1)
            if lf:
                try:
                    lf.close()
                except Exception:
                    pass
            
            exit_status = channel.recv_exit_status()
            return exit_status == 0
            
        finally:
            ssh.close()

    # 修正：新增远程进程终止（Windows 节点）
    def kill_remote_fanse_processes(self, node_name: str) -> bool:
        node = self.nodes.get(node_name)
        if not node:
            return False
        ssh = self._create_ssh_connection(node)
        if not ssh:
            return False
        try:
            is_windows = self._is_windows_system(ssh)
            if is_windows:
                cmds = [
                    'taskkill /F /IM FANSe3g.exe /T',
                    'taskkill /F /IM FANSe3.exe /T'
                ]
                ok = True
                for cmd in cmds:
                    success, _, _ = self._execute_remote_command(ssh, cmd)
                    ok = ok and success
                return ok
            else:
                return True
        except Exception:
            return False
        finally:
            ssh.close()


# 优化后的cluster_command函数
def cluster_command(args):
    """优化的集群命令处理"""
    cluster_mgr = OptimizedClusterManager(get_config_dir())
    
    try:
        if args.cluster_command == 'add':
            success = cluster_mgr.add_node(
                args.name, args.host, args.user, args.fanse_path,
                args.key, args.password, args.port
            )
            if not success:
                return 1
        
        elif args.cluster_command == 'update':
            # 修正：支持更新节点配置（host/user/password/key/port/fanse_path/max_jobs/enabled/work_dir）
            name = getattr(args, 'name', None) or getattr(args, 'n', None)
            node = cluster_mgr.nodes.get(name) if name else None
            if not node:
                print(f"❌ 节点 '{name}' 不存在")
                return 1
            changed = []
            # 应用变更
            if getattr(args, 'host', None):
                node.host = args.host; changed.append('host')
            if getattr(args, 'user', None):
                node.user = args.user; changed.append('user')
            if getattr(args, 'password', None):
                node.password = args.password; changed.append('password')
            if getattr(args, 'key', None):
                node.key_path = args.key; changed.append('key')
            if getattr(args, 'port', None):
                node.port = args.port; changed.append('port')
            if getattr(args, 'fanse_path', None):
                node.fanse_path = args.fanse_path; changed.append('fanse_path')
            if getattr(args, 'max_jobs', None) is not None:
                node.max_jobs = args.max_jobs; changed.append('max_jobs')
            if getattr(args, 'enable', False):
                node.enabled = True; changed.append('enabled=TRUE')
            if getattr(args, 'disable', False):
                node.enabled = False; changed.append('enabled=FALSE')
            if getattr(args, 'work_dir', None):
                node.work_dir = args.work_dir; changed.append('work_dir')
            cluster_mgr._save_cluster_config()
            print(f"✅ 节点 '{name}' 已更新: {', '.join(changed) if changed else '无变更'}")
            if getattr(args, 'test', False):
                print(f"🔍 变更后测试节点 '{name}'...")
                if cluster_mgr.test_node_connection(node):
                    print("✅ 连接测试成功")
                else:
                    print("❌ 连接测试失败")
                    return 1
                
        elif args.cluster_command == 'remove':
            cluster_mgr.remove_node(args.name)
            print(f"✅ 节点 '{args.name}' 移除成功")
            
        elif args.cluster_command == 'list':
            nodes = cluster_mgr.list_nodes()
            if not nodes:
                print("📭 集群中暂无节点")
                return
                
            print("🏢 集群节点列表:")
            # 离线读取缓存
            status_map = {}
            try:
                with open(cluster_mgr.status_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    status_map = cache.get('results', {}) or {}
            except Exception:
                status_map = {}

            if getattr(args, 'table', False):
                headers = ['Node_name','Online','Resp(ms)','CPU_usage','Mem_usage','Disk_usage','Address','Path','Auth']
                print("-" * 120)
                print(" ".join([f"{h:<10}" for h in headers]))
                print("-" * 120)
                for node in nodes:
                    info = status_map.get(node.name, {})
                    is_online = bool(info.get('online'))
                    rt = info.get('response_time')
                    cores = info.get('cpu_cores')
                    cpu = info.get('cpu_usage')
                    mem = info.get('memory_usage')
                    disk = info.get('disk_usage')
                    address = f"{node.user}@{node.host}:{node.port}"
                    path = node.fanse_path if node.fanse_path else '-'
                    auth = '密钥' if node.key_path else '密码'
                    row = [
                        f"{node.name:<10}",
                        f"{'在线' if is_online else '离线':<10}",
                        f"{(str(rt) if rt is not None else '-'):<10}",
                        f"{(str(cores) if cores is not None else '-'):<10}",
                        f"{(cpu if cpu is not None else '-'):<10}",
                        f"{(mem if mem is not None else '-'):<10}",
                        f"{address:<24}",
                        f"{path:<24}",
                        f"{auth:<8}"
                    ]
                    print(" ".join(row))
                print("-" * 120)
            else:
                print("-" * 80)
                for node in nodes:
                    info = status_map.get(node.name, {})
                    is_online = bool(info.get('online'))
                    status = "✅" if is_online else "❌"
                    auth_type = "密钥" if node.key_path else "密码"
                    # print(f"{status} {node.name}")
                    # print(f"   地址: {node.user}@{node.host}:{node.port}")
                    # print(f"   路径: {node.fanse_path if node.fanse_path else '-'}")
                    # print(f"   认证: {auth_type}")
                    # print(f"   状态: {'在线' if is_online else '离线'}")
                    rt = info.get('response_time')
                    cores = info.get('cpu_cores')
                    cpu = info.get('cpu_usage')
                    mem = info.get('memory_usage')
                    disk = info.get('disk_usage')
                    # print(f"   响应: {rt if rt is not None else '-'} ms")
                    # print(f"   CPU核: {cores if cores is not None else '-'}")
                    # print(f"   CPU用量: {cpu if cpu is not None else '-'}")
                    # print(f"   内存用量: {mem if mem is not None else '-'}")
                    # print(f"   磁盘用量: {disk if disk is not None else '-'}")
                    print(f"Node: {status} {node.name} | 地址: {node.user}@{node.host}:{node.port} | FANse路径: {node.fanse_path if node.fanse_path else '-'} | 认证: {auth_type} | CPU核心数: {cores if cores is not None else '-'} | 内存信息: {mem if mem is not None else '-'} | 磁盘信息: {disk if disk is not None else '-'} | 最近响应速度: {rt if rt is not None else '-'} ms")
 
                    print("-" * 80)
                
        elif args.cluster_command == 'check':
            # 修正：支持 --watch 实时刷新；重构输出为两行，第一列始终为 node_name，并在第二行显示环境与路径检查
            interval = max(1, min(5, getattr(args, 'watch', 0) or 0))
            iterations = getattr(args, 'count', 0) or 0
            run_forever = interval > 0 and iterations == 0
            loop_count = iterations if iterations > 0 else 1
            try:
                while True:
                    status_map = cluster_mgr.check_all_nodes_parallel(detail=getattr(args, 'detail', False))
                    if not status_map:
                        print("📭 集群中暂无节点")
                        return

                    online_count = sum(1 for info in status_map.values() if info.get('online'))
                    print(f"📊 节点状态: {online_count}/{len(status_map)} 在线")

                    # 行1：核心硬件与负载信息
                    headers1 = ['Node_name','Online','Resp(ms)','CPU_usage','Mem_usage','Disk_usage','CPU型号','频率(MHz)']
                    widths1 = [16,8,10,10,30,30,32,12]
                    sep_len1 = sum(widths1) + len(widths1) - 1
                    print("-" * sep_len1)
                    print(" ".join([h.ljust(w) for h, w in zip(headers1, widths1)]))
                    print("-" * sep_len1)



                    for name, info in status_map.items():
                        # 行1数据
                        is_online = bool(info.get('online'))
                        rt = info.get('response_time')
                        cores = info.get('cpu_cores')
                        cpu = info.get('cpu_usage')
                        mem = info.get('memory_usage')
                        disk = info.get('disk_usage')
                        model = info.get('cpu_model') or '-'
                        freq = info.get('cpu_freq_mhz')
                        freq_str = str(freq) if freq is not None else '-'
                        row1 = [
                            str(name),
                            '在线' if is_online else '离线',
                            str(rt) if rt is not None else '-',
                            cpu if cpu is not None else '-',
                            mem if mem is not None else '-',
                            disk if disk is not None else '-',
                            model,
                            freq_str
                        ]
                        print(" ".join([str(v)[:widths1[i]].ljust(widths1[i]) for i, v in enumerate(row1)]))

                    # 行2：环境与路径检查 + 可选网络信息（同时显示路径与检查结果）
                    headers2 = ['Node_name','Conda','Fansetools','Fanse_Path','Fanse_Path(ck)','TempFolder(-w)','TempFolder(ck)']
                    widths2 = [16,16,16,28,12,28,12]
                    if getattr(args, 'detail', False):
                        headers2 += ['Kernel','LoadAvg','Net RX','Net TX']
                        widths2 += [20,16,10,10]
                    sep_len2 = sum(widths2) + len(widths2) - 1
                    print("-" * sep_len2)
                    print(" ".join([h.ljust(w) for h, w in zip(headers2, widths2)]))
                    print("-" * sep_len2)
                    for name, info in status_map.items():
                        # 行2数据（带检查标记与路径）
                        c_ok = info.get('conda_ok')
                        c_ver = info.get('conda_version')
                        c_str = '-' if c_ok is None else (f"✓ {c_ver}" if c_ok and c_ver else ("✓" if c_ok else "✗"))
                        f_ok = info.get('fansetools_ok')
                        f_ver = info.get('fansetools_version')
                        f_str = '-' if f_ok is None else (f"✓ {f_ver}" if f_ok and f_ver else ("✓" if f_ok else "✗"))
                        node_obj = cluster_mgr.nodes.get(name)
                        fanse_path_str = (node_obj.fanse_path if (node_obj and node_obj.fanse_path) else '-')
                        p_ok = info.get('fanse_path_ok')
                        p_ck = '-' if p_ok is None else ('✓' if p_ok else '✗')
                        temp_folder_str = (node_obj.work_dir if (node_obj and node_obj.work_dir) else '-')
                        t_ok = info.get('temp_folder_ok')
                        t_ck = '-' if t_ok is None else ('✓' if t_ok else '✗')

                        row2 = [str(name), c_str, f_str, fanse_path_str, p_ck, temp_folder_str, t_ck]
                        if getattr(args, 'detail', False):
                            row2 += [
                                info.get('kernel_version') or '-',
                                info.get('load_avg') or '-',
                                str(info.get('net_rx_mbps')) if info.get('net_rx_mbps') is not None else '-',
                                str(info.get('net_tx_mbps')) if info.get('net_tx_mbps') is not None else '-'
                            ]

                        print(" ".join([str(v)[:widths2[i]].ljust(widths2[i]) for i, v in enumerate(row2)]))

                    print("-" * sep_len2)






                    if interval > 0:
                        if not run_forever:
                            loop_count -= 1
                            if loop_count <= 0:
                                break
                        # 修正：支持Windows下ESC立即终止watch
                        slept = 0.0
                        step = 0.1
                        while slept < interval:
                            if _HAS_MSVCRT and msvcrt.kbhit():
                                ch = msvcrt.getch()
                                if ch in (b'\x1b',):  # ESC键
                                    print("🔴 监控已终止（ESC）")
                                    return
                            time.sleep(step)
                            slept += step
                    else:
                        break
            except KeyboardInterrupt:
                pass
            # # 修正：支持 --watch 实时刷新
            # interval = max(1, min(5, getattr(args, 'watch', 0) or 0))
            # iterations = getattr(args, 'count', 0) or 0
            # run_forever = interval > 0 and iterations == 0
            # loop_count = iterations if iterations > 0 else 1
            # try:
            #     while True:
            #         status_map = cluster_mgr.check_all_nodes_parallel(detail=getattr(args, 'detail', False))
            #         if not status_map:
            #             print("📭 集群中暂无节点")
            #             return

            #         online_count = sum(1 for info in status_map.values() if info.get('online'))
            #         print(f"📊 节点状态: {online_count}/{len(status_map)} 在线")

            #         # 默认以表格形式输出（-t 效果）
            #         headers = ['Node_name','Online','Resp(ms)','CPU_usage','Mem_usage','Disk_usage','CPU型号','频率(MHz)']
            #         if getattr(args, 'detail', False):
            #             headers += ['Kernel','LoadAvg','Net RX','Net TX']  # detail增加Kernel列
            #         widths = [12,8,10,8,22,22,28,12]
            #         if getattr(args, 'detail', False):
            #             widths += [18,16,10,10]  # 为Kernel与扩展列分配宽度
            #         sep_len = sum(widths) + len(widths) - 1
            #         print("-" * sep_len)
            #         print(" ".join([h.ljust(w) for h, w in zip(headers, widths)]))
            #         print("-" * sep_len)
            #         for name, info in status_map.items():
            #             is_online = bool(info.get('online'))
            #             rt = info.get('response_time')
            #             cores = info.get('cpu_cores')
            #             cpu = info.get('cpu_usage')
            #             mem = info.get('memory_usage')
            #             disk = info.get('disk_usage')
            #             model = info.get('cpu_model') or '-'
            #             freq = info.get('cpu_freq_mhz')
            #             freq_str = str(freq) if freq is not None else '-'
            #             row = [
            #                 str(name),
            #                 '在线' if is_online else '离线',
            #                 str(rt) if rt is not None else '-',
            #                 str(cores) if cores is not None else '-',
            #                 mem if mem is not None else '-',
            #                 disk if disk is not None else '-',
            #                 model,
            #                 freq_str
            #             ]
            #             if getattr(args, 'detail', False):
            #                 row += [info.get('kernel_version') or '-',
            #                         info.get('load_avg') or '-',
            #                         str(info.get('net_rx_mbps')) if info.get('net_rx_mbps') is not None else '-',
            #                         str(info.get('net_tx_mbps')) if info.get('net_tx_mbps') is not None else '-']
            #             print(" ".join([str(v)[:widths[i]].ljust(widths[i]) for i, v in enumerate(row)]))
            #         print("-" * sep_len)

            #         if interval > 0:
            #             if not run_forever:
            #                 loop_count -= 1
            #                 if loop_count <= 0:
            #                     break
            #             # 修正：支持Windows下ESC立即终止watch
            #             slept = 0.0
            #             step = 0.1
            #             while slept < interval:
            #                 if _HAS_MSVCRT and msvcrt.kbhit():
            #                     ch = msvcrt.getch()
            #                     if ch in (b'\x1b',):  # ESC键
            #                         print("🔴 监控已终止（ESC）")
            #                         return
            #                 time.sleep(step)
            #                 slept += step
            #         else:
            #             break
            # except KeyboardInterrupt:
            #     pass
                
        elif args.cluster_command == 'run':
            # 修正：支持直接传 run 参数；支持 -n/--nodes 和 -p 自动选择
            node_list = getattr(args, 'nodes', None)
            jobs_file = getattr(args, 'jobs', None)
            pick_n = int(getattr(args, 'p', 0) or 0)
            wait_sec = int(getattr(args, 'wait', 0) or 0)
            auto_yes = bool(getattr(args, 'yes', False))
            quiet = bool(getattr(args, 'quiet', False))
            log_dir = getattr(args, 'log_dir', None)
            # 修正：读取运行稳定性参数
            hard_timeout = int(getattr(args, 'timeout', 0) or 0)
            idle_timeout = int(getattr(args, 'idle_timeout', 0) or 0)
            heartbeat_sec = int(getattr(args, 'heartbeat', 0) or 0)
            native_mode = bool(getattr(args, 'native', False))
            # 修正：优先使用未知参数集合，以完整保留 -i/-r/-E 等原run参数
            remainder = []
            if hasattr(args, '_unknown') and args._unknown:
                remainder.extend(list(args._unknown))
            if getattr(args, 'command', []):
                remainder.extend(list(getattr(args, 'command')))

            def _build_remote_cmd(tokens: List[str], node_name: str) -> str:
                # 修正：将原run参数组装为远程命令，默认前缀 'fanse run '
                # 修正：为 -i/-r/-o 的参数值加引号，避免中文/空格路径被拆分
                # 修正：支持 native 模式，直接调用 fanse3g.exe 并转换参数
                
                if native_mode:
                    node = cluster_mgr.nodes.get(node_name)
                    exe_path = node.fanse_path if node and node.fanse_path else "fanse3g.exe"
                    
                    # 解析并转换参数: -i -> -D, -r -> -R, -o -> -O
                    # 保留其他参数
                    cmd_parts = [f'"{exe_path}"'] if ' ' in exe_path else [exe_path]
                    
                    i = 0
                    while i < len(tokens):
                        t = tokens[i]
                        if t == '-i':
                            if i + 1 < len(tokens):
                                val = tokens[i+1].strip('"')
                                cmd_parts.append(f'-D"{val}"')
                                i += 2
                                continue
                        elif t == '-r':
                            if i + 1 < len(tokens):
                                val = tokens[i+1].strip('"')
                                cmd_parts.append(f'-R"{val}"')
                                i += 2
                                continue
                        elif t == '-o':
                            if i + 1 < len(tokens):
                                val = tokens[i+1].strip('"')
                                cmd_parts.append(f'-O"{val}"')
                                i += 2
                                continue
                        elif t == '-y': # fanse3g 不需要 -y
                            i += 1
                            continue
                        else:
                            cmd_parts.append(t)
                            i += 1
                    return " ".join(cmd_parts)
                
                # 常规 fanse run 模式
                safe_tokens: List[str] = []
                i = 0
                while i < len(tokens):
                    t = tokens[i]
                    safe_tokens.append(t)
                    if t in ('-i', '-r', '-o') and (i + 1) < len(tokens):
                        v = tokens[i + 1]
                        if not (v.startswith('"') and v.endswith('"')):
                            v = f'"{v}"'
                        safe_tokens.append(v)
                        i += 2
                        continue
                    i += 1
                prefix = ['fanse', 'run']
                return " ".join(prefix + safe_tokens)

            # 修正：新增本地输出校验工具，确保作业仅在远端进程退出且输出文件非空后判定完成
            def _extract_output_path(tokens: List[str]) -> Optional[str]:
                # 修正：从参数集合中解析 -o 输出路径（本地/UNC均可），用于后置校验
                try:
                    oi = tokens.index('-o')
                    if oi + 1 < len(tokens):
                        return tokens[oi + 1].strip('"')
                except ValueError:
                    return None
                return None

            def _validate_output_nonempty(out_path: str, wait_sec: int = 30, poll_interval: float = 0.5) -> bool:
                # 修正：轮询校验输出文件是否存在且大小>0；用于UNC网络盘写入的最终一致性等待
                deadline = time.time() + max(0, wait_sec)
                p = out_path.strip('"')
                while True:
                    try:
                        if os.path.isfile(p):
                            try:
                                if os.path.getsize(p) > 0:
                                    return True
                            except Exception:
                                pass
                        # 若为目录或尚未创建文件，则继续等待直到超时
                    except Exception:
                        pass
                    if time.time() >= deadline:
                        return False
                    time.sleep(poll_interval)

            # 选择节点集合：指定 -n 或者按响应时间选择 -p 台
            selected_nodes: List[str] = []
            if node_list:
                selected_nodes = [n.strip() for n in str(node_list).split(',') if n.strip()]
            elif pick_n > 0:
                # 修正：支持等待节点就绪并选择最快N台；非原生模式需确认已安装 fansetools
                deadline = time.time() + max(0, wait_sec)
                while True:
                    status_map = cluster_mgr.check_all_nodes_parallel()
                    candidates = []
                    for name, info in status_map.items():
                        if not info.get('online'):
                            continue
                        rt = info.get('response_time')
                        if not isinstance(rt, (int, float)):
                            continue
                        if not native_mode:
                            if info.get('fansetools_ok') is not True:
                                continue
                        candidates.append((name, rt))
                    candidates.sort(key=lambda x: x[1])
                    selected_nodes = [name for name, _ in candidates[:pick_n]]
                    if selected_nodes:
                        break
                    if wait_sec > 0 and time.time() < deadline:
                        # 支持ESC终止等待
                        slept = 0.0
                        step = 0.1
                        while slept < 2.0:
                            if _HAS_MSVCRT and msvcrt.kbhit():
                                ch = msvcrt.getch()
                                if ch in (b'\x1b',):
                                    print("🔴 已终止等待（ESC）")
                                    selected_nodes = []
                                    break
                            time.sleep(step)
                            slept += step
                        if selected_nodes:
                            break
                        continue
                    break
            else:
                print("❌ 需要指定节点：使用 -n/--nodes 或 -p 选择最快N台")
                return 1
            if not selected_nodes:
                print("❌ 未找到可用节点，请检查集群状态")
                return 1

            # 修正：运行 fanse run 时仅分发到 Windows 节点，自动跳过 Linux 节点
            # 这样避免远端无 fanse.exe 的 Linux 系统导致执行失败
            win_nodes: List[str] = []
            skipped_nodes: List[str] = []
            try:
                for name in selected_nodes:
                    node_obj = cluster_mgr.nodes.get(name)
                    ssh = cluster_mgr._create_ssh_connection(node_obj)
                    if not ssh:
                        skipped_nodes.append(name)
                        continue
                    try:
                        if cluster_mgr._is_windows_system(ssh):
                            win_nodes.append(name)
                        else:
                            skipped_nodes.append(name)
                    finally:
                        ssh.close()
            except KeyboardInterrupt:
                print("\n🔴 已取消节点筛选，退出运行")
                return 1
            if not win_nodes:
                print("❌ 所选节点均非Windows或不可连接，run命令将跳过Linux节点")
                return 1
            if skipped_nodes:
                print(f"⚠️ 已跳过非Windows或不可连接节点: {', '.join(skipped_nodes)}")
            selected_nodes = win_nodes

            # 修正：输出可连接Windows节点列表与响应速度，便于快速确认
            try:
                status_map_print = cluster_mgr.check_all_nodes_parallel()
                summary = []
                for n in selected_nodes:
                    info = status_map_print.get(n, {})
                    rt = info.get('response_time')
                    summary.append(f"{n}:{(str(rt)+'ms') if rt is not None else '-'}")
                if summary:
                    print(f"✅ 可连接Windows节点: {' | '.join(summary)}")
            except Exception:
                pass

            # 准备作业列表：优先使用 --jobs，其次解析 -i 模式
            jobs: List[List[str]] = []
            if jobs_file:
                try:
                    with open(jobs_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            s = line.strip()
                            if not s or s.startswith('#'):
                                continue
                            tokens = s.split()
                            if ('-i' not in tokens) or ('-r' not in tokens):
                                print(f"❌ 作业行缺少必要参数 -i/-r: {s}")
                                return 1
                            jobs.append(tokens)
                except Exception as e:
                    print(f"❌ 读取作业文件失败: {e}")
                    return 1
            else:
                # 修正：解析 -i <pattern>，在首个选定节点上展开为文件列表
                tokens = list(remainder)
                # 修正：最少参数校验 -i/-r
                if ('-r' not in tokens):
                    print("❌ 缺少必要参数: -r <参考序列路径>")
                    return 1
                try:
                    i_idx = tokens.index('-i')
                except ValueError:
                    i_idx = -1
                if i_idx >= 0 and i_idx + 1 < len(tokens):
                    pattern = tokens[i_idx + 1]
                    # 修正：解析 -o 参数（若为目录，为每个输入生成专属输出文件）
                    o_val = None
                    try:
                        o_idx = tokens.index('-o')
                        if o_idx + 1 < len(tokens):
                            o_val = tokens[o_idx + 1]
                    except ValueError:
                        o_val = None
                    # 使用统一的 PathProcessor 解析输入
                    processor = PathProcessor()
                    # 使用 PathProcessor 解析路径，支持通配符和目录，统一使用 FASTQ 扩展名
                    files = [str(p) for p in processor.parse_input_paths(pattern, processor.FASTQ_EXTENSIONS)]
                    
                    if not files:
                        print(f"📭 未解析到匹配的输入文件: {pattern}")
                        return 1
                    # 以每个文件生成一条作业，将 -i 参数替换为具体文件
                    base = tokens[:i_idx] + tokens[i_idx+2:]
                    for f in files:
                        jt = base + ['-i', f]
                        # 修正：-o 为目录时，按输入文件名生成唯一输出文件，避免目录解析错误
                        if o_val:
                            try:
                                out_dir = o_val.strip('"')
                                file_base = os.path.basename(f.strip('"'))
                                out_file = os.path.join(out_dir, f"{file_base}.fanse3")
                                try:
                                    oi = jt.index('-o')
                                    if oi + 1 < len(jt):
                                        jt[oi + 1] = out_file
                                    else:
                                        jt.extend(['-o', out_file])
                                except ValueError:
                                    jt.extend(['-o', out_file])
                            except Exception:
                                pass
                        # 修正：为 fanse run 自动添加 -y，确保非交互
                        if '-y' not in jt:
                            jt.append('-y')
                        jobs.append(jt)
                else:
                    # 无 -i 模式，作为单作业直接运行
                    # 修正：最少参数校验 -i 缺失
                    print("❌ 缺少必要参数: -i <输入文件或通配符>")
                    return 1

            # 分发并并发执行（新增：动态任务队列，支持抢占式调度）
            job_queue = queue.Queue()
            for j in jobs:
                job_queue.put(j)
            print(f"🚀 将 {len(jobs)} 个作业放入动态队列，由 {len(selected_nodes)} 个节点抢占执行：{', '.join(selected_nodes)}")

            # 进度条初始化（tqdm），若不可用则回退为简单计数
            pbar = None
            progress_failed = 0
            try:
                from tqdm import tqdm  # 仅在需要时导入
                pbar = tqdm(total=len(jobs), desc="cluster run 进度", unit="job")
            except Exception:
                pbar = None

            import threading
            lock = threading.Lock()

            futures = []
            import threading
            stop_event = threading.Event()
            try:
                with ThreadPoolExecutor(max_workers=len(selected_nodes)) as executor:
                    for node in selected_nodes:
                        def run_node_jobs(n=node):
                            nonlocal pbar, progress_failed
                            while True:
                                if stop_event.is_set():
                                    return False
                                try:
                                    jt = job_queue.get(block=False)
                                except queue.Empty:
                                    break
                                
                                # 修正：为 fanse run 自动添加 -y（双保险）
                                if not native_mode and '-y' not in jt:
                                    jt.append('-y')

                                ok = True
                                temp_decompressed_file = None
                                remote_cmd = ""
                                job_input = None
                                
                                # 修正：Native模式下GZ文件自动解压（本地Python解压，更可靠）
                                try:
                                    if native_mode:
                                        input_idx = -1
                                        try:
                                            if '-i' in jt:
                                                input_idx = jt.index('-i')
                                            elif '-D' in jt:
                                                input_idx = jt.index('-D')
                                        except ValueError:
                                            pass
                                        
                                        if input_idx != -1 and input_idx + 1 < len(jt):
                                            raw_input = jt[input_idx + 1].strip('"')
                                            job_input = raw_input # 记录原始输入
                                            
                                            if raw_input.lower().endswith('.gz') and os.path.exists(raw_input):
                                                if not quiet:
                                                    print(f"[{n}] ⏳ 正在解压 GZ 文件: {os.path.basename(raw_input)} ...")
                                                
                                                input_path = Path(raw_input)
                                                # 尝试在同目录创建临时文件（确保远端节点可通过UNC路径访问）
                                                temp_dir = input_path.parent
                                                base_name = input_path.stem
                                                ts = int(time.time() * 1000)
                                                temp_name = f"{base_name}_{ts}_{n}.fastq"
                                                temp_decompressed_file = temp_dir / temp_name
                                                
                                                # 解压逻辑：优先 pigz，失败回退到 gzip
                                                decompression_success = False
                                                
                                                # 1. 尝试 pigz
                                                pigz_path = shutil.which('pigz')
                                                if not pigz_path:
                                                    # 尝试查找 bin 目录下的 pigz
                                                    try:
                                                        bin_pigz = Path(__file__).parent / "bin" / "windows" / "pigz.exe"
                                                        if bin_pigz.exists():
                                                            pigz_path = str(bin_pigz)
                                                    except:
                                                        pass

                                                if pigz_path:
                                                    try:
                                                        with open(temp_decompressed_file, 'wb') as f_out:
                                                            subprocess.run([pigz_path, '-d', '-c', str(input_path)], 
                                                                         stdout=f_out, 
                                                                         check=True)
                                                            f_out.flush()
                                                            os.fsync(f_out.fileno())
                                                        decompression_success = True
                                                    except Exception as e:
                                                        print(f"[{n}] ⚠️ pigz 解压失败，尝试使用 Python gzip: {e}")
                                                        # 如果失败，删除可能不完整的文件
                                                        if temp_decompressed_file.exists():
                                                            try:
                                                                os.remove(temp_decompressed_file)
                                                            except:
                                                                pass
                                                
                                                # 2. 回退到 Python gzip
                                                if not decompression_success:
                                                    try:
                                                        with gzip.open(input_path, 'rb') as f_in:
                                                            with open(temp_decompressed_file, 'wb') as f_out:
                                                                shutil.copyfileobj(f_in, f_out)
                                                                f_out.flush()
                                                                os.fsync(f_out.fileno())
                                                        decompression_success = True
                                                    except Exception as e:
                                                        print(f"[{n}] ❌ Python gzip 解压也失败: {e}")
                                                
                                                if decompression_success:
                                                    jt[input_idx + 1] = f'"{temp_decompressed_file}"'
                                                    if not quiet:
                                                        print(f"[{n}] ✅ 解压完成: {temp_name}")
                                                else:
                                                    ok = False
                                except Exception as e:
                                    print(f"[{n}] ❌ 准备作业失败(解压): {e}")
                                    ok = False

                                try: # 使用 try...finally 确保清理
                                    if ok:
                                        remote_cmd = _build_remote_cmd(jt, n)
                                        print(f"[{n}] 🚀 执行: {remote_cmd}")
                                        
                                        # 修正：输出管理：前缀与日志文件
                                        if not job_input:
                                            try:
                                                ii = jt.index('-i')
                                                if ii + 1 < len(jt):
                                                    job_input = jt[ii + 1].strip('"')
                                            except Exception:
                                                pass
                                        log_file = None
                                        if log_dir and job_input:
                                            try:
                                                base = os.path.basename(job_input)
                                                ts = int(time.time())
                                                log_file = os.path.join(log_dir, f"{n}_{base}_{ts}.log")
                                            except Exception:
                                                log_file = None
                                        
                                        ok = cluster_mgr.monitor_node_execution(n, remote_cmd, quiet=quiet, log_file=log_file, prefix=f"[{n}]", idle_timeout=idle_timeout or None, hard_timeout=hard_timeout or None, heartbeat_sec=heartbeat_sec, stop_event=stop_event)
                                        
                                        if ok:
                                            out_path = _extract_output_path(jt)
                                            if out_path:
                                                wait_after = idle_timeout if (isinstance(idle_timeout, int) and idle_timeout > 0) else 30
                                                valid = _validate_output_nonempty(out_path, wait_sec=wait_after)
                                                if not valid:
                                                    ok = False
                                                    if not quiet:
                                                        print(f"[{n}] ⚠️ 输出文件不存在或大小为0，判定作业失败: {out_path}")

                                    with lock:
                                        if pbar:
                                            pbar.update(1)
                                        else:
                                            print(f"✅ [{n}] 完成 1 项（剩余 {job_queue.qsize()}）")
                                        if not ok:
                                            progress_failed += 1
                                            if remote_cmd:
                                                print(f"❌ 节点 {n} 执行失败: {remote_cmd}")
                                            else:
                                                print(f"❌ 节点 {n} 作业预处理失败")
                                finally:
                                    # 清理临时解压文件 (确保在任何情况下都尝试清理)
                                    if temp_decompressed_file and os.path.exists(temp_decompressed_file):
                                        try:
                                            os.remove(temp_decompressed_file)
                                            if not quiet:
                                                pass # print(f"[{n}] 🧹 已清理临时文件")
                                        except Exception as e:
                                            print(f"[{n}] ⚠️ 无法清理临时文件 {temp_decompressed_file}: {e}")


                                job_queue.task_done()
                                
                                if stop_event.is_set():
                                    return False
                            return True
                        futures.append(executor.submit(run_node_jobs))
                    all_ok = True
                    for fut in as_completed(futures):
                        try:
                            if not fut.result():
                                all_ok = False
                        except Exception as e:
                            print(f"❌ 并发执行错误: {e}")
                            all_ok = False
            except KeyboardInterrupt:
                print("\n🔴 接收到终止请求，正在停止远程作业...")
                stop_event.set()
                for n in selected_nodes:
                    try:
                        cluster_mgr.kill_remote_fanse_processes(n)
                    except Exception:
                        pass
                print("🛑 远程作业已发送终止信号（Windows节点 taskkill）")
                return 1
            finally:
                if pbar:
                    pbar.close()
                if progress_failed:
                    print(f"⚠️ 共有 {progress_failed} 项作业失败")

            return 0 if progress_failed == 0 else 1

        elif args.cluster_command == 'test':
            node = cluster_mgr.nodes.get(args.name)
            if not node:
                print(f"❌ 节点 '{args.name}' 不存在")
                return 1
                
            print(f"🔍 测试节点 '{args.name}'...")
            if cluster_mgr.test_node_connection(node):
                print(f"✅ 节点 '{args.name}' 连接测试成功")
            else:
                print(f"❌ 节点 '{args.name}' 连接测试失败")
                return 1
                
        elif args.cluster_command == 'install':
            target_nodes = []
            if args.nodes.lower() == 'all':
                target_nodes = list(cluster_mgr.nodes.values())
            else:
                names = [n.strip() for n in args.nodes.split(',') if n.strip()]
                for name in names:
                    node = cluster_mgr.nodes.get(name)
                    if node:
                        target_nodes.append(node)
                    else:
                        print(f"⚠️ 节点 '{name}' 不存在，跳过")
            
            if not target_nodes:
                print("❌ 未指定有效节点")
                return 1
            
            print(f"📦 准备在 {len(target_nodes)} 个节点上安装软件...")
            success_count = 0
            for node in target_nodes:
                print("-" * 60)
                if cluster_mgr.install_node_software(
                    node, 
                    install_conda=args.conda, 
                    install_fansetools=args.fansetools, 
                    pip_mirror=args.pip_mirror
                ):
                    success_count += 1
            
            print("-" * 60)
            print(f"✅ 安装完成: {success_count}/{len(target_nodes)} 成功")
            return 0 if success_count == len(target_nodes) else 1

        else:
            print("❌ 未知的子命令")
            return 1
            
    except Exception as e:
        print(f"❌ 操作失败: {e}")
        return 1
        
    return 0

def add_cluster_subparser(subparsers):
    """添加集群管理子命令"""
    cluster_parser = subparsers.add_parser('cluster', 
        help='集群节点管理',
        description='''
FANSe3 集群管理工具
用于添加、管理和监控远程计算节点，实现分布式计算。

基本使用流程:
1. 添加节点: fanse cluster add <名称> <主机> <用户> <FANSe3路径>
2. 检查状态: fanse cluster check
3. 使用集群: fanse run --cluster 或 fanse run -n <节点名称>
        ''',
        formatter_class=CustomHelpFormatter
    )
    
    cluster_subparsers = cluster_parser.add_subparsers(
        dest='cluster_command', 
        title='子命令',
        description='使用 fanse cluster <子命令> -h 查看详细帮助'
    )
    
    # 添加节点
    add_parser = cluster_subparsers.add_parser('add', 
        help='添加集群节点',
        description='''
添加新的远程计算节点到集群。

基本语法:
  fanse cluster add <节点名称> <主机地址> <用户名> <FANSe3路径> [选项]

参数说明:
  <节点名称>    : 给节点起的唯一标识名（如: workstation1, server-a）
  <主机地址>    : 远程计算机的IP地址或主机名（如: 192.168.1.100, compute-node.local）
  <用户名>      : SSH登录用户名（如: user, admin, root）
  <FANSe3路径>  : 远程计算机上FANSe3可执行文件的完整路径

认证方式（二选一）:
  --key        : SSH私钥文件路径（推荐，更安全）
  --password   : SSH密码（如未提供密钥则使用密码）

其他选项:
  --port       : SSH端口号（默认: 22）
        ''',
        epilog='''
使用示例:

1. 使用SSH密钥添加节点:
   fanse cluster add lab-pc1 192.168.1.100 user /home/user/fanse/FANSe3g.exe --key ~/.ssh/id_rsa

2. 使用密码添加Windows节点:
   fanse cluster add win-server 192.168.1.101 administrator "C:\\\\Program Files\\\\FANSe3\\\\FANSe3g.exe" --password mypass123

3. 使用非标准端口:
   fanse cluster add remote-server example.com user /opt/fanse/FANSe3g.exe --key ~/.ssh/key --port 2222

验证节点:
  添加完成后使用以下命令验证:
  fanse cluster test <节点名称>    # 测试单个节点
  fanse cluster check            # 检查所有节点状态
  fanse cluster list             # 列出所有节点信息
        ''',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    add_parser.add_argument('name', help='节点唯一标识名称')
    add_parser.add_argument('host', help='远程主机地址（IP或域名）')
    add_parser.add_argument('user', help='SSH登录用户名')
    add_parser.add_argument('--fanse-path', help='远程FANSe3可执行文件完整路径（可选，可后续update再配置）')
    
    auth_group = add_parser.add_mutually_exclusive_group()
    auth_group.add_argument('--key', help='SSH私钥文件路径（推荐使用）')
    auth_group.add_argument('--password', help='SSH登录密码')
    
    add_parser.add_argument('--port', type=int, default=22, 
                           help='SSH端口号（默认: 22）')
    
    # 移除节点
    remove_parser = cluster_subparsers.add_parser('remove', 
        help='移除集群节点',
        description='从集群中移除指定的节点。',
        epilog='''
示例:
  fanse cluster remove lab-pc1    # 移除名为lab-pc1的节点
        '''
    )
    remove_parser.add_argument('name', help='要移除的节点名称')
    
    # 列出节点
    list_parser = cluster_subparsers.add_parser('list', 
        help='列出所有集群节点',
        description='显示当前配置的所有集群节点及其状态信息。',
        epilog='''
输出说明:
  ✅ 节点在线且可访问
  ❌ 节点离线或无法连接
        '''
    )
    list_parser.add_argument('-t', '--table', action='store_true', help='以表格形式显示（离线缓存）')
    
    # 检查节点
    check_parser = cluster_subparsers.add_parser('check', 
        help='检查所有节点状态',
        description='快速检查所有集群节点的连接状态。',
        epilog='''
示例输出:
  ✅ node1: 在线
  ❌ node2: 离线（可能网络问题或服务未启动）
        '''
    )
    check_parser.add_argument('-t', '--table', action='store_true', help='以表格形式显示（实时检测）')
    # 修正：新增实时监控刷新参数
    check_parser.add_argument('-w', '--watch', type=int, default=0, help='持续监控，间隔秒数（1-5）')
    check_parser.add_argument('-c', '--count', type=int, default=1, help='刷新次数（0为无限直到Ctrl+C）')
    # 修正：新增扩展指标
    check_parser.add_argument('--detail', action='store_true', help='显示负载均值与网络带宽信息')
    
    # 测试节点
    test_parser = cluster_subparsers.add_parser('test', 
        help='测试节点连接',
        description='测试指定节点的SSH连接和FANSe3路径可访问性。',
        epilog='''
示例:
  fanse cluster test lab-pc1    # 测试lab-pc1节点的连接
        '''
    )
    test_parser.add_argument('name', help='要测试的节点名称')

    # 更新节点
    update_parser = cluster_subparsers.add_parser('update',
        help='更新节点配置',
        description='更新已存在的节点字段（host/user/password/key/port/fanse_path/max_jobs/enabled/work_dir）',
        epilog='示例: fanse cluster update -n c128 --fanse-path C:\\FANSe3\\FANSe3g.exe --max-jobs 2 --enable'
    )
    update_parser.add_argument('-n', '--name', help='节点名称', required=True)
    update_parser.add_argument('--host', help='主机地址（IP或域名）')
    update_parser.add_argument('--user', help='SSH用户名')
    auth_group_u = update_parser.add_mutually_exclusive_group()
    auth_group_u.add_argument('--key', help='SSH私钥文件路径')
    auth_group_u.add_argument('--password', help='SSH密码')
    update_parser.add_argument('--port', type=int, help='SSH端口')
    update_parser.add_argument('--fanse-path', help='FANSe3可执行路径')
    update_parser.add_argument('--max-jobs', type=int, help='节点最大并行作业数')
    en_group = update_parser.add_mutually_exclusive_group()
    en_group.add_argument('--enable', action='store_true', help='启用节点')
    en_group.add_argument('--disable', action='store_true', help='禁用节点')
    update_parser.add_argument('-w', '--work-dir', help='工作目录（可选）')
    update_parser.add_argument('--test', action='store_true', help='更新后立即测试连接')

    # 运行作业（最小版）
    run_parser = cluster_subparsers.add_parser('run', 
        help='在节点上运行命令（最小版）',
        description='将原 fanse run 参数通过 SSH 在指定节点执行（支持 -i 通配符展开、--jobs 作业文件、-p 自动选择最快N台）',
        epilog='示例: fanse cluster run -n nodeA -i C\\data\\*.fastq.gz -r C\\ref\\ref.fa -E5 -C20')
    # 修正：统一 -n/--nodes，支持单/多；新增 -p 选择最快N台
    run_parser.add_argument('-n', '--nodes', help='节点名称（单个或逗号分隔多个）')
    run_parser.add_argument('-p', type=int, default=0, help='自动选择响应最快的N台节点')
    run_parser.add_argument('--jobs', help='作业文件（每行一个命令参数串）')
    run_parser.add_argument('--wait', type=int, default=0, help='等待节点就绪的秒数（用于 -p 自动选择），0 为不等待')
    # 修正：使用REMAINDER捕获剩余的原run参数（无需 --）
    run_parser.add_argument('command', nargs=argparse.REMAINDER, help='原 fanse run 参数（可直接跟在此命令后）')
    # 修正：新增 cluster 级别确认选项，缺省会自动为 fanse run 添加 -y
    run_parser.add_argument('-y', '--yes', action='store_true', help='在远端 fanse run 中自动添加确认（等同添加 -y）')
    # 修正：输出管理：静默与日志目录
    run_parser.add_argument('-q','--quiet', action='store_true', help='静默模式，不显示远端输出，仅显示进度与结果')
    run_parser.add_argument('--log-dir', help='保存每个作业的远端输出到本地目录（文件名含节点与输入基名）')
    # 修正：新增稳定性参数，避免远端假死
    run_parser.add_argument('--timeout', type=int, default=0, help='总超时时长（秒），超时后主动结束远端进程并标记失败（0为不限制）')
    run_parser.add_argument('--idle-timeout', type=int, default=0, help='空闲超时时长（秒），长时间无输出视为假死并主动结束（0为不限制）')
    run_parser.add_argument('--heartbeat', type=int, default=0, help='SSH心跳间隔（秒），用于保持长连接稳定（0为关闭）')
    # 修正：新增原生模式，直接调用fanse3g.exe而不是fanse run
    run_parser.add_argument('--native', action='store_true', help='原生模式，直接调用远端 fanse3g.exe，不依赖远端 fansetools 环境')

    # 安装命令
    install_parser = cluster_subparsers.add_parser('install',
        help='在节点上安装软件',
        description='在远程节点上安装 Conda 环境和 fansetools',
        epilog='示例: fanse cluster install -n node1 --conda --fansetools'
    )
    install_parser.add_argument('-n', '--nodes', help='节点名称（单个或逗号分隔多个，all表示所有）', required=True)
    install_parser.add_argument('--conda', action='store_true', help='安装 Miniconda')
    install_parser.add_argument('--fansetools', action='store_true', help='安装 fansetools')
    install_parser.add_argument('--pip-mirror', help='指定 pip 镜像源', default='https://pypi.tuna.tsinghua.edu.cn/simple')

    return cluster_parser
    
    # 在add_cluster_subparser中添加新命令
    deploy_parser = cluster_subparsers.add_parser('deploy',   # pyright: ignore[reportUnreachable]
        help='部署FANSe3到节点')
    deploy_parser.add_argument('name', help='节点名称')

    monitor_parser = cluster_subparsers.add_parser('monitor', 
        help='实时监控节点')
    monitor_parser.add_argument('name', help='节点名称')
    monitor_parser.add_argument('--command', help='要执行的命令')
    
    
def get_config_dir() -> Path:
    """获取配置目录"""
    if os.name == 'nt':  # Windows
        appdata = os.environ.get('LOCALAPPDATA') or os.environ.get('APPDATA') or os.path.expanduser("~")
        return Path(appdata) / 'Fansetools'
    else:  # Linux/macOS
        return Path.home() / '.config' / 'fansetools'

# 确保模块可以被正确导入和使用
if __name__ != "__main__":
    # 这些函数和类需要被外部模块访问
    __all__ = [
        'ClusterManager', 
        'ClusterNode', 
        'add_cluster_subparser', 
        'cluster_command',
        'get_config_dir'
    ]
