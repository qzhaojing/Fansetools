import json
import os
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import paramiko
from dataclasses import dataclass
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

@dataclass
class ClusterNode:
    """集群节点配置"""
    name: str
    host: str
    user: str
    fanse_path: str
    key_path: Optional[str] = None
    password: Optional[str] = None
    port: int = 22
    max_jobs: int = 1
    enabled: bool = True

class OptimizedClusterManager:
    """优化后的集群管理器"""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.cluster_file = config_dir / "cluster.json"
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
    
    def _test_network_connectivity(self, host: str, port: int, timeout: int = 5) -> bool:
        """优化的网络连通性测试"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception:
            return False
    
    def _create_ssh_connection(self, node: ClusterNode, timeout: int = 15) -> Optional[paramiko.SSHClient]:
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
            
            # 4. 验证路径存在性
            if verbose:
                print(f"  📁 验证路径: {node.fanse_path}")
            path_exists = False
            if is_windows:
                path_exists = self._test_windows_path(ssh, node.fanse_path)
            else:
                path_exists = self._test_linux_path(ssh, node.fanse_path)
            
            if path_exists:
                if verbose:
                    print("  ✅ 路径验证成功")
                return True
            else:
                if verbose:
                    print("  ❌ 路径不存在或不可访问")
                    # 提供调试信息
                    success, output, error = self._execute_remote_command(
                        ssh, f'dir "{os.path.dirname(node.fanse_path)}"'
                    )
                    if success:
                        print(f"  📂 目录内容: {output[:200]}...")
                return False
                
        finally:
            ssh.close()
    
    def add_node(self, name: str, host: str, user: str, fanse_path: str, 
                 key_path: str = None, password: str = None, port: int = 22) -> bool:
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
            ("路径具备", self.test_node_connection, (node, False))
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
        # 在路径验证失败时尝试自动拷贝
        if not path_exists:
            print(f"  📦📦 目标路径不存在，尝试自动部署FANSe3...")
            if self._deploy_fanse_to_remote(node, ssh):
                print("  ✅ FANSe3部署成功")
                path_exists = True
            else:
                print("  ❌❌ 自动部署失败")
                return False
        
        # 保存节点配置
        self.nodes[name] = node
        self._save_cluster_config()
        
        print("=" * 60)
        print(f"✅ 节点 '{name}' 添加成功!")
        print(f"   地址: {user}@{host}:{port}")
        print(f"   路径: {fanse_path}")
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
    
    
    def remove_node(self, name: str):
        """移除节点"""
        if name not in self.nodes:
            raise ValueError(f"节点 '{name}' 不存在")
        del self.nodes[name]
        self._save_cluster_config()
    
    def list_nodes(self) -> List[ClusterNode]:
        """列出所有节点"""
        return list(self.nodes.values())
    
    def check_all_nodes_parallel(self, max_workers: int = 3) -> Dict[str, bool]:
        """并行检查所有节点状态"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_node = {
                executor.submit(self.test_node_connection, node, False): node.name 
                for node in self.nodes.values()
            }
            
            results = {}
            for future in as_completed(future_to_node):
                node_name = future_to_node[future]
                try:
                    results[node_name] = future.result()
                except Exception as e:
                    results[node_name] = False
                    print(f"节点 {node_name} 检查异常: {e}")
            
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
    def monitor_node_execution(self, node_name: str, command: str):
        """实时监控远程节点执行"""
        node = self.nodes.get(node_name)
        if not node:
            raise ValueError(f"节点不存在: {node_name}")
        
        ssh = self._create_ssh_connection(node)
        if not ssh:
            return False
        
        try:
            # 创建交互式会话
            transport = ssh.get_transport()
            channel = transport.open_session()
            
            # 设置伪终端以获得实时输出
            channel.get_pty()
            channel.exec_command(command)
            
            # 实时读取输出
            while True:
                if channel.recv_ready():
                    data = channel.recv(1024).decode('utf-8')
                    print(data, end='', flush=True)
                if channel.recv_stderr_ready():
                    data = channel.recv_stderr(1024).decode('utf-8')
                    print(f"[STDERR] {data}", end='', flush=True)
                if channel.exit_status_ready():
                    break
                time.sleep(0.1)
                    
            exit_status = channel.recv_exit_status()
            return exit_status == 0
            
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
                
        elif args.cluster_command == 'remove':
            cluster_mgr.remove_node(args.name)
            print(f"✅ 节点 '{args.name}' 移除成功")
            
        elif args.cluster_command == 'list':
            nodes = cluster_mgr.list_nodes()
            if not nodes:
                print("📭 集群中暂无节点")
                return
                
            print("🏢 集群节点列表:")
            print("-" * 80)
            status_map = cluster_mgr.check_all_nodes_parallel()
            
            for node in nodes:
                status = "✅" if status_map.get(node.name, False) else "❌"
                auth_type = "密钥" if node.key_path else "密码"
                print(f"{status} {node.name}")
                print(f"   地址: {node.user}@{node.host}:{node.port}")
                print(f"   路径: {node.fanse_path}")
                print(f"   认证: {auth_type}")
                print(f"   状态: {'在线' if status_map.get(node.name, False) else '离线'}")
                print("-" * 80)
                
        elif args.cluster_command == 'check':
            status_map = cluster_mgr.check_all_nodes_parallel()
            if not status_map:
                print("📭 集群中暂无节点")
                return
                
            online_count = sum(status_map.values())
            print(f"📊 节点状态: {online_count}/{len(status_map)} 在线")
            
            for name, is_online in status_map.items():
                status_icon = "✅" if is_online else "❌"
                print(f"{status_icon} {name}: {'在线' if is_online else '离线'}")
                
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
        formatter_class=argparse.RawTextHelpFormatter
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
    add_parser.add_argument('fanse_path', help='远程FANSe3可执行文件完整路径')
    
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

    return cluster_parser
    
    # 在add_cluster_subparser中添加新命令
    deploy_parser = cluster_subparsers.add_parser('deploy', 
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
    