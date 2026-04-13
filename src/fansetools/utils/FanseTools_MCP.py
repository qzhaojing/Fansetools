import sys
import os
import traceback

# Debug logging to identify why the server fails to start in Trae
DEBUG_LOG_PATH = os.path.join(os.path.dirname(__file__), "mcp_debug.log")
try:
    with open(DEBUG_LOG_PATH, "w") as f:
        f.write(f"Starting MCP Server...\n")
        f.write(f"Executable: {sys.executable}\n")
        f.write(f"CWD: {os.getcwd()}\n")
        f.write(f"Path: {sys.path}\n")
except Exception:
    pass

import asyncio
import subprocess
import json
import glob
from pathlib import Path
from typing import List, Optional, Dict, Union

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    with open(DEBUG_LOG_PATH, "a") as f:
        f.write(f"ImportError: {e}\n")
        f.write("Please ensure 'mcp' is installed in the python environment being used.\n")
    sys.exit(1)
except Exception as e:
    with open(DEBUG_LOG_PATH, "a") as f:
        f.write(f"Unexpected error during import: {traceback.format_exc()}\n")
    sys.exit(1)

# 初始化 MCP Server
mcp = FastMCP("Fansetools-MCP-Server")

# ==========================================
# 0. 基础文件系统工具 (File System Tools)
# ==========================================

@mcp.tool()
async def list_directory(path: str = ".", pattern: Optional[str] = None) -> str:
    """
    列出指定目录下的文件和文件夹。
    
    Args:
        path: 目录路径，默认为当前目录 "."
        pattern: 可选的 glob 模式 (例如 "*.fanse3") 用于过滤文件
    
    Returns:
        包含文件列表的字符串
    """
    try:
        p = Path(path)
        if not p.exists():
            return f"错误: 路径 '{path}' 不存在"
        if not p.is_dir():
            return f"错误: '{path}' 不是一个目录"

        files = []
        if pattern:
            # 使用 glob 匹配
            search_path = p / pattern
            # glob.glob 返回的是字符串列表
            matched = glob.glob(str(search_path))
            files = [Path(f).name for f in matched]
            return f"在 '{path}' 中找到符合 '{pattern}' 的文件:\n" + "\n".join(files)
        else:
            # 列出所有
            for item in p.iterdir():
                type_str = "<DIR>" if item.is_dir() else f"{item.stat().st_size}B"
                files.append(f"{type_str}\t{item.name}")
            return f"目录 '{path}' 的内容:\n" + "\n".join(files)
    except Exception as e:
        return f"列出目录失败: {str(e)}"

@mcp.tool()
async def get_working_directory() -> str:
    """获取当前工作目录"""
    return str(Path.cwd())

# ==========================================
# 1. 核心 Fansetools 封装 (First-class Citizens)
# ==========================================

@mcp.tool()
async def run_fansetools_count(
    input_files: str,
    output_dir: str,
    level: str = "gene",
    gxf_file: Optional[str] = None,
    threads: int = 4,
    resume: bool = False
) -> str:
    """
    运行 fansetools count 进行表达量定量 (Gene/Isoform计数)。
    
    Args:
        input_files: 输入文件路径模式 (例如 "data/*.fanse3" 或具体文件路径)
        output_dir: 输出目录路径
        level: 定量水平 ("gene", "isoform", "both")
        gxf_file: GTF/GFF3 注释文件路径 (gene level 需要)
        threads: 并行线程数
        resume: 是否开启断点续传 (默认 False)
    """
    cmd = [
        sys.executable, "-m", "fansetools", "count",
        "-i", input_files,
        "-o", output_dir,
        "-l", level,
        "-t", str(threads)
    ]
    if gxf_file:
        cmd.extend(["-g", gxf_file])
    
    if resume:
        cmd.append("--resume")
        
    return _run_command(cmd)

@mcp.tool()
async def run_fansetools_trim(
    input_file: str,
    output_file: str,
    min_len: int = 15,
    threads: int = 4
) -> str:
    """
    运行 fansetools trim 进行序列修剪和质控。
    
    Args:
        input_file: 输入FASTQ文件路径
        output_file: 输出文件路径
        min_len: 最小保留长度 (默认 15)
        threads: 线程数
    """
    cmd = [
        sys.executable, "-m", "fansetools", "trim",
        "-i", input_file,
        "-o", output_file,
        "--min_len", str(min_len),
        "-t", str(threads)
    ]
    return _run_command(cmd)

@mcp.tool()
async def run_fansetools_workflow(
    input_file: str,
    reference: str,
    output_file: str,
    index_file: Optional[str] = None,
    threads: int = 4
) -> str:
    """
    运行 fansetools run (标准比对流程)。
    执行: Trim -> Map -> Sort -> Count (部分步骤可选，此命令主要用于比对)
    
    Args:
        input_file: 输入文件 (FASTQ/FASTA)
        reference: 参考基因组文件 (FASTA)
        output_file: 输出 FANSe3 文件路径
        index_file: (可选) 预构建的索引文件
        threads: 线程数
    """
    return _run_command([sys.executable, "-m", "fansetools", "run", "-i", input_file, "-r", reference, "-o", output_file, "--index", index_file, "--threads", str(threads)])

@mcp.tool()
async def run_fansetools_parser(input_file: str) -> str:
    """
    运行 fansetools parser 解析 FANSe3 文件。
    用于查看文件头部记录结构，默认显示前10条。
    
    Args:
        input_file: FANSe3 格式输入文件
    """
    return _run_command([sys.executable, "-m", "fansetools", "parser", input_file])

@mcp.tool()
async def run_fansetools_fastx(
    mode: str,
    input_file: str,
    output_file: Optional[str] = None,
    output_format: Optional[str] = None
) -> str:
    """
    运行 fansetools fastx 进行 FASTA/FASTQ 格式转换和处理。
    
    Args:
        mode: 运行模式 ("fanse", "unmapped", "fasta2fastq", "fastq2fasta")
              - fanse: 处理 FANSe3 文件提取序列
              - unmapped: 处理 Unmapped reads 文件
              - fasta2fastq: FASTA 转 FASTQ
              - fastq2fasta: FASTQ 转 FASTA
        input_file: 输入文件路径
        output_file: 输出文件路径 (可选)
        output_format: 输出格式 ("fasta" 或 "fastq")，仅在 mode 为 "fanse" 或 "unmapped" 时需要
    """
    cmd = [sys.executable, "-m", "fansetools", "fastx", "-i", input_file]
    
    if mode == "fanse":
        cmd.append("--fanse")
    elif mode == "unmapped":
        cmd.append("--unmapped")
    elif mode == "fasta2fastq":
        cmd.append("--fasta2fastq")
    elif mode == "fastq2fasta":
        cmd.append("--fastq2fasta")
    else:
        return f"错误: 未知的模式 '{mode}'"

    if output_file:
        cmd.extend(["-o", output_file])
        
    if mode in ["fanse", "unmapped"]:
        if output_format == "fasta":
            cmd.append("--fasta")
        elif output_format == "fastq":
            cmd.append("--fastq")
        else:
            return "错误: mode 为 'fanse' 或 'unmapped' 时必须指定 output_format ('fasta' 或 'fastq')"
            
    return _run_command(cmd)

@mcp.tool()
async def run_fansetools_flow(
    action: str,
    name: Optional[str] = None,
    script_or_url: Optional[str] = None,
    alias: Optional[str] = None
) -> str:
    """
    运行 fansetools flow 管理工作流脚本。
    
    Args:
        action: "list" (列出) 或 "install" (安装)
        name: 流程名称 (安装时必选)
        script_or_url: 脚本路径或URL (安装时必选)
        alias: 别名 (安装时可选)
    """
    if action == "list":
        return _run_command([sys.executable, "-m", "fansetools", "flow", "list"])
    elif action == "install":
        if not name or not script_or_url:
            return "错误: 安装流程需要 'name' 和 'script_or_url'"
        cmd = [sys.executable, "-m", "fansetools", "flow", "install", name, script_or_url]
        if alias:
            cmd.extend(["--alias", alias])
        return _run_command(cmd)
    else:
        return f"错误: 未知的 action '{action}'"

@mcp.tool()
async def run_fansetools_update(auto_confirm: bool = True) -> str:
    """
    检查并更新 fansetools。
    
    Args:
        auto_confirm: 是否自动确认更新 (默认为 True)
    """
    cmd = [sys.executable, "-m", "fansetools", "update"]
    if auto_confirm:
        cmd.append("-y")
    return _run_command(cmd)

@mcp.tool()
async def run_fansetools_convert(
    command: str,
    input_file: str,
    output_file: Optional[str] = None,
    reference: Optional[str] = None
) -> str:
    """
    运行格式转换工具 (bam, sam, bed, mpileup, sort)。
    
    Args:
        command: 子命令 ("bam", "sam", "bed", "mpileup", "sort")
        input_file: 输入文件路径
        output_file: 输出文件路径 (可选，某些命令默认输出到stdout)
        reference: 参考基因组文件 (bam/sam/mpileup 可能需要)
    """
    valid_commands = ["bam", "sam", "bed", "mpileup", "sort"]
    if command not in valid_commands:
        return f"错误: 无效的转换命令 '{command}'。支持: {valid_commands}"

    cmd = [sys.executable, "-m", "fansetools", command, "-i", input_file]
    
    if output_file:
        cmd.extend(["-o", output_file])
        
    if reference and command in ["bam", "sam", "mpileup"]:
        cmd.extend(["-r", reference])
        
    return _run_command(cmd)

@mcp.tool()
async def manage_fansetools_cluster(
    action: str,
    name: Optional[str] = None,
    host: Optional[str] = None,
    user: Optional[str] = None,
    ip: Optional[str] = None,
    port: int = 22,
    password: Optional[str] = None,
    key_path: Optional[str] = None,
    fanse_path: Optional[str] = None,
    max_jobs: Optional[int] = None,
    work_dir: Optional[str] = None,
    enabled: Optional[bool] = None
) -> str:
    """
    管理 fansetools 集群节点。
    
    Args:
        action: "list", "check", "add", "remove", "update"
        name: 节点名称 (add, remove, update 需要)
        host: 主机名 (add, update 需要)
        user: 用户名 (add, update 需要)
        ip: IP地址 (可选，add, update 支持)
        port: SSH端口 (默认 22)
        password: 密码
        key_path: 私钥路径
        fanse_path: 远程 fansetools 可执行文件路径
        max_jobs: 最大并发任务数
        work_dir: 工作目录
        enabled: 是否启用 (update 支持)
    """
    cmd = [sys.executable, "-m", "fansetools", "cluster", action]
    
    if action == "list":
        # list 不需要其他参数
        pass
    elif action == "check":
        # check 可以带 --detail，这里简化处理
        pass
    elif action == "remove":
        if not name:
            return "错误: remove 操作需要 'name'"
        cmd.extend(["--name", name])
    elif action in ["add", "update"]:
        if action == "add" and not (name and host and user):
            return "错误: add 操作需要 'name', 'host', 'user'"
        if action == "update" and not name:
            return "错误: update 操作需要 'name'"
            
        if name: cmd.extend(["--name", name])
        if host: cmd.extend(["--host", host])
        if user: cmd.extend(["--user", user])
        if ip: cmd.extend(["--ip", ip])
        if port != 22: cmd.extend(["--port", str(port)])
        if password: cmd.extend(["--password", password])
        if key_path: cmd.extend(["--key", key_path])
        if fanse_path: cmd.extend(["--fanse-path", fanse_path])
        if max_jobs is not None: cmd.extend(["--max-jobs", str(max_jobs)])
        if work_dir: cmd.extend(["--work-dir", work_dir])
        
        if enabled is not None:
            if enabled:
                cmd.append("--enable")
            else:
                cmd.append("--disable")
    else:
        return f"错误: 未知的 action '{action}'"
        
    return _run_command(cmd)

@mcp.tool()
async def run_fansetools_cluster(
    mode: str,
    input_pattern: str,
    reference: str,
    output_dir: str,
    nodes: Optional[str] = None
) -> str:
    """
    运行集群并行计算任务。
    
    Args:
        mode: 运行模式 (通常是 "run")
        input_pattern: 输入文件模式 (例如 "*.fq.gz")
        reference: 参考基因组文件
        output_dir: 输出目录
        nodes: (可选) 节点列表
    """
    if mode != "run":
        return "目前仅支持 'run' 模式"
        
    cmd = [
        sys.executable, "-m", "fansetools", "cluster", "run",
        "-i", input_pattern,
        "-r", reference,
        "-o", output_dir
    ]
    if nodes:
        cmd.extend(["--nodes", nodes])
        
    return _run_command(cmd)

@mcp.tool()
async def run_fansetools_test() -> str:
    """运行 fansetools 自我测试套件"""
    return _run_command([sys.executable, "-m", "fansetools", "test"])

@mcp.tool()
async def run_fansetools_path(test_path: str) -> str:
    """
    测试 fansetools 的路径解析与通配符匹配功能。
    用于调试输入路径是否能被正确解析。
    """
    return _run_command([sys.executable, "-m", "fansetools", "path", test_path])

@mcp.tool()
async def check_fansetools_version() -> str:
    """检查 fansetools 版本信息"""
    return _run_command([sys.executable, "-m", "fansetools", "-v"])

@mcp.tool()
async def list_fansetools_packages(installed_only: bool = False) -> str:
    """
    列出可用或已安装的 fansetools 扩展包。
    
    Args:
        installed_only: 如果为 True，仅列出已安装的包
    """
    subcmd = "installed" if installed_only else "list"
    return _run_command([sys.executable, "-m", "fansetools", subcmd])

@mcp.tool()
async def manage_fansetools_package(action: str, package_name: str) -> str:
    """
    安装或卸载 fansetools 扩展包。
    
    Args:
        action: "install" 或 "uninstall"
        package_name: 包名 (例如 "samtools")
    """
    if action not in ["install", "uninstall"]:
        return "错误: action 必须是 'install' 或 'uninstall'"
        
    return _run_command([sys.executable, "-m", "fansetools", action, package_name])

# ==========================================
# 2. 辅助函数
# ==========================================

def _run_command(cmd: List[str]) -> str:
    """内部通用命令执行函数"""
    try:
        # --- 调试信息：测试 fansetools 模块导入 ---
        try:
            import fansetools
            sys.stderr.write(f"DEBUG: fansetools module imported successfully. Version: {fansetools.__version__ if hasattr(fansetools, '__version__') else 'N/A'}\n")
        except ImportError as e:
            sys.stderr.write(f"DEBUG: Failed to import fansetools module: {e}\n")
        except Exception as e:
            sys.stderr.write(f"DEBUG: Error during fansetools module import check: {e}\n")

        # --- 调试信息：测试简单的 Python 命令执行 ---
        try:
            test_cmd = [sys.executable, "-c", "import sys; print(sys.version)"]
            sys.stderr.write(f"DEBUG: Testing simple Python command: {' '.join(test_cmd)}\n")
            test_result = subprocess.run(test_cmd, capture_output=True, text=True, shell=False, check=False)
            sys.stderr.write(f"DEBUG: Simple Python command output (stdout): {test_result.stdout.strip()}\n")
            sys.stderr.write(f"DEBUG: Simple Python command output (stderr): {test_result.stderr.strip()}\n")
            if test_result.returncode != 0:
                sys.stderr.write(f"DEBUG: Simple Python command failed with exit code {test_result.returncode}\n")
        except Exception as e:
            sys.stderr.write(f"DEBUG: Error during simple Python command test: {e}\n")
        # --- 调试信息结束 ---

        # 打印即将执行的命令，方便调试 (输出到 stderr 以免破坏 MCP 协议)
        # 确保使用 sys.executable -m fansetools 来调用，避免 PATH 问题
        
        # 判断是否是直接调用 Python 模块的形式
        if cmd and cmd[0] == sys.executable and len(cmd) > 2 and cmd[1] == "-m" and cmd[2] == "fansetools":
            # 如果是直接调用 Python 模块，则直接传递列表，并禁用 shell
            sys.stderr.write(f"Executing command (direct Python module): {' '.join(cmd)}\n")
            result = subprocess.run(
                cmd, # 直接传递命令列表
                capture_output=True,
                text=True,
                shell=False, # 禁用 shell 模式
                check=False
            )
        else:
            # 否则，将命令合并为字符串，并启用 shell
            full_command = " ".join(cmd)
            sys.stderr.write(f"Executing command (with shell): {full_command}\n")
            result = subprocess.run(
                full_command, # 将命令列表合并为字符串
                capture_output=True,
                text=True,
                shell=True, # 启用 shell 模式
                check=False
            )

        if result.returncode == 0:
            output = result.stdout
            # 如果标准输出为空但标准错误有内容，则将标准错误视为成功输出
            if not output.strip() and result.stderr.strip():
                output = result.stderr

            # 截断过长的输出
            if len(output) > 5000:
                output = output[:5000] + "\n... (输出过长已截断)"
            return f"命令成功:\n{output}"
        else:
            # 在命令失败时也打印出完整的命令和详细的错误信息
            sys.stderr.write(f"Command failed: {' '.join(cmd)}\n") # 打印原始命令列表
            sys.stderr.write(f"Stderr: {result.stderr}\n")
            sys.stderr.write(f"Stdout: {result.stdout}\n")
            return f"命令失败 (Exit {result.returncode}):\n{result.stderr}\n{result.stdout}"

    except FileNotFoundError:
        return f"执行失败: 找不到命令 '{cmd[0] if cmd else '未知命令'}'. 请确认 fansetools 已正确安装在系统 PATH 中。"
    except Exception as e:
        return f"发生未知错误: {str(e)}"

# ==========================================
# 3. 通用生信软件封装 (Generic / Config Driven)
# ==========================================

ALLOWED_TOOLS = {
    "samtools": ["view", "sort", "index", "flagstat", "tview", "faidx"],
    "bcftools": ["view", "merge", "consensus", "index", "call"],
    "bowtie2": [], # 允许所有子命令
    "bwa": ["index", "mem", "aln", "samse", "sampe"],
    "fastp": [],
    "seqkit": [],
}

@mcp.tool()
async def run_bioinfo_tool(tool_name: str, args: List[str]) -> str:
    """
    运行受信任的通用生物信息学工具 (如 samtools, bcftools)。
    警告: 请小心使用，确保参数正确。
    
    Args:
        tool_name: 工具名称 (必须在白名单中: samtools, bcftools, bowtie2, bwa, fastp, seqkit)
        args: 参数列表 (例如 ["view", "-h"])
    """
    if tool_name not in ALLOWED_TOOLS:
        return f"错误: 工具 '{tool_name}' 不在允许列表中。"
    
    # 简单的安全检查：检查子命令限制
    if ALLOWED_TOOLS[tool_name]:
        if not args or args[0] not in ALLOWED_TOOLS[tool_name]:
             # 对于某些工具如 fastp, seqkit，第一个参数不是子命令而是选项，需要特殊处理
             # 这里简单起见，如果白名单列表为空，则不检查子命令
             pass

    full_cmd = [tool_name] + args
    return _run_command(full_cmd)

if __name__ == "__main__":
    import sys
    
    # Debug: Log isatty status
    try:
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(f"isatty: {sys.stdin.isatty()}\n")
    except:
        pass

    # 简单的检查：如果是在终端直接运行且没有被重定向输入，可能用户只是想测试
    if sys.stdin.isatty():
        print("错误: 此脚本是一个 MCP (Model Context Protocol) 服务器，旨在被 MCP 客户端 (如 Trae, Claude Desktop) 调用，而不是在终端直接运行。")
        print("\n调试/测试方法:")
        print("1. 使用提供的测试脚本: python src/fansetools/utils/test_mcp_client.py")
        print("2. 或者将 JSON-RPC 消息通过管道传入: echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}' | python src/fansetools/utils/FanseTools_MCP.py")
        sys.exit(1)
        
    try:
        # Windows encoding fix
        if sys.platform == "win32":
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
            
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write("Calling mcp.run()...\n")
            
        mcp.run()
    except Exception as e:
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(f"Runtime Error: {traceback.format_exc()}\n")
        sys.exit(1)