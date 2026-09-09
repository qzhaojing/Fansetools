# fansetools/bam.py
import os
import sys
import json
import subprocess
import shutil
import tempfile
from pathlib import Path
from .bin_utils import bin_manager
from .utils.rich_help import CustomHelpFormatter, add_rich_epilog

# 新增：参考序列信息缓存（header 缓存）
# 同一 -r FASTA 的 @SQ header 恒定不变，批量转换时只需 parse_fasta 一次，
# 之后每个 fanse sam 子进程通过 --ref-info-json 毫秒级读取缓存，
# 避免每个文件都重新逐行读取数 GB 网络盘 FASTA（实测 5.86GB 每次要数分钟）

def build_ref_info_cache(fasta_path: str, console=None) -> str:
    """
    解析 FASTA 一次，将 {序列名: 长度} 写入参考基因组同目录 JSON 并返回路径。

    修正：缓存直接放在参考基因组所在路径，便于自动发现与复用；
    缓存文件名固定为 <fasta>.ref_info.json，首次构建后后续自动命中。
    """
    from .sam import parse_fasta

    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    fasta_p = Path(fasta_path)
    cache_path = fasta_p.with_name(fasta_p.name + '.ref_info.json')
    if cache_path.exists() and cache_path.stat().st_size > 0:
        log(f"参考序列信息缓存已存在，直接复用: {cache_path}")
        return str(cache_path)

    log(f"构建参考序列信息缓存（仅此一次，后续转换直接复用）: {fasta_path}")
    ref_info = parse_fasta(fasta_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(ref_info, f, ensure_ascii=False)
    if not cache_path.exists() or cache_path.stat().st_size == 0:
        raise RuntimeError(f"参考序列缓存写入失败: {cache_path}")
    log(f"缓存就绪: {cache_path} ({len(ref_info)} 条序列)")
    return str(cache_path)

def _fanse_sam_cmd(fanse_file, fasta_path, ref_info_json=None, is_paired_end=False, preload_network=False, force_pair=False):
    """新增：统一构建 fanse sam 命令；提供缓存时追加 --ref-info-json 跳过 FASTA 解析"""
    cmd = ['fanse', 'sam', '-i', str(fanse_file), '-r', str(fasta_path)]
    # 修正：双端模式透传——用户命令行 --pe 需要最终传到 fanse sam
    if is_paired_end:
        cmd.append('--pe')
    # 修正(2026-09-09)：PE 配对一致性检查已在 fanse2bam 调度层统一执行，
    # 内层 fanse sam 一律传 --force-pair 跳过，避免同一文件重复检查/重复询问
    if is_paired_end and force_pair:
        cmd.append('--force-pair')
    # 修正：网络盘预加载透传——用户命令行 --preload 需要最终传到 fanse sam
    if preload_network:
        cmd.append('--preload')
    if ref_info_json:
        cmd.extend(['--ref-info-json', str(ref_info_json)])
    # 修正：强制 -t 1（单线程流式输出）。
    # fanse sam 默认 -t 4 会启用 multiprocessing Pool 并按 20000 条/批 pickle 结果，
    # 对 multi-mapping 重的文件（实测 280k 条记录产出 6.5GB SAM，约 23KB/条）单批可达
    # 数百 MB，4 个 worker + 队列积压直接 MemoryError（见 *.bam_conv.log）。
    # 管道模式下下游 samtools view/sort 才是真正的并行主力，上游流式单线程最稳。
    cmd.extend(['-t', '1'])
    return cmd


def _local_root(local):
    # --local=True 使用系统临时目录；传入路径时使用用户指定根目录。
    if local is True:
        return Path(tempfile.gettempdir()) / 'fansetools_temp'
    if local:
        return Path(local)
    return None


def _create_local_workspace(local):
    root = _local_root(local)
    if root is None:
        return None
    root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix='bam_', dir=str(root)))


def _copy_final_outputs(local_bam, target_bam, index):
    # 目标盘只在完整生成后通过 .copying 文件一次性替换，避免留下半成品。
    target_bam = Path(target_bam)
    target_bam.parent.mkdir(parents=True, exist_ok=True)
    staged_bam = target_bam.with_name(target_bam.name + '.copying')
    staged_bai = Path(str(target_bam) + '.bai.copying')
    try:
        shutil.copy2(str(local_bam), str(staged_bam))
        os.replace(str(staged_bam), str(target_bam))
        if index:
            local_bai = Path(str(local_bam) + '.bai')
            target_bai = Path(str(target_bam) + '.bai')
            shutil.copy2(str(local_bai), str(staged_bai))
            os.replace(str(staged_bai), str(target_bai))
    except Exception:
        for temporary in (staged_bam, staged_bai):
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        raise


def _finalize_bam_output(output_bam, target_bam, samtools_path, index, workspace, log, error_context='生成的 BAM'):
    # 修正意图：所有路径统一在 BAM 完整生成后校验，再建立 BAI，最后复制到目标盘。
    output_bam = Path(output_bam)
    if not output_bam.exists() or output_bam.stat().st_size < 1000:
        actual_size = output_bam.stat().st_size if output_bam.exists() else 0
        raise RuntimeError(f'{error_context}过小 ({actual_size} 字节)')
    if index:
        subprocess.run([samtools_path, 'index', str(output_bam)], check=True)
    if workspace:
        _copy_final_outputs(output_bam, target_bam, index)
    return target_bam


def _run_fixmate_pipeline(tmp_ns_bam, output_bam, samtools_path, legacy, index, log, local_work_dir=None):
    """
    双端配对专用管道：queryname-sorted BAM → fixmate → coordinate-sorted BAM

    修正：双端数据要让 IGV 识别 mate pair 关系，必须用 samtools fixmate
    基于 read name 统一设置 FLAG 位（0x1/0x2/0x40/0x80/0x20/0x8）以及
    RNEXT/PNEXT/TLEN 字段。fixmate 要求输入按 queryname 排序，处理完后
    再按 coordinate 排序得到最终 BAM。

    流程：
        tmp_ns_bam (queryname 排序)
          → samtools fixmate -m - - (stdin 读，stdout 写)
          → samtools sort -o output_bam - (coordinate 排序输出)
          → 由调用方统一校验、索引并复制最终输出

    参数:
        tmp_ns_bam:   已按 queryname 排序的 BAM 输入文件（fixmate 前提条件）
        output_bam:   最终 coordinate 排序的输出 BAM 路径
        samtools_path: samtools 可执行文件路径
        legacy:       是否为旧版 samtools 0.x（旧版无 fixmate，需 warning 跳过）
        index:        是否同时建立索引
        log:          日志函数（log(msg, style=None)）
    """
    if legacy:
        # samtools fixmate 仅在 1.0+ 提供；0.x 跳过并提示用户
        log(f"[bold yellow]警告: 当前 samtools 为 0.x 旧版，不支持 fixmate 命令。[/bold yellow]")
        log(f"[bold yellow]无法自动设置 mate pair FLAG，请升级 samtools 后重跑。[/bold yellow]")
        log(f"[bold yellow]当前输出将是未 fixmate 的 coordinate-sorted BAM。[/bold yellow]")
        return

    if not Path(tmp_ns_bam).exists() or Path(tmp_ns_bam).stat().st_size < 1000:
        raise RuntimeError(f'fixmate 输入 (queryname sorted BAM) 不存在或过小: {tmp_ns_bam}')

    log(f"[PE fixmate] 修复 mate pair 信息...")
    # fixmate -m: 同时添加 ms (mate score) 标签，有利于下游分析
    # stdin=tmp_ns_bam, stdout=交给 sort 管道
    fixmate_cmd = [samtools_path, 'fixmate', '-m', str(tmp_ns_bam), '-']
    sort_cmd = [samtools_path, 'sort', '-@', '4', '-m', '512M']
    if local_work_dir:
        # 新版 samtools 的排序分块也放入当前本地任务目录。
        sort_cmd.extend(['-T', str(Path(local_work_dir) / 'sort_coord')])
    sort_cmd.extend(['-o', str(output_bam), '-'])

    p_fix = subprocess.Popen(fixmate_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p_sort = subprocess.Popen(sort_cmd, stdin=p_fix.stdout)
    p_fix.stdout.close()

    rc_sort = p_sort.wait()
    rc_fix = p_fix.wait()

    # 收集 fixmate 的 stderr 用于诊断
    fix_stderr = p_fix.stderr.read().decode() if p_fix.stderr else ''
    if fix_stderr:
        log(f"fixmate stderr: {fix_stderr.strip()[:200]}", style="yellow")

    if rc_fix != 0:
        raise RuntimeError(f'samtools fixmate 失败 (rc={rc_fix}): {fix_stderr[:300]}')
    if rc_sort != 0:
        raise RuntimeError(f'samtools sort (fixmate后) 失败 (rc={rc_sort})')

    log(f"[PE fixmate] mate pair 信息修复完成，输出: {output_bam}")
    # 索引延后到调用方统一完成，确保先校验 BAM，再复制 BAM/BAI。

def _detect_legacy_samtools(samtools_path, log):
    """
    检测 samtools 是否为 0.x 旧版。
    旧版 (0.x) 不支持 fixmate 命令，且 sort 语法不同。
    结果缓存到模块级变量，同一进程内多次调用只检测一次。
    """
    try:
        ver_cmd = [samtools_path, '--version']
        ver_result = subprocess.run(ver_cmd, capture_output=True, text=True, check=True)
        legacy = ('samtools 0.' in ver_result.stdout) or ('samtools 0.' in ver_result.stderr)
        log(f"Samtools version check: legacy={legacy}")
        return legacy
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        log(f"Samtools version detection failed ({e}). Assuming legacy for compatibility.")
        return True
    except Exception as e:
        log(f"Unexpected error during samtools version detection ({e}). Assuming legacy for compatibility.")
        return True

def fanse2bam_unix(fanse_file, fasta_path, output_bam=None, sort=True, index=True, console=None, ref_info_json=None, is_paired_end=False, preload_network=False, local=None):
    """
    将FANSe3文件直接转换为BAM格式（精简版）
    
    参数:
        fanse_file: 输入FANSe3文件路径
        fasta_path: 参考基因组FASTA文件路径
        output_bam: 输出BAM文件路径（可选）
        sort: 是否排序BAM文件
        index: 是否创建索引
        console: rich console object (optional)
        
        
    # 基本用法（自动排序和索引）
		fanse bam -i sample.fanse3 -r reference.fa -o sample.bam

		# 不排序
		fanse bam -i sample.fanse3 -r reference.fa --no-sort

		# 不创建索引
		fanse bam -i sample.fanse3 -r reference.fa --no-index

    """
    
    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    target_bam = Path(output_bam)
    workspace = _create_local_workspace(local)
    output_bam = workspace / target_bam.name if workspace else target_bam

    # 步骤1: 直接通过管道将输入转换为BAM
    try:
        input_path = Path(fanse_file)
        input_suffix = input_path.suffix.lower()
        samtools_path = bin_manager.get_samtools_path()

        # 构建samtools命令
        samtools_cmd = [samtools_path, 'view', '-bS', '-']
        if sort:
            samtools_sort_cmd = [samtools_path, 'sort', '-@', '4', '-m', '512M']
            if workspace:
                # 本地模式下 sort 分块和最终 BAM 都留在任务目录。
                samtools_sort_cmd.extend(['-T', str(workspace / 'sort_coord')])
            samtools_sort_cmd.extend(['-o', str(output_bam)])
        else:
            samtools_cmd.extend(['-o', str(output_bam)])

        # 三种输入：
        # 1) fanse3/fanse -> fanse sam -> samtools view/sort
        # 2) sam -> 直接 samtools view/sort
        # 3) bam(已存在) -> 直接返回/报错由上层处理
        if input_suffix in ['.sam']:
            log(f"Converting SAM to BAM: {fanse_file}...")
            # 修正：SAM 输入先统一转 BAM 流，再走 sort 输出，避免直写路径在复杂 SAM 下生成空/极小文件
            if sort:
                p1 = subprocess.Popen([samtools_path, 'view', '-bS', str(fanse_file)], stdout=subprocess.PIPE)
                sort_cmd = [samtools_path, 'sort', '-@', '4', '-m', '512M']
                if workspace:
                    # 本地模式下排序分块不写网络输出目录。
                    sort_cmd.extend(['-T', str(workspace / 'sort_coord')])
                sort_cmd.extend(['-o', str(output_bam), '-'])
                p2 = subprocess.Popen(sort_cmd, stdin=p1.stdout)
                p1.stdout.close()
                p2.communicate()
                if p1.wait() != 0 or p2.returncode != 0:
                    raise RuntimeError('sam -> bam sort pipeline failed')
            else:
                #如果不sort
                with open(output_bam, 'wb') as out_f:
                    p1 = subprocess.Popen([samtools_path, 'view', '-bS', str(fanse_file)], stdout=out_f)
                    rc = p1.wait()
                    if rc != 0:
                        raise RuntimeError('sam -> bam view failed')
            # 修正意图：先完成 BAM 大小校验，再生成索引并复制最终结果。
            if not output_bam.exists() or output_bam.stat().st_size < 1000:
                actual_size = output_bam.stat().st_size if output_bam.exists() else 0
                raise RuntimeError(f'生成的 BAM 文件过小 ({actual_size} 字节)')
            return _finalize_bam_output(
                output_bam, target_bam, samtools_path, index, workspace, log,
                error_context='生成的 BAM 文件'
            )

        # 修正意图：调度层已完成配对检查；此处固定跳过重复检查。
        fanse_cmd = _fanse_sam_cmd(
            fanse_file, fasta_path, ref_info_json, is_paired_end,
            preload_network, force_pair=True
        )
        log(f"Converting {fanse_file} to BAM...")

        if sort:
            # fanse sam | samtools view -bS - | samtools sort -o output.bam
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(samtools_cmd, stdin=p1.stdout, stdout=subprocess.PIPE)
            p3 = subprocess.Popen(samtools_sort_cmd, stdin=p2.stdout)
            p1.stdout.close()
            p2.stdout.close()
            p3.communicate()
            
        else:
            # fanse sam | samtools view -bS - -o output.bam
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(samtools_cmd, stdin=p1.stdout)
            p1.stdout.close()
            p2.communicate()

        # 修正意图：所有输入类型都统一在 BAM 完整生成后校验、索引和复制。
        result = _finalize_bam_output(
            output_bam, target_bam, samtools_path, index, workspace, log,
            error_context='生成的 BAM 文件'
        )
        log(f"Successfully created BAM file: {target_bam}", style="bold green")
        return result
        
    except subprocess.CalledProcessError as e:
        log(f"Error converting to BAM: {e.stderr.decode() if e.stderr else str(e)}", style="bold red")
        sys.exit(1)
    except Exception as e:
        log(f"Unexpected error: {str(e)}", style="bold red")
        sys.exit(1)
    finally:
        if workspace:
            shutil.rmtree(workspace, ignore_errors=True)

from .utils.path_utils import PathProcessor
from rich.console import Console

def bam_command(args):
    """处理bam子命令（精简版）"""
    console = Console(force_terminal=True)
    processor = PathProcessor()
    
    # 1. 解析输入文件
    try:
        # 修正：bam 既支持 fanse3，也支持上一步生成的 SAM（尤其是 *_PE.sam 双端合并结果）
        input_files = processor.parse_input_paths(args.fanse_file, ['.fanse3', '.fanse', '.sam'])
    except Exception as e:
        console.print(f"[bold red]错误: 解析输入文件失败 - {e}[/bold red]")
        sys.exit(1)

    if not input_files:
        console.print(f"[bold red]错误: 未找到有效的输入文件: {args.fanse_file}[/bold red]")
        sys.exit(1)
        
    # 2. 处理输出
    output_path = Path(args.output_bam) if args.output_bam else None

    # 新增：并行线程数（仅批量模式生效；单文件模式管道本身已是 fanse sam/samtools view/sort
    # 三个子进程的流式并行，再并行无意义）
    threads = max(1, int(getattr(args, 'threads', 1) or 1))

    # 新增：header 缓存——同一 -r FASTA 的 @SQ header 恒定，只解析一次写本地 JSON，
    # 后续每个 fanse sam 子进程毫秒级读缓存，避免每文件重复读取数 GB 网络盘 FASTA
    ref_info_cache = None
    try:
        ref_info_cache = build_ref_info_cache(args.fasta_path, console)
    except Exception as e:
        # 修正：缓存构建失败不阻断转换，回退为每个 fanse sam 自行解析 FASTA（旧行为）
        console.print(f"[bold yellow]警告: 参考序列缓存构建失败({e})，将回退为逐文件解析FASTA[/bold yellow]")
        ref_info_cache = None

    # 批量模式检查
    if len(input_files) > 1:
        if output_path and output_path.suffix:
             console.print(f"[bold red]错误: 批量处理 {len(input_files)} 个文件时，输出路径必须是目录 (如果指定)[/bold red]")
             sys.exit(1)

        if output_path and not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)

        console.print(f"检测到批量模式，将处理 {len(input_files)} 个文件...")

        # 新增：threads>1 时用线程池并行转换（每个任务各自拉起独立的
        # fanse sam | samtools view | samtools sort 子进程管道，互不干扰）
        if threads > 1:
            import concurrent.futures
            console.print(f"[bold blue]启用并行模式: {threads} 个线程同时转换[/bold blue]")

            def _convert_one(infile):
                outfile = output_path / (infile.stem + ".bam") if output_path else None
                try:
                    fanse2bam(
                        fanse_file=str(infile),
                        fasta_path=args.fasta_path,
                        output_bam=outfile,
                        sort=not args.no_sort,
                        index=not args.no_index,
                        keep_sam=getattr(args, 'sam', False),
                        console=console,
                        ref_info_json=ref_info_cache,  # 新增：header缓存透传
                        is_paired_end=getattr(args, 'is_paired_end', False),  # 修正：--pe 双端模式透传
                        preload_network=getattr(args, 'preload', False),  # 修正：--preload 网络盘预加载透传
                        force_pair=getattr(args, 'force_pair', False),  # 修正(2026-09-09): 配对检查跳过透传
                        local=getattr(args, 'local', None)
                    )
                    return infile.name, None
                except Exception as e:
                    return infile.name, e

            ok_count = fail_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                futures = [executor.submit(_convert_one, infile) for infile in input_files]
                for fut in concurrent.futures.as_completed(futures):
                    name, err = fut.result()
                    if err is None:
                        ok_count += 1
                        console.print(f"[bold green]完成 ({ok_count + fail_count}/{len(input_files)}): {name}[/bold green]")
                    else:
                        fail_count += 1
                        console.print(f"[bold red]处理 {name} 失败: {err}[/bold red]")
            console.print(f"批量转换完成: 成功 {ok_count}, 失败 {fail_count}")
            # 修正：删除了原 try/finally 结构——原 finally 里主动删除 ref_info_cache
            # 导致 FASTA 缓存每次被删掉无法复用。ref_info_cache 放在 FASTA 同目录是持久化的。
            if fail_count:
                sys.exit(1)
            return

        for infile in input_files:
            console.print(f"\n[bold blue]处理任务 ({input_files.index(infile) + 1}/{len(input_files)}): {infile.name}[/bold blue]")
            
            # 确定单个文件的输出路径
            if output_path:
                outfile = output_path / (infile.stem + ".bam")
            else:
                outfile = None # fanse2bam 会自动处理为同目录同名
            
            try:
                fanse2bam(
                    fanse_file=str(infile),
                    fasta_path=args.fasta_path,
                    output_bam=outfile,
                    sort=not args.no_sort,
                    index=not args.no_index,
                    keep_sam=getattr(args, 'sam', False),
                    console=console,
                    ref_info_json=ref_info_cache,  # 新增：header缓存透传
                    is_paired_end=getattr(args, 'is_paired_end', False),  # 修正：--pe 双端模式透传
                    force_pair=getattr(args, 'force_pair', False),  # 修正(2026-09-09): 配对检查跳过透传
                    local=getattr(args, 'local', None)
                )
            except Exception as e:
                console.print(f"[bold red]处理 {infile.name} 失败: {e}[/bold red]")
                
    else:
        # 单文件模式
        infile = input_files[0]
        try:
            fanse2bam(
                fanse_file=str(infile),
                fasta_path=args.fasta_path,
                output_bam=output_path,
                sort=not args.no_sort,
                index=not args.no_index,
                keep_sam=getattr(args, 'sam', False),
                console=console,
                ref_info_json=ref_info_cache,  # 新增：header缓存透传
                is_paired_end=getattr(args, 'is_paired_end', False),  # 修正：--pe 双端模式透传
                force_pair=getattr(args, 'force_pair', False),  # 修正(2026-09-09): 配对检查跳过透传
                local=getattr(args, 'local', None)
            )
        except Exception as e:
            console.print(f"[bold red]错误: {e}[/bold red]")
            sys.exit(1)
        # 修正：同批量模式，ref_info_cache 是持久化缓存，不再主动删除
        # （跨运行复用可节省数分钟 FASTA 解析时间）

def fanse2bam_win_pipe(fanse_file, fasta_path, output_bam=None, sort=True, index=True, console=None, ref_info_json=None, is_paired_end=False, preload_network=False, local=None):
    #直接原位生成BAM文件，并进行排序索引
    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    target_bam = Path(output_bam)
    workspace = _create_local_workspace(local)
    output_bam = workspace / target_bam.name if workspace else target_bam

    # 修正：Windows 路径此前未按输入后缀分流，.sam 被错误交给 fanse sam 导致空 BAM
    # （fanse sam 无法解析 SAM 格式，产生空输出流 → 下游 samtools 生成 92 字节空骨架）。
    # 以下镜像 Unix 路径 fanse2bam_unix() L111-130 的分流语义：
    #   .sam → 直接 samtools view/sort，提前 return
    #   .fanse3/.fanse → fanse sam → samtools view/sort
    input_path = Path(fanse_file)
    input_suffix = input_path.suffix.lower()
    samtools_path = bin_manager.get_samtools_path()

    if input_suffix in ['.sam']:
        # 修正意图：SAM 直通路径使用本地输出，并在完成或失败后清理 workspace。
        log(f"[SAM 直通] 检测到 .sam 输入，跳过 fanse sam 直接转换: {fanse_file}")
        # 用独立日志记录 SAM 直通过程，避免与 fanse sam 日志混淆
        conv_log = target_bam.with_suffix('.bam_conv.log')

        # 修正：PE 模式需要 fixmate，先检查 samtools 版本
        # （.sam 路径原先未做版本检查，此处补齐以支持 fixmate）
        legacy = _detect_legacy_samtools(samtools_path, log)

        # PE + sort → 特殊管道：view → sort -n (queryname) → fixmate → sort (coordinate) → index
        if is_paired_end and sort:
            tmp_ns_bam = Path(output_bam).with_suffix('.tmp.ns.bam')
            tmp_ns_bam.parent.mkdir(parents=True, exist_ok=True)
            log(f"[PE 模式] SAM 直通 + fixmate: view → sort -n → fixmate → sort (coord)...")
            try:
                # Step 1: SAM → BAM → queryname 排序临时文件
                p1 = subprocess.Popen([samtools_path, 'view', '-bS', str(fanse_file)], stdout=subprocess.PIPE)
                name_sort_cmd = [samtools_path, 'sort', '-n', '-@', '4', '-m', '512M']
                if workspace:
                    # PE name sort 的分块也放入本地任务目录。
                    name_sort_cmd.extend(['-T', str(workspace / 'sort_name')])
                name_sort_cmd.extend(['-o', str(tmp_ns_bam), '-'])
                p2 = subprocess.Popen(name_sort_cmd, stdin=p1.stdout)
                p1.stdout.close()
                p2.communicate()
                rc1, rc2 = p1.wait(), p2.returncode
                if rc1 != 0 or rc2 != 0:
                    raise RuntimeError(f'SAM→queryname-sort 管道失败 (view={rc1}, sort={rc2})')

                # Step 2: fixmate → coordinate sort → index（含 legacy 检查）
                if not legacy:
                    _run_fixmate_pipeline(tmp_ns_bam, output_bam, samtools_path, legacy, index, log, workspace)
                else:
                    # 旧版 samtools 无 fixmate，退回普通 coordinate sort。
                    log("[bold yellow]警告: 旧版 samtools 无 fixmate，退回普通 coordinate sort[/bold yellow]")
                    output_prefix = str(output_bam).removesuffix('.bam')
                    subprocess.run([
                        samtools_path,
                        'sort',
                        str(tmp_ns_bam),
                        output_prefix
                    ], check=True)
            finally:
                if tmp_ns_bam.exists():
                    try: tmp_ns_bam.unlink()
                    except OSError: pass

        elif sort:
            # 非 PE + sort：SAM 先转 BAM，再在本地任务目录完成排序。
            p1 = subprocess.Popen(
                [samtools_path, 'view', '-bS', str(fanse_file)],
                stdout=subprocess.PIPE
            )
            sort_cmd = [samtools_path, 'sort', '-@', '4', '-m', '512M']
            if workspace:
                # 修正意图：排序分块只写本地任务目录，避免网络盘临时 I/O。
                sort_cmd.extend(['-T', str(workspace / 'sort_coord')])
            sort_cmd.extend(['-o', str(output_bam), '-'])
            p2 = subprocess.Popen(sort_cmd, stdin=p1.stdout)
            p1.stdout.close()
            p2.communicate()
            rc1 = p1.wait()
            rc2 = p2.returncode
            if rc1 != 0 or rc2 != 0:
                raise RuntimeError(f'sam -> bam sort pipeline failed (view={rc1}, sort={rc2})')

        else:
            # 不排序：直接 view 写 BAM（PE 也无法 fixmate，fixmate 要求 name-sorted 前提）
            with open(output_bam, 'wb') as out_f:
                p1 = subprocess.Popen([samtools_path, 'view', '-bS', str(fanse_file)], stdout=out_f)
                rc = p1.wait()
                if rc != 0:
                    raise RuntimeError(f'sam -> bam view failed (rc={rc})')

        # 修正：输出 BAM 有效性校验——避免生成 92 字节空骨架却继续索引
        result = _finalize_bam_output(
            output_bam, target_bam, samtools_path, index, workspace, log,
            error_context='生成的 BAM 文件'
        )
        log(f"成功创建 BAM 文件: {target_bam}", style="bold green")
        # .sam 直通分支提前返回，不能依赖后方 fanse3 分支的 finally 清理。
        if workspace:
            shutil.rmtree(workspace, ignore_errors=True)
        return result

    # fanse3/fanse 路径：构建 fanse sam 命令 + 日志重定向
    conv_log = target_bam.with_suffix('.bam_conv.log')
    conv_log_fp = None
    try:
        fanse_cmd = _fanse_sam_cmd(fanse_file, fasta_path, ref_info_json, is_paired_end, preload_network, force_pair=True)  # 修正(2026-09-09): 上游调度层已做配对检查，内层跳过
        # samtools_path 已在后缀分流前获取（L327），这里直接复用避免重复调用
        log(f"Using samtools from: {samtools_path}")

        # 修正：复用统一的 legacy 检测函数
        legacy = _detect_legacy_samtools(samtools_path, log)

        samtools_view = [samtools_path, 'view', '-bS', '-']
        log(f"Converting {fanse_file} to BAM via pipe...")
        # 修正：fanse sam 的 stderr 原为 PIPE 时从未读取会导致缓冲区写满死锁，
        # 改为重定向到日志文件（方案A）：既消除死锁，又保留进度/警告供 tail 排查
        conv_log_fp = open(conv_log, 'w', encoding='utf-8')
        log(f"fanse sam 进度日志: {conv_log}")

        # ===== PE + sort：特殊管道 fanse sam → view → sort -n → fixmate → sort (coord) =====
        if is_paired_end and sort:
            tmp_ns_bam = Path(output_bam).with_suffix('.tmp.ns.bam')
            log(f"[PE 模式] fanse3 + fixmate: fanse sam → view → sort -n → fixmate → sort (coord)...")
            try:
                # Step 1: fanse sam | view | sort -n → queryname 排序临时 BAM
                p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE, stderr=conv_log_fp)
                p2 = subprocess.Popen(samtools_view, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                name_sort_cmd = [samtools_path, 'sort', '-n', '-@', '4', '-m', '512M']
                if workspace:
                    # PE name sort 的分块也放入本地任务目录。
                    name_sort_cmd.extend(['-T', str(workspace / 'sort_name')])
                name_sort_cmd.extend(['-o', str(tmp_ns_bam), '-'])
                p3 = subprocess.Popen(name_sort_cmd, stdin=p2.stdout, stderr=subprocess.DEVNULL)
                p1.stdout.close()
                p2.stdout.close()
                rc_p3 = p3.wait()
                rc_p2 = p2.wait()
                rc_p1 = p1.wait()
                if rc_p1 != 0:
                    raise RuntimeError(f'fanse sam 失败 (rc={rc_p1})，详见日志: {conv_log}')
                if rc_p2 != 0:
                    raise RuntimeError(f'samtools view 失败 (rc={rc_p2})')
                if rc_p3 != 0:
                    raise RuntimeError(f'samtools sort -n (queryname) 失败 (rc={rc_p3})')

                # Step 2: fixmate → coordinate sort → index（含 legacy 检查）
                if not legacy:
                    _run_fixmate_pipeline(tmp_ns_bam, output_bam, samtools_path, legacy, index, log, workspace)
                else:
                    log("[bold yellow]警告: 旧版 samtools 无 fixmate，退回普通 coordinate sort[/bold yellow]")
                    # 旧版 samtools 不支持新版 -T/-o 参数，保持其原生语法。
                    output_prefix = str(output_bam).removesuffix('.bam')
                    subprocess.run([samtools_path, 'sort', str(tmp_ns_bam), output_prefix], check=True)
            finally:
                if tmp_ns_bam.exists():
                    try: tmp_ns_bam.unlink()
                    except OSError: pass

        # ===== 非 PE + sort + 新版 samtools：fanse sam | view | sort =====
        elif sort and not legacy:
            # fanse sam | samtools view -bS - | samtools sort -o output.bam
            samtools_sort = [samtools_path, 'sort', '-@', '4', '-m', '512M']
            if workspace:
                # 新版 sort 分块统一落到本地任务目录。
                samtools_sort.extend(['-T', str(workspace / 'sort_coord')])
            samtools_sort.extend(['-o', str(output_bam), '-'])
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE, stderr=conv_log_fp)
            p2 = subprocess.Popen(samtools_view, stdin=p1.stdout, stdout=subprocess.PIPE)
            p3 = subprocess.Popen(samtools_sort, stdin=p2.stdout)
            p1.stdout.close()
            p2.stdout.close()
            rc_p3 = p3.wait()
            # 修正：p1 (fanse sam) 返回码检查——此前缺失导致 fanse sam 失败时下游
            # 仍继续处理空输出，最终生成 92 字节空 BAM 骨架而无任何报错
            rc_p1 = p1.wait()
            if rc_p1 != 0:
                raise RuntimeError(f'fanse sam 失败 (rc={rc_p1})，详见日志: {conv_log}')
            if rc_p3 != 0:
                raise RuntimeError(f'samtools sort 失败 (rc={rc_p3})')

        # ===== 非 PE + sort + 旧版 samtools =====
        elif sort and legacy:  # 基本都是走这条通道（当前环境安装的是旧版 samtools）
            # 通过管道连接排序，避免临时文件，直接输出排序后的 BAM 文件
            log("Using legacy samtools pipe for sorting (attempting direct output)...")
            output_prefix = str(output_bam).replace('.bam', '')
            # 旧版 samtools sort 语法: `sort <in.bam> <out.prefix>`，stdin 时用 `sort - <out.prefix>`
            samtools_sort_legacy = [samtools_path, 'sort', '-', output_prefix]
            
            # 修正：p2 的 stderr=PIPE 从未被读取，改为 DEVNULL 避免管道缓冲区写满死锁
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE, stderr=conv_log_fp)
            p2 = subprocess.Popen(samtools_view, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            p3 = subprocess.Popen(samtools_sort_legacy, stdin=p2.stdout, stderr=subprocess.PIPE)
            p1.stdout.close()
            p2.stdout.close()
            
            stdout_p3, stderr_p3 = p3.communicate()
            rc_p1 = p1.wait()  # 修正：添加 fanse sam 返回码检查
            if rc_p1 != 0:
                raise RuntimeError(f'fanse sam 失败 (rc={rc_p1})，详见日志: {conv_log}')
            if stdout_p3:
                log(f"Samtools sort (legacy) stdout: {stdout_p3.decode()}")
            if stderr_p3:
                log(f"Samtools sort (legacy) stderr: {stderr_p3.decode()}", style="red")
            if p3.returncode != 0:
                raise RuntimeError(f'samtools sort (legacy) failed (rc={p3.returncode})')

        else:
            # 不排序：直接写入输出 BAM（PE 时也无法 fixmate）
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE, stderr=conv_log_fp)
            samtools_view_nosort = [samtools_path, 'view', '-bS', '-', '-o', str(output_bam)]
            p2 = subprocess.Popen(samtools_view_nosort, stdin=p1.stdout)
            p1.stdout.close()
            rc_p2 = p2.wait()
            rc_p1 = p1.wait()  # 修正：添加 fanse sam 返回码检查
            if rc_p1 != 0:
                raise RuntimeError(f'fanse sam 失败 (rc={rc_p1})，详见日志: {conv_log}')
            if rc_p2 != 0:
                raise RuntimeError(f'samtools view failed (rc={rc_p2})')

        # 修正意图：BAM 校验、索引和最终复制统一收尾，避免各分支提前索引。
        result = _finalize_bam_output(
            output_bam, target_bam, samtools_path, index, workspace, log,
            error_context=f'生成的 BAM 文件（详见日志: {conv_log}）'
        )
        if conv_log_fp:
            conv_log_fp.close()
            conv_log_fp = None
        log(f"Successfully created BAM file: {output_bam}", style="bold green")
        return result
    except Exception as e:
        if conv_log_fp:
            conv_log_fp.close()
            conv_log_fp = None
        log(f"Pipe conversion failed: {e}", style="bold red")
        raise
    finally:
        # 本地任务失败或完成后仅清理任务子目录，保留日志和 local 根目录。
        if workspace:
            shutil.rmtree(workspace, ignore_errors=True)
        # 修正：确保日志句柄在任何路径下都被关闭，避免并行模式下句柄泄漏
        if conv_log_fp is not None:
            try:
                if not conv_log_fp.closed:
                    conv_log_fp.close()
            except Exception:
                pass
            finally:
                conv_log_fp = None

def fanse2bam_win(fanse_file, fasta_path, output_bam=None, sort=True, index=True, keep_sam=False, console=None, is_paired_end=False, preload_network=False, local=None):
    """Windows专用版本（使用临时文件，避免管道问题）"""
    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    target_bam = Path(output_bam)
    workspace = _create_local_workspace(local)
    output_bam = workspace / target_bam.name if workspace else target_bam
    
    temp_sam = (workspace / (target_bam.stem + '.temp.sam')) if workspace else target_bam.with_suffix('.temp.sam')
    temp_bam = (workspace / (target_bam.stem + '.temp.bam')) if workspace else target_bam.with_suffix('.temp.bam')

    # 修正：fallback 路径此前同样未按后缀分流，.sam 被错误交给 fanse sam
    # 镜像 fanse2bam_win_pipe() 的分流逻辑：.sam 跳过 fanse sam，直接走 samtools
    input_path = Path(fanse_file)
    input_suffix = input_path.suffix.lower()

    try:
        # 修正：samtools_path 获取移入 try 块内，找不到时让 fallback 有机会触发
        samtools_path = bin_manager.get_samtools_path()

        if input_suffix in ['.sam']:
            log(f"[SAM 直通] fallback 模式检测到 .sam 输入，跳过 fanse sam: {fanse_file}")
            # 直接把输入 SAM 当成 temp_sam 使用（无需 fanse sam 中间步骤）
            # 为保持后续统一流程，复制一份（或直接用原路径）
            temp_sam = input_path

        # 仅 .fanse3/.fanse 输入才需要 fanse sam 转换
        if input_suffix not in ['.sam']:
            log(f"Creating temporary SAM file: {temp_sam}")
            # 修正：加 -t 1 单线程流式输出。fallback 常见于超大 multi 文件（内存
            # 敏感场景），多进程模式每批 20000 条 pickle 回传会重现 MemoryError
            fanse_sam_args = ['fanse', 'sam', '-t', '1', '-i', str(fanse_file), '-r', str(fasta_path), '-o', str(temp_sam)]
            if is_paired_end:
                fanse_sam_args.append('--pe')  # 修正：透传双端模式
                # 修正(2026-09-09): 调度层已做配对检查，内层跳过避免重复询问
                fanse_sam_args.append('--force-pair')
            # 修正意图：stderr 实时写入原始输出目录日志，便于网络任务随时查看。
            conv_log = target_bam.with_suffix('.bam_conv.log')
            with open(conv_log, 'w', encoding='utf-8') as conv_log_fp:
                subprocess.run(fanse_sam_args, stderr=conv_log_fp, check=True)

        # 转换为BAM
        log(f"Using samtools from: {samtools_path}")

        # 先将SAM转换为BAM（临时文件）
        subprocess.run([samtools_path, 'view', '-bS', str(temp_sam), '-o', str(temp_bam)], check=True)

        if sort:
            # 修正：PE 模式下 temp_bam 需要先按 queryname 排序才能 fixmate
            # 新版: sort -@ 1 -m 512M -o out.bam in.bam；旧版(0.x): sort in.bam out.prefix
            legacy = _detect_legacy_samtools(samtools_path, log)

            if is_paired_end and not legacy:
                # PE + sort + 新版 samtools → queryname-sort → fixmate → coordinate-sort
                tmp_ns_bam = Path(output_bam).with_suffix('.tmp.ns.bam')
                log(f"[PE 模式] fallback + fixmate: sort -n → fixmate → sort (coord)...")
                try:
                    name_sort_cmd = [samtools_path, 'sort', '-n', '-@', '4', '-m', '512M']
                    if workspace:
                        # 修正意图：PE queryname sort 的临时分块写入本地任务目录。
                        name_sort_cmd.extend(['-T', str(workspace / 'sort_name')])
                    name_sort_cmd.extend(['-o', str(tmp_ns_bam), str(temp_bam)])
                    subprocess.run(name_sort_cmd, check=True)
                    _run_fixmate_pipeline(tmp_ns_bam, output_bam, samtools_path, legacy, index, log, workspace)
                finally:
                    if tmp_ns_bam.exists():
                        try: tmp_ns_bam.unlink()
                        except OSError: pass
            else:
                # 非 PE 或旧版 samtools：原有 sort 逻辑不变
                if is_paired_end and legacy:
                    log("[bold yellow]警告: 旧版 samtools 无 fixmate，跳过 mate pair 修复[/bold yellow]")
                try:
                    sort_cmd = [samtools_path, 'sort', '-@', '4', '-m', '512M']
                    if workspace:
                        # fallback 的新版 sort 临时分块也写本地任务目录。
                        sort_cmd.extend(['-T', str(workspace / 'sort_coord')])
                    sort_cmd.extend(['-o', str(output_bam), str(temp_bam)])
                    subprocess.run(sort_cmd,
                                   check=True)
                except subprocess.CalledProcessError:
                    # 旧版语法兜底
                    output_prefix = str(output_bam).replace('.bam', '')
                    subprocess.run([samtools_path, 'sort', str(temp_bam), output_prefix], check=True)
        else:
            # 不排序，直接移动临时BAM到输出位置
            temp_bam.rename(output_bam)

        return _finalize_bam_output(
            output_bam, target_bam, samtools_path, index, workspace, log,
            error_context='生成的 BAM 文件'
        )
    except subprocess.CalledProcessError as e:
        log(f"Samtools error: {e.stderr.decode() if e.stderr else str(e)}", style="bold red")
        raise
    except Exception as e:
        log(f"Unexpected error: {str(e)}", style="bold red")
        raise
    finally:
        # 本地模式失败时清理任务子目录，不触碰输入、日志和本地根目录。
        if workspace:
            shutil.rmtree(workspace, ignore_errors=True)
        # 修正：.sam 直通模式下 temp_sam 指向用户原始输入文件，
        # 此时绝对不能删它——只删真正由本函数创建的临时 temp.sam
        files_to_clean = [temp_bam]
        if input_suffix not in ['.sam'] and not keep_sam:
            files_to_clean.append(temp_sam)
        for temp_file in files_to_clean:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception as e:
                    log(f"Warning: Could not delete temporary file {temp_file}: {e}", style="yellow")

def _fallback_conversion(fanse_file, fasta_path, output_bam, sort, index, console=None):
    """备用转换方法"""
    if console:
        console.print("Using fallback conversion method...", style="yellow")
    else:
        print("Using fallback conversion method...")
    # 这里可以添加纯Python的SAM到BAM转换逻辑
    # 或者尝试其他方法
    
    raise RuntimeError("Fallback conversion not implemented. Please install a working version of samtools.")

def fanse2bam(fanse_file, fasta_path, output_bam=None, sort=True, index=True, keep_sam=False, console=None, ref_info_json=None, is_paired_end=False, preload_network=False, force_pair=False, local=None):
    """自动选择平台最优方法"""
    # 修正(2026-09-09): PE 模式统一在调度层做 R1/R2 配对一致性检查（唯一检查点）：
    # 1) 用户 --force-pair → ensure_pair_consistency(force=True) 仅打印警告
    # 2) 检查通过后给内层所有 fanse sam 子进程追加 --force-pair，避免重复检查/询问
    # 内层平台函数无需感知用户原始 force_pair 值
    if is_paired_end:
        try:
            from .sam import _discover_paired_fanse_files, ensure_pair_consistency
            _r1, _r1u, _r2f, _r2u = _discover_paired_fanse_files(Path(fanse_file))
            if _r2f:
                ensure_pair_consistency(str(fanse_file), str(_r2f),
                                        force=bool(force_pair), console=console)
        except KeyboardInterrupt:
            raise  # 用户在确认提示中选择否 → 原样上抛终止转换
        except RuntimeError:
            raise  # 非交互环境的 mismatch 报错 → 原样上抛
        except Exception:
            pass  # R2 发现/检查本身的意外异常不阻断转换，交由内层原有警告逻辑兜底
    if os.name == 'nt':
        try:
            # 新增：ref_info_json 透传给管道转换，跳过每文件的 FASTA 全量解析
            return fanse2bam_win_pipe(fanse_file, fasta_path, output_bam, sort, index, console, ref_info_json, is_paired_end, preload_network, local)
        except Exception:
            #如果Windows版本失败，尝试使用传统方法
            return fanse2bam_win(fanse_file, fasta_path, output_bam, sort, index, keep_sam, console, is_paired_end, preload_network, local)
    else:  # Linux/Mac
        return fanse2bam_unix(fanse_file, fasta_path, output_bam, sort, index, console, ref_info_json, is_paired_end, preload_network, local)



	# fansetools/cli.py
def add_bam_subparser(subparsers):
    """添加精简版bam子命令解析器"""
    bam_parser = subparsers.add_parser(
        'bam',
        help='直接转换FANSe3文件为BAM格式',
        description='将FANSe3文件直接转换为BAM格式（自动排序和索引）。支持通配符批量处理。',
        formatter_class=CustomHelpFormatter
    )
    
    bam_parser.add_argument(
        '-i', '--input', dest='fanse_file', required=True,
        help='输入FANSe3文件路径 (支持通配符 *.fanse3)'
    )
    bam_parser.add_argument(
        '-r', '--fasta', dest='fasta_path', required=True,
        help='参考基因组FASTA文件路径'
    )
    bam_parser.add_argument(
        '-o', '--output', dest='output_bam',
        help='输出BAM文件路径或目录（默认：输入文件同目录，同名.bam）'
    )
    bam_parser.add_argument(
        '--no-sort', action='store_true',
        help='不排序BAM文件'
    )
    bam_parser.add_argument(
        '--no-index', action='store_true',
        help='不创建BAM索引'
    )

    bam_parser.add_argument(
        '-s', '--sam', action='store_true',
        help='保留中间SAM文件（Windows临时文件模式）'
    )

    # 新增：双端模式（自动调用 samtools fixmate 修复 mate pair FLAG）
    # 无论 .fanse3 还是 .sam 输入，PE + sort 模式都会走：
    #   sort -n (queryname) → samtools fixmate -m → sort (coordinate) → index
    # 旧版 samtools 0.x 无 fixmate，会 warning 并退回普通 coordinate sort
    bam_parser.add_argument(
        '--pe', '--paired-end', action='store_true', dest='is_paired_end',
        help='双端配对模式：自动调用 samtools fixmate 修复 mate pair FLAG（0x1/0x40/0x80/'
             '0x2/0x20/0x8）及 RNEXT/PNEXT/TLEN，使 IGV 等工具正确显示 pair 关系。'
             '内部流程: sort -n (queryname) → fixmate → sort (coordinate) → index。'
             '需要 samtools ≥1.0，旧版 0.x 会 warning 并跳过 fixmate。'
    )
    bam_parser.add_argument(
        '--preload', action='store_true',
        help='网络盘 UNC 路径文件预加载到本地 temp（加速 5-10x），透传给内部 fanse sam。'
             '默认不预加载，在本地 SSD + 千兆网络环境下效果显著；本地文件会自动跳过。'
    )
    bam_parser.add_argument(
        '--local', nargs='?', const=True, default=None, metavar='PATH',
        help='将 BAM 中间文件、PE fixmate 和排序放入本地工作区；不带 PATH 时使用系统临时目录下的 fansetools_temp，带 PATH 时使用指定目录；与 --preload 独立。'
    )
    # 修正(2026-09-09): PE 配对一致性检查的跳过开关
    bam_parser.add_argument(
        '--force-pair', action='store_true',
        help='跳过 --pe 模式的 R1/R2 配对一致性检查（flowcell 集合+QNAME 采样，毫秒级）。'
             '默认开启检查：R1/R2 flowcell 集合不相交（典型场景：不同测序 run 的文件'
             '被错误配对，fixmate 会静默剥掉全部 paired 标志）时，交互终端询问是否继续，'
             '非交互环境（脚本/批处理）直接报错终止。确认数据无误后可用此参数跳过。'
    )

    # 新增：并行转换线程数（仅批量模式生效）
    bam_parser.add_argument(
        '-t', '--threads', type=int, default=1,
        help='并行转换线程数（仅批量模式生效）：每个线程独立运行一条 '
             '"fanse sam | samtools view | samtools sort" 管道（3个子进程），'
             '线程数为N时同时转换N个文件；单文件模式下管道本身已是多进程流式并行，'
             '增大该值无效果。建议2-4，默认1（串行）。注意每个线程约占1个CPU核与数百MB内存'
    )

    bam_parser.set_defaults(func=bam_command)

    add_rich_epilog(bam_parser, """
[bold]功能说明:[/bold]
  直接将 FANSe3 结果转换为 BAM 文件，无需生成中间的 SAM 文件。
  自动调用 samtools 进行排序和索引 (需安装 samtools)。
  转换前自动解析一次 FASTA 生成参考序列信息缓存（本地临时JSON），
  后续每个文件的 fanse sam 直接读缓存跳过数GB级FASTA重复解析，大幅提速。
  每个文件的 fanse sam 进度/警告写入 <输出名>.bam_conv.log，可随时 tail 查看。

[bold]示例:[/bold]
  1. 标准转换 (自动排序+索引):
     [green]fanse bam -i sample.fanse3 -r ref.fa -o sample.bam[/green]

  2. 仅转换不排序:
     [green]fanse bam -i sample.fanse3 -r ref.fa --no-sort[/green]

  3. 批量处理 (使用通配符):
     [green]fanse bam -i "*.fanse3" -r ref.fa[/green]

  4. 批量并行转换 (4个文件同时转换):
     [green]fanse bam -i "*.fanse3" -r ref.fa -o bam_out/ -t 4[/green]

[bold]双端模式 (--pe) 与配对检查:[/bold]
  --pe 会自动搜索同目录的 R2.fanse3/R2.unmapped 合并四文件流，并执行:
  sort -n → fixmate → sort (coordinate) → index。
  转换前默认执行 R1/R2 配对一致性检查（毫秒级头部采样）:
  - R1/R2 flowcell 集合一致或多 run 合并（部分重叠）→ 直接继续
  - 集合完全不相交（如不同测序 run 的文件被错误配对）→ 交互终端询问
    是否继续；非交互环境（脚本/批处理）直接报错，此时请先人工确认
    两个文件是否真的为 mate pair
  - 确认数据无误后可用 [green]--force-pair[/green] 跳过检查:
     [green]fanse bam --pe -i sample_R1.fanse3 -r ref.fa -o sample_PE.bam --force-pair[/green]
  检查背景: R1/R2 来自不同 flowcell 时 fixmate 找不到任何同名 read，
  会把全部记录按 orphan 处理剥掉 paired 标志(0x1)，且管道三进程返回码
  全为 0 静默通过——只能靠此类前置检查拦截。
""")
