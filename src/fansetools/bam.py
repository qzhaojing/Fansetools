# fansetools/bam.py
import os
import sys
import json
import subprocess
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
    解析 FASTA 一次，将 {序列名: 长度} 写入本地临时 JSON 并返回路径。

    修正：缓存放本地临时目录而非网络盘输出目录——本地写读都快且可靠，
    避免对 \\fs2 这类网络盘的额外写放大。
    """
    from .sam import parse_fasta

    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    log(f"构建参考序列信息缓存（仅此一次，后续转换直接复用）: {fasta_path}")
    ref_info = parse_fasta(fasta_path)
    cache_path = os.path.join(tempfile.gettempdir(), f'fansetools_ref_info_{os.getpid()}.json')
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(ref_info, f, ensure_ascii=False)
    log(f"缓存就绪: {cache_path} ({len(ref_info)} 条序列)")
    return cache_path

def _fanse_sam_cmd(fanse_file, fasta_path, ref_info_json=None):
    """新增：统一构建 fanse sam 命令；提供缓存时追加 --ref-info-json 跳过 FASTA 解析"""
    cmd = ['fanse', 'sam', '-i', str(fanse_file), '-r', str(fasta_path)]
    if ref_info_json:
        cmd.extend(['--ref-info-json', str(ref_info_json)])
    # 修正：强制 -t 1（单线程流式输出）。
    # fanse sam 默认 -t 4 会启用 multiprocessing Pool 并按 20000 条/批 pickle 结果，
    # 对 multi-mapping 重的文件（实测 280k 条记录产出 6.5GB SAM，约 23KB/条）单批可达
    # 数百 MB，4 个 worker + 队列积压直接 MemoryError（见 *.bam_conv.log）。
    # 管道模式下下游 samtools view/sort 才是真正的并行主力，上游流式单线程最稳。
    cmd.extend(['-t', '1'])
    return cmd

def fanse2bam_unix(fanse_file, fasta_path, output_bam=None, sort=True, index=True, console=None):
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
    
    # 步骤1: 直接通过管道将fanse sam输出传递给samtools
    try:
        # 构建fanse sam命令
        fanse_cmd = ['fanse', 'sam', '-i', str(fanse_file), '-r', str(fasta_path)]
        
        # 构建samtools命令
        samtools_cmd = [bin_manager.get_samtools_path(), 'view', '-bS', '-']
        if sort:
            samtools_sort_cmd = [bin_manager.get_samtools_path(), 'sort', '-@ 4', '-o', str(output_bam)]
            samtools_index_cmd = [bin_manager.get_samtools_path(), 'index', str(output_bam)] if index else None
        else:
            samtools_cmd.extend(['-o', str(output_bam)])
            samtools_index_cmd = [bin_manager.get_samtools_path(), 'index', str(output_bam)] if index else None
        
        log(f"Converting {fanse_file} to BAM...")
        
        # 执行管道操作
        if sort:
            # fanse sam | samtools view -bS - | samtools sort -o output.bam
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(samtools_cmd, stdin=p1.stdout, stdout=subprocess.PIPE)
            p3 = subprocess.Popen(samtools_sort_cmd, stdin=p2.stdout)
            p1.stdout.close()
            p2.stdout.close()
            p3.communicate()
            
            if index and samtools_index_cmd:
                subprocess.run(samtools_index_cmd, check=True)
        else:
            # fanse sam | samtools view -bS - -o output.bam
            p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(samtools_cmd, stdin=p1.stdout)
            p1.stdout.close()
            p2.communicate()
            
            if index and samtools_index_cmd:
                subprocess.run(samtools_index_cmd, check=True)
        
        log(f"Successfully created BAM file: {output_bam}", style="bold green")
        return output_bam
        
    except subprocess.CalledProcessError as e:
        log(f"Error converting to BAM: {e.stderr.decode() if e.stderr else str(e)}", style="bold red")
        sys.exit(1)
    except Exception as e:
        log(f"Unexpected error: {str(e)}", style="bold red")
        sys.exit(1)

from .utils.path_utils import PathProcessor
from rich.console import Console

def bam_command(args):
    """处理bam子命令（精简版）"""
    console = Console(force_terminal=True)
    processor = PathProcessor()
    
    # 1. 解析输入文件
    try:
        input_files = processor.parse_input_paths(args.fanse_file, ['.fanse3', '.fanse'])
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
                        ref_info_json=ref_info_cache  # 新增：header缓存透传
                    )
                    return infile.name, None
                except Exception as e:
                    return infile.name, e

            ok_count = fail_count = 0
            try:
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
            finally:
                # 新增：批量结束删除临时 header 缓存
                if ref_info_cache:
                    try:
                        os.remove(ref_info_cache)
                    except OSError:
                        pass
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
                    ref_info_json=ref_info_cache  # 新增：header缓存透传
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
                ref_info_json=ref_info_cache  # 新增：header缓存透传
            )
        except Exception as e:
            console.print(f"[bold red]错误: {e}[/bold red]")
            sys.exit(1)
        finally:
            # 新增：转换结束删除临时 header 缓存（单文件模式）
            if ref_info_cache:
                try:
                    os.remove(ref_info_cache)
                except OSError:
                    pass

def fanse2bam_win_pipe(fanse_file, fasta_path, output_bam=None, sort=True, index=True, console=None, ref_info_json=None):
    #直接原位生成BAM文件，并进行排序索引
    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    # 新增：fanse sam 的 stderr 重定向到日志文件（方案A替代DEVNULL）
    # 转换期间屏幕静默时，可 tail 该日志查看 fanse sam 进度/警告，不再"假装卡死"
    conv_log = Path(output_bam).with_suffix('.bam_conv.log')
    conv_log_fp = None
    try:
        fanse_cmd = _fanse_sam_cmd(fanse_file, fasta_path, ref_info_json)
        samtools_path = bin_manager.get_samtools_path()
        log(f"Using samtools from: {samtools_path}")

        # 检测samtools版本是否为旧版(0.x)，旧版不支持 '-o'
        legacy = False
        try:
            ver_cmd = [samtools_path, '--version']
            ver_result = subprocess.run(ver_cmd, capture_output=True, text=True, check=True)
            legacy = ('samtools 0.' in ver_result.stdout) or ('samtools 0.' in ver_result.stderr)
            log(f"Samtools version check: legacy={legacy}")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            log(f"Samtools version detection failed ({e}). Assuming legacy for compatibility.")
            legacy = True  # If --version fails or samtools not found, assume legacy or problematic
        except Exception as e:
            log(f"Unexpected error during samtools version detection ({e}). Assuming legacy for compatibility.")
            legacy = True # Fallback for other unexpected errors

        samtools_view = [samtools_path, 'view', '-bS', '-']
        if sort and not legacy:
            samtools_sort = [samtools_path, 'sort', '-o', str(output_bam), '-']
        log(f"Converting {fanse_file} to BAM via pipe...")
        # 修正：fanse sam 的 stderr 原为 PIPE 时从未读取会导致缓冲区写满死锁，
        # 改为重定向到日志文件（方案A）：既消除死锁，又保留进度/警告供 tail 排查
        conv_log_fp = open(conv_log, 'w', encoding='utf-8')
        log(f"fanse sam 进度日志: {conv_log}")
        p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE, stderr=conv_log_fp)
        
        if sort and not legacy:
            # fanse sam | fanse samtools view -bS - | fanse samtools sort -o output.bam
            p2 = subprocess.Popen(samtools_view, stdin=p1.stdout, stdout=subprocess.PIPE)
            p3 = subprocess.Popen(samtools_sort, stdin=p2.stdout)
            p1.stdout.close()
            p2.stdout.close()
            rc = p3.wait()
            if rc != 0:
                raise RuntimeError('samtools sort failed')

        elif sort and legacy and  False:     
            # 旧版：写入临时BAM再排序（避免SAM中间文件），保证兼容性
            log("Using legacy samtools with temporary file for sorting...")
            temp_bam = Path(output_bam).with_suffix('.temp.bam')
            samtools_view_to_file = [samtools_path, 'view', '-bS', '-o', str(temp_bam), '-']
            
            p2 = subprocess.Popen(samtools_view_to_file, stdin=p1.stdout)
            p1.stdout.close()
            rc = p2.wait()
            if rc != 0:
                raise RuntimeError('samtools view failed')

            output_prefix = str(output_bam).replace('.bam', '')
            rc2 = subprocess.run([samtools_path, 'sort', str(temp_bam), output_prefix], capture_output=True, text=True)
            if rc2.returncode != 0:
                log(f"Samtools sort (legacy) failed with error: {rc2.stderr}", style="bold red")
                raise RuntimeError('samtools sort (legacy) failed')

            try:
                temp_bam.unlink()
            except Exception as e:
                log(f"Warning: Could not delete temporary file {temp_bam}: {e}", style="yellow")
        elif sort and legacy:  #基本都是走这条通道，其他的不走，因为装的就是旧版samtools，没有其他的版本，但是目前还算是够用吧
            # 通过管道连接排序，避免临时文件,直接输出排序后的bam文件，更直接，然后生成索引
            log("Using legacy samtools pipe for sorting (attempting direct output)...")
            output_prefix = str(output_bam).replace('.bam', '')
            # 旧版 samtools sort 的语法是 `sort <in.bam> <out.prefix>`
            # 从 stdin 读取时，使用 `sort - <out.prefix>`
            samtools_sort_legacy = [samtools_path, 'sort', '-', output_prefix]
            
            # 修正：p2 的 stderr=PIPE 从未被读取，同样存在缓冲区写满死锁隐患，改为 DEVNULL
            p2 = subprocess.Popen(samtools_view, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            p3 = subprocess.Popen(samtools_sort_legacy, stdin=p2.stdout, stderr=subprocess.PIPE)
            p1.stdout.close()
            p2.stdout.close()
            
            # 捕获并打印错误，以便更好地诊断
            stdout_p3, stderr_p3 = p3.communicate()
            if stdout_p3:
                log(f"Samtools sort (legacy) stdout: {stdout_p3.decode()}")
            if stderr_p3:
                log(f"Samtools sort (legacy) stderr: {stderr_p3.decode()}", style="red")

            if p3.returncode != 0:
                raise RuntimeError('samtools sort (legacy) failed')
      
        else:
            # 不排序：直接写入输出BAM
            samtools_view_nosort = [samtools_path, 'view', '-bS', '-', '-o', str(output_bam)]
            p2 = subprocess.Popen(samtools_view_nosort, stdin=p1.stdout)
            p1.stdout.close()
            rc = p2.wait()
            if rc != 0:
                raise RuntimeError('samtools view failed')

        if index:
            subprocess.run([samtools_path, 'index', str(output_bam)], check=True)
        if conv_log_fp:
            conv_log_fp.close()
            conv_log_fp = None
        log(f"Successfully created BAM file: {output_bam}", style="bold green")
        return output_bam
    except Exception as e:
        if conv_log_fp:
            conv_log_fp.close()
            conv_log_fp = None
        log(f"Pipe conversion failed: {e}", style="bold red")
        raise
    finally:
        # 修正：确保日志句柄在任何路径下都被关闭，避免并行模式下句柄泄漏
        if conv_log_fp is not None:
            try:
                conv_log_fp.close()
            except Exception:
                pass

def fanse2bam_win(fanse_file, fasta_path, output_bam=None, sort=True, index=True, keep_sam=False, console=None):
    """Windows专用版本（使用临时文件，避免管道问题）"""
    def log(msg, style=None):
        if console:
            console.print(msg, style=style)
        else:
            print(msg)

    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    
    temp_sam = Path(output_bam).with_suffix('.temp.sam')
    temp_bam = Path(output_bam).with_suffix('.temp.bam')
    
    try:
        # 生成临时SAM文件
        log(f"Creating temporary SAM file: {temp_sam}")
        subprocess.run(['fanse', 'sam', '-i', str(fanse_file), '-r', str(fasta_path), '-o', str(temp_sam)], 
                      check=True, capture_output=True, text=True)
        
        # 转换为BAM
        samtools_path = bin_manager.get_samtools_path()
        log(f"Using samtools from: {samtools_path}")
        
        # 先将SAM转换为BAM（临时文件）
        subprocess.run([samtools_path, 'view', '-bS', str(temp_sam), '-o', str(temp_bam)], check=True)
        
        if sort:
            # 排序BAM文件 - 使用输出前缀（不带.bam后缀）
            output_prefix = str(output_bam).replace('.bam', '')
            subprocess.run([samtools_path, 'sort', str(temp_bam), output_prefix], check=True)
            
            if index:
                subprocess.run([samtools_path, 'index', str(output_bam)], check=True)
        else:
            # 不排序，直接移动临时BAM到输出位置
            temp_bam.rename(output_bam)
            if index:
                subprocess.run([samtools_path, 'index', str(output_bam)], check=True)
                
        return output_bam
        
    except subprocess.CalledProcessError as e:
        log(f"Samtools error: {e.stderr.decode() if e.stderr else str(e)}", style="bold red")
        # 尝试使用备用方法
        return _fallback_conversion(fanse_file, fasta_path, output_bam, sort, index, console)
    except Exception as e:
        log(f"Unexpected error: {str(e)}", style="bold red")
        return _fallback_conversion(fanse_file, fasta_path, output_bam, sort, index, console)
    finally:
        files_to_clean = [temp_bam]
        if not keep_sam:
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

def fanse2bam(fanse_file, fasta_path, output_bam=None, sort=True, index=True, keep_sam=False, console=None, ref_info_json=None):
    """自动选择平台最优方法"""
    if os.name == 'nt':
        try:
            # 新增：ref_info_json 透传给管道转换，跳过每文件的 FASTA 全量解析
            return fanse2bam_win_pipe(fanse_file, fasta_path, output_bam, sort, index, console, ref_info_json)
        except Exception:
            #如果Windows版本失败，尝试使用传统方法
            return fanse2bam_win(fanse_file, fasta_path, output_bam, sort, index, keep_sam, console)
    else:  # Linux/Mac
        return fanse2bam_unix(fanse_file, fasta_path, output_bam, sort, index, console)



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
""")
