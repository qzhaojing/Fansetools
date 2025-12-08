# fansetools/bam.py
import os
import sys
import subprocess
from pathlib import Path
from .bin_utils import bin_manager
from .utils.rich_help import CustomHelpFormatter

def fanse2bam_unix(fanse_file, fasta_path, output_bam=None, sort=True, index=True):
    """
    将FANSe3文件直接转换为BAM格式（精简版）
    
    参数:
        fanse_file: 输入FANSe3文件路径
        fasta_path: 参考基因组FASTA文件路径
        output_bam: 输出BAM文件路径（可选）
        sort: 是否排序BAM文件
        index: 是否创建索引
        
        
    # 基本用法（自动排序和索引）
		fanse bam -i sample.fanse3 -r reference.fa -o sample.bam

		# 不排序
		fanse bam -i sample.fanse3 -r reference.fa --no-sort

		# 不创建索引
		fanse bam -i sample.fanse3 -r reference.fa --no-index

    """
    
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
        
        print(f"Converting {fanse_file} to BAM...")
        
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
        
        print(f"Successfully created BAM file: {output_bam}")
        return output_bam
        
    except subprocess.CalledProcessError as e:
        print(f"Error converting to BAM: {e.stderr.decode() if e.stderr else str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)

def bam_command(args):
    """处理bam子命令（精简版）"""
    output_file = fanse2bam(
        fanse_file=args.fanse_file,
        fasta_path=args.fasta_path,
        output_bam=args.output_bam,
        sort=not args.no_sort,
        index=not args.no_index,
        keep_sam=getattr(args, 'sam', False)
    )

def fanse2bam_win_pipe(fanse_file, fasta_path, output_bam=None, sort=True, index=True):
    #直接原位生成BAM文件，并进行排序索引
    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    try:
        fanse_cmd = ['fanse', 'sam', '-i', str(fanse_file), '-r', str(fasta_path)]
        samtools_path = bin_manager.get_samtools_path()
        print(f"Using samtools from: {samtools_path}")

        # 检测samtools版本是否为旧版(0.x)，旧版不支持 '-o'
        legacy = False
        try:
            ver_cmd = [samtools_path, '']
            ver_result = subprocess.run(ver_cmd, capture_output=True, text=True, check=True)
            legacy = ('samtools 0.' in ver_result.stdout) or ('samtools 0.' in ver_result.stderr)
            print(f"Samtools version check: legacy={legacy}")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"Samtools version detection failed ({e}). Assuming legacy for compatibility.")
            legacy = True  # If --version fails or samtools not found, assume legacy or problematic
        except Exception as e:
            print(f"Unexpected error during samtools version detection ({e}). Assuming legacy for compatibility.")
            legacy = True # Fallback for other unexpected errors

        samtools_view = [samtools_path, 'view', '-bS', '-']
        if sort and not legacy:
            samtools_sort = [samtools_path, 'sort', '-o', str(output_bam), '-']
        print(f"Converting {fanse_file} to BAM via pipe...")
        p1 = subprocess.Popen(fanse_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
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
            print("Using legacy samtools with temporary file for sorting...")
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
                print(f"Samtools sort (legacy) failed with error: {rc2.stderr}")
                raise RuntimeError('samtools sort (legacy) failed')

            try:
                temp_bam.unlink()
            except Exception as e:
                print(f"Warning: Could not delete temporary file {temp_bam}: {e}")
        elif sort and legacy:  #基本都是走这条通道，其他的不走，因为装的就是旧版samtools，没有其他的版本，但是目前还算是够用吧
            # 通过管道连接排序，避免临时文件,直接输出排序后的bam文件，更直接，然后生成索引
            print("Using legacy samtools pipe for sorting (attempting direct output)...")
            output_prefix = str(output_bam).replace('.bam', '')
            # 旧版 samtools sort 的语法是 `sort <in.bam> <out.prefix>`
            # 从 stdin 读取时，使用 `sort - <out.prefix>`
            samtools_sort_legacy = [samtools_path, 'sort', '-', output_prefix]
            
            p2 = subprocess.Popen(samtools_view, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            p3 = subprocess.Popen(samtools_sort_legacy, stdin=p2.stdout, stderr=subprocess.PIPE)
            p1.stdout.close()
            p2.stdout.close()
            
            # 捕获并打印错误，以便更好地诊断
            stdout_p3, stderr_p3 = p3.communicate()
            if stdout_p3:
                print(f"Samtools sort (legacy) stdout: {stdout_p3.decode()}")
            if stderr_p3:
                print(f"Samtools sort (legacy) stderr: {stderr_p3.decode()}")

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
        print(f"Successfully created BAM file: {output_bam}")
        return output_bam
    except Exception as e:
        print(f"Pipe conversion failed: {e}")
        raise

def fanse2bam_win(fanse_file, fasta_path, output_bam=None, sort=True, index=True, keep_sam=False):
    """Windows专用版本（使用临时文件，避免管道问题）"""
    if output_bam is None:
        output_bam = Path(fanse_file).with_suffix('.bam')
    
    temp_sam = Path(output_bam).with_suffix('.temp.sam')
    temp_bam = Path(output_bam).with_suffix('.temp.bam')
    
    try:
        # 生成临时SAM文件
        print(f"Creating temporary SAM file: {temp_sam}")
        subprocess.run(['fanse', 'sam', '-i', str(fanse_file), '-r', str(fasta_path), '-o', str(temp_sam)], 
                      check=True, capture_output=True, text=True)
        
        # 转换为BAM
        samtools_path = bin_manager.get_samtools_path()
        print(f"Using samtools from: {samtools_path}")
        
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
        print(f"Samtools error: {e.stderr.decode() if e.stderr else str(e)}")
        # 尝试使用备用方法
        return _fallback_conversion(fanse_file, fasta_path, output_bam, sort, index)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return _fallback_conversion(fanse_file, fasta_path, output_bam, sort, index)
    finally:
        files_to_clean = [temp_bam]
        if not keep_sam:
            files_to_clean.append(temp_sam)
        for temp_file in files_to_clean:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception as e:
                    print(f"Warning: Could not delete temporary file {temp_file}: {e}")

def _fallback_conversion(fanse_file, fasta_path, output_bam, sort, index):
    """备用转换方法"""
    print("Using fallback conversion method...")
    # 这里可以添加纯Python的SAM到BAM转换逻辑
    # 或者尝试其他方法
    
    raise RuntimeError("Fallback conversion not implemented. Please install a working version of samtools.")

def fanse2bam(fanse_file, fasta_path, output_bam=None, sort=True, index=True, keep_sam=False):
    """自动选择平台最优方法"""
    if os.name == 'nt':
        try:
            return fanse2bam_win_pipe(fanse_file, fasta_path, output_bam, sort, index)
        except Exception:
            #如果Windows版本失败，尝试使用传统方法
            return fanse2bam_win(fanse_file, fasta_path, output_bam, sort, index, keep_sam)
    else:  # Linux/Mac
        return fanse2bam_unix(fanse_file, fasta_path, output_bam, sort, index)



	# fansetools/cli.py
def add_bam_subparser(subparsers):
	"""添加精简版bam子命令解析器"""
	bam_parser = subparsers.add_parser(
		'bam',
		help='直接转换FANSe3文件为BAM格式',
		description='将FANSe3文件直接转换为BAM格式（自动排序和索引）',
		formatter_class=CustomHelpFormatter
	)
	
	bam_parser.add_argument(
		'-i', '--input', dest='fanse_file', required=True,
		help='输入FANSe3文件路径'
	)
	bam_parser.add_argument(
		'-r', '--fasta', dest='fasta_path', required=True,
		help='参考基因组FASTA文件路径'
	)
	bam_parser.add_argument(
		'-o', '--output', dest='output_bam',
		help='输出BAM文件路径（默认：输入文件同目录，同名.bam）'
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

	
	bam_parser.set_defaults(func=bam_command)
