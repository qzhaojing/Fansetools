# fansetools/__init__.py
"""
Fansetools - FANSe3文件处理工具包
"""
__version__ = "1.0.1"
__author__ = "Zhao Jing"
__email__ = "qzhaojing@qq.com"
__github_repo__ = "qzhaojing/fansetools"  # 替换为您的GitHub仓库


from .cli import main
from .run import add_run_subparser
from .run import FanseRunner
from .sam import fanse2sam
from .bed import fanse2bed
from .fastx import fanse2fasta, fanse2fastq, unmap2fasta, unmap2fastq
from .count import FanseCounter

# 版本检查
#try:
#    from .utils.version_check import check_fansetools_version
#     在导入时自动检查版本
#    check_fansetools_version()
#except ImportError:
#     如果版本检查模块不可用，静默跳过
#    pass

__all__ = ['add_run_subparser', 'main', 'FanseRunner', 'fanse2sam', 'fanse2bed', 'fanse2fasta', 'fanse2fastq','unmap2fasta', 'unmap2fastq','FanseCounter', ]