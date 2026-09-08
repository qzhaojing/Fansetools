# fansetools/__init__.py
"""
Fansetools - FANSe3文件处理工具包
"""
try:
    from fansetools._version import __version__
except ImportError:
    __version__ = "1.2.1"
#__version__ = "1.0.3"
__author__ = "Zhao Jing"
__email__ = "qzhaojing@qq.com"
__github_repo__ = "qzhaojing/fansetools"  # 替换为您的GitHub仓库

import warnings
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass

from .cli import main
from .run import add_run_subparser
from .run import FanseRunner
from .sam import fanse2sam
from .bed import fanse2bed
from .fastx import fanse2fasta, fanse2fastq, unmap2fasta, unmap2fastq
from .count import FanseCounter
from .cluster import add_cluster_subparser # 新增：导入cluster子命令的解析器添加函数

__all__ = ['add_run_subparser', 'main', 'FanseRunner', 'fanse2sam', 'fanse2bed', 'fanse2fasta', 'fanse2fastq','unmap2fasta', 'unmap2fastq','FanseCounter', 'add_cluster_subparser', ] # 新增：将add_cluster_subparser添加到__all__
