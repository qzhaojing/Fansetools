from setuptools import setup, find_packages
import os
import sys
package_data = {}
if sys.platform == 'win32':
    package_data['fansetools'] = ['bin/windows/*']


setup(
    name='fansetools',
    use_scm_version={
        "root": ".",
        "relative_to": __file__,
        "write_to": "src/fansetools/_version.py",  # 自动生成版本文件
        "version_scheme": "post-release",  # 版本生成策略
        #"local_scheme": "dirty-tag",      # 本地修改标记
        "local_scheme": "no-local-version",  # 这行很重要，避免 +dirty 后缀
        "write_to_template": '__version__ = "{version}"',  # 自定义文件格式
        "fallback_version": "1.2.1",     # Git无标签时的默认版本
    },
    
    setup_requires=["setuptools_scm"],
    #version='v1.0.2',
    package_dir={"": "src"},  # 指定包根目录为src
    packages=find_packages(where="src"),
    
    entry_points={
        'console_scripts': [
            'fanse=fansetools.cli:main',
        ],
    },
    package_data={
    'fansetools': [
        'bin/windows/*.exe', 
        'bin/windows/*.txt',
        'bin/windows/*.pl'
    ]
    },
    include_package_data=True,
    install_requires=[
        'tqdm>=4.0.0',
        'colorama>=0.4.0; platform_system=="Windows"',  # Windows下推荐安装
        'pandas>=1.0.0',
        'packaging>=20.0',
        'requests>=2.20.0',
        'paramiko',
        'rich>=13.0.0',   # 修正(2026-09-14): sam/bam/cli/count 等直接 import rich，
                          # 原先仅靠 rich_argparse 传递安装，依赖声明不完整
        'rich_argparse',
        'psutil>=5.0.0',  # 修正(2026-09-14): run.py 内存缓存检测使用（缺失时优雅降级），
                          # 补入默认依赖以保证内存检查功能可用
        # 修正(2026-09-14): 移除 'biopython>=1.78' —— 全源码递归扫描确认从未
        # import Bio，属历史遗留无用依赖（wheel 体积大，白装）
        # pysam: 仅 bam_linux.py 使用且该模块未被其他代码引用，改为可选
        # （Linux 用户需要时手动 pip install pysam），见 requirements.txt
    ],

    extras_require={
        'test': [
            'pytest>=6.0.0',
        ],
        'full': [
            'numpy>=1.20.0',
        ]
    }
)
