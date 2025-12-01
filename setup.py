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
        "fallback_version": "1.0.0",     # Git无标签时的默认版本
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
    # install_requires=[
    #    'tqdm',   #进度条
    #    # 你的依赖项
    # ],
    install_requires=[
        'tqdm>=4.0.0',
        'colorama>=0.4.0; platform_system=="Windows"',  # Windows下推荐安装
        'pandas>=1.0.0',
        'biopython>=1.78',
        'packaging>=20.0',
        'requests>=2.20.0',
        # 'cutadapt',  # 如需 cutadapt 功能，请取消注释
    ],

    extras_require={
        'test': [
            # 'mock>=3.0.0',
            'pytest>=6.0.0',
        ],
        'full': [
            'numpy>=1.20.0',
            # 'pysam>=0.16.0',
        ]
    }




)
