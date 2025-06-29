from setuptools import setup, find_packages

setup(
    name='fansetools',
    version='v1.0.0',
    package_dir={"": "src"},  # 指定包根目录为src
    packages=find_packages(where="src"),
    entry_points={
        'console_scripts': [
            'fanse=fansetools.cli:main',
        ],
    },
    #install_requires=[
    #    'tqdm',   #进度条
    #    # 你的依赖项
    #],
    install_requires = [
        'tqdm>=4.0.0',
        'colorama>=0.4.0; platform_system=="Windows"',  # Windows下推荐安装
    ]

    extras_require = {
        'test': [
            #'mock>=3.0.0',
            'pytest>=6.0.0',
        ],
        'full': [
            'numpy>=1.20.0',
            #'pysam>=0.16.0',
        ]
}
    
    
    
    
)