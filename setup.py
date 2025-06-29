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
    install_requires=[
        tqdm,   #进度条
        # 你的依赖项
    ],
)