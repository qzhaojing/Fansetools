from setuptools import setup, find_packages

setup(
    name='fansetools',
    version='0.1',
    package_dir={"": "src"},  # 关键：指定包根目录为src
    packages=find_packages(where="src"),
    entry_points={
        'console_scripts': [
            'fanse=fansetools.cli:main',
        ],
    },
    install_requires=[
        # 你的依赖项
    ],
)