from setuptools import setup, find_packages

setup(
    name="faird",
    version="0.1.0",
    description="A local SDK for working with DataFrame",
    author="yxzhang",
    author_email="yxzhang@cnic.com",
    packages=find_packages(include=["core*", "parser", "local-sdk*"]),  # 自动发现包含的所有包
    install_requires=[
        "pyarrow==19.0.0",  # 指定依赖的 pyarrow 版本
    ],
    entry_points={
        "console_scripts": [
            # 可选：定义命令行工具
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",  # 指定支持的 Python 版本
)