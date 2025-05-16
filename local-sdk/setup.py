from setuptools import setup, find_packages

setup(
    name="faird-local",
    version="1.0.3",
    description="A local SDK for working with DataFrame",
    author="rdcn",
    author_email="rdcn@cnic.com",
    packages=find_packages(include=["core*", "parser", "local-sdk*"]),  # 自动发现包含的所有包
    install_requires=[
        "pyarrow==19.0.0",  # 指定依赖的 pyarrow 版本
        "tabulate",
        "pandas",
        "netCDF4",
        "rasterio",
    ],
    python_requires=">=3.9",
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
)