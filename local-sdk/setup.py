from setuptools import setup, find_packages

setup(
    name="local-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyarrow>=12.0.0"
    ],
    entry_points={
        "console_scripts": []
    },
    description="A local SDK for working with DataFrame",
    author="Your Name",
    author_email="your_email@example.com",
    url="https://github.com/Hulululu910/local-sdk",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)