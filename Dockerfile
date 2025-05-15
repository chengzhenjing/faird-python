# faird/Dockerfile

ARG PYTHON_VERSION=3.12-slim
ARG BASE_REGISTRY=docker.io/library/

FROM ${BASE_REGISTRY}python:${PYTHON_VERSION}

# 安装系统依赖（NetCDF C 库）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libnetcdf-dev \
        netcdf-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制整个项目目录
COPY . /app

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 暴露服务端口
EXPOSE 3101

# 启动命令
CMD ["python", "main.py"]


