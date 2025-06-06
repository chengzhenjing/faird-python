ARG PYTHON_VERSION=3.12-slim
ARG BASE_REGISTRY=docker.io/library/

FROM ${BASE_REGISTRY}python:${PYTHON_VERSION}

# 使用 Debian 12（bookworm）对应的阿里源并安装依赖和时区
RUN echo "deb http://mirrors.aliyun.com/debian bookworm main contrib non-free\n\
deb http://mirrors.aliyun.com/debian bookworm-updates main contrib non-free\n\
deb http://mirrors.aliyun.com/debian-security bookworm-security main contrib non-free" \
> /etc/apt/sources.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends --fix-missing \
        libnetcdf-dev \
        netcdf-bin \
        libexpat1 \
        libexpat1-dev \
        libgdal-dev \
        libproj-dev \
        libtiff-dev \
        libjpeg-dev \
        zlib1g-dev \
        tzdata && \
    ln -snf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制整个项目目录
COPY . /app

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 安装本地 SDK 模块
RUN pip install -e .

# 创建日志目录
RUN mkdir -p /var/log/faird

# 暴露服务端口
EXPOSE 3101

# 启动命令，并将输出写入日志文件
CMD python -u main.py > /var/log/faird/app.log 2>&1
