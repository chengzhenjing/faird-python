faird重构版本(2025-05-07)

## 环境配置
### 1.安装conda

**1.1 下载Miniconda（Python3 版本，可参考[这里](https://blog.csdn.net/weixin_43651674/article/details/134880766)）**

```wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh```

**1.2 安装 Miniconda**

```bash Miniconda3-latest-Linux-x86_64.sh```

**1.3 配置conda国内源（最好用中科大的）**

```conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/main/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/free/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/cloud/conda-forge/
conda config --set show_channel_urls yes
```

### 2.创建python虚拟环境

**2.1 创建指令（pyarrow19.0.0可用版本为Python 3.9, 3.10, 3.11, 3.12 and 3.13.）**

```conda create --name py312 python=3.12.0```

**2.2 激活环境**

```conda activate py312```

**2.3 安装依赖**

```conda install --file requirements.txt```


### 3.启动服务

```python app/main.py```