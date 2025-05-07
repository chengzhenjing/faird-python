

#### 配置开发环境
##### 1.安装miniconda

**下载Miniconda（Python3 版本，可参考[这里](https://blog.csdn.net/weixin_43651674/article/details/134880766)）**
```wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh```

**安装 Miniconda**
```bash Miniconda3-latest-Linux-x86_64.sh```

**配置conda国内源（最好用中科大的，用清华源安装最新的pyarrow==19.0.0找不到包）**
```conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/main/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/free/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/cloud/conda-forge/
conda config --set show_channel_urls yes
```

##### 2.创建python虚拟环境

**创建指令（pyarrow19.0.0可用版本为Python 3.9, 3.10, 3.11, 3.12 and 3.13.）**
```conda create --name py312 python=3.12```

**激活环境**
```conda activate py312```

**安装依赖**
```conda install requirements.txt```

