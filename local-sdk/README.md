# Local SDK

## 简介
`faird` 是一个用于处理 DataFrame 的本地 SDK，提供便捷的接口来操作数据。

## 安装
在项目根目录下运行以下命令安装 `faird` SDK：
```bash
pip install -e local-sdk
```
查看安装信息
```bash
pip show faird
```

## 使用
1. 引入模块
在代码中引入 faird 模块：
```python
from faird import open
```

2. 打开 DataFrame
使用 open 方法打开指定的 DataFrame.
```python
from faird import open

dataframe = open("dataframe_id")
print(dataframe)
```
