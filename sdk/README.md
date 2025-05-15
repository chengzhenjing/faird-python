# sdk使用方式

## 1. 安装

```bash
pip install faird
```

## 2. 使用
### 2.1 引入依赖
```python
from sdk import DataFrame, DacpClient, Principal
```
### 2.2 连接faird服务
```python
conn = DacpClient.connect("dacp://{host}:{port}", Principal.oauth("conet", "{username}", "{password}"))
```

### 2.3 获取数据目录
```python
datasets = conn.list_datasets()
dataframe_ids = conn.list_dataframes(datasets[0].get('name'))
```

### 2.4 打开dataframe
```python
df = conn.open(dataframe_ids[0])
```

### 2.5 操作dataframe
```python
"""
1. compute remotely
"""
print(df.schema)
print(df.num_rows)
print(df)
print(df.limit(5).select("OBJECTID", "start_p", "end_p"))

"""
2. compute locally
"""
print(df.collect().limit(3).select("from_node"))

"""
3. compute remote & local
"""
print(df.limit(3).collect().select("OBJECTID", "start_p", "end_p"))


"""
4. streaming
"""
for chunk in df.get_stream(): # 默认1000行
    print(chunk)
    print(f"Chunk size: {chunk.num_rows}")

for chunk in df.get_stream(max_chunksize=100):
    print(chunk)
    print(f"Chunk size: {chunk.num_rows}")
```

