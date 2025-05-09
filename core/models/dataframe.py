import pyarrow as pa
import pyarrow.compute as pc

class DataFrame:
    def __init__(self, id: str, data: pa.Table):
        self.id = id
        self.data = data
        self.schema = data.schema
        self.rowNums = data.num_rows

    def __getitem__(self, index):
        if isinstance(index, int):  # 行操作
            return {col: self.data[col][index].as_py() for col in self.data.column_names}
        elif isinstance(index, str):  # 列操作
            return self.data[index]
        else:
            raise TypeError("Index must be an integer (row) or string (column).")

    def get_stream(self):
        """返回数据流（模拟流式读取）"""
        return self.data.to_batches()

    def collect(self):
        """加载数据到本地（返回完整数据）"""
        return self.data

    def limit(self, rowNum: int):
        """限制返回的行数"""
        return DataFrame(self.id, self.data.slice(0, rowNum))

    def select(self, *fields):
        """选择特定列"""
        return DataFrame(self.id, self.data.select(fields))

    def filter(self, condition):
        """根据条件过滤数据"""
        filtered_data = self.data.filter(condition)
        return DataFrame(self.id, filtered_data)


