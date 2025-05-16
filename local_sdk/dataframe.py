import pyarrow as pa
import pyarrow.compute as pc
from tabulate import tabulate

from core.models.dataframe import DataFrame
import os

class DataFrame(DataFrame):
    def __init__(self, id: str, data: pa.Table):
        self.id = id
        self.data = data.combine_chunks()
        self.schema = data.schema
        self.column_names = data.column_names
        self.columns = data.columns
        self.num_rows = data.num_rows
        self.num_columns = data.num_columns
        self.shape = data.shape
        self.nbytes = data.nbytes

    def __getitem__(self, index):
        """
        Args:
            index (int or str): Row index (int) or column name (str).
        Returns:
            result (dict or pyarrow.Array): Row data as a dictionary or column data as a PyArrow Array.
        """
        if isinstance(index, int):  # 行操作
            return {col: self.data[col][index].as_py() for col in self.data.column_names}
        elif isinstance(index, str):  # 列操作
            return self.data[index]
        else:
            raise TypeError("Index must be an integer (row) or string (column).")

    def __str__(self):
        """
        Returns:
            str: The string representation of the table.
        """
        return self.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False)

    def collect(self):
        return self

    def get_stream(self, max_chunksize=1000):
        """
        Args:
            max_chunksize (int, default None) – Maximum size for RecordBatch chunks.
        Returns:
            batches (list of RecordBatch)
        """
        return self.data.to_batches(max_chunksize)

    def limit(self, rowNum: int):
        """
        Args:
            rowNum (int): Number of rows to limit.
        Returns:
            DataFrame: A new DataFrame with the limited rows.
        """
        return DataFrame(self.id, self.data.slice(0, rowNum))

    def slice(self, offset=0, length=None):
        """
        Args:
            offset (int, default 0): Starting row index.
            length (int, default None): Number of rows to include.
        Returns:
            DataFrame: A new DataFrame with the sliced data.
        """
        new_data = self.data.slice(offset, length)
        return DataFrame(self.id, new_data)

    def select(self, *columns):
        """
        Args:
            columns (str): Column names to select.
        Returns:
            DataFrame: A new DataFrame with the selected columns.
        """
        return DataFrame(self.id, self.data.select(columns))

    def append_column(self, field_, column):
        """
        Args:
            field_ (pyarrow.Field): The field definition.
            column (pyarrow.Array): The column data.
        Returns:
            DataFrame: A new DataFrame with the appended column.
        """
        new_data = self.data.append_column(field_, column)
        return DataFrame(self.id, new_data)

    def filter(self, mask):
        """
        Args:
            mask (pyarrow.Array): Boolean mask for filtering rows.
        Returns:
            DataFrame: A new DataFrame with filtered rows.
        """
        new_data = self.data.filter(mask)
        return DataFrame(self.id, new_data)

    def sum(self, column):
        """
        Args:
            column (str): The name of the column.
        Returns:
            int/float: The sum of the column values.
        """
        return pc.sum(self.data[column]).as_py()

    def map(self, column, func, new_column_name=None):
        """
        Args:
            column (str): The name of the column.
            func (callable): The function to apply.
            new_column_name (str, optional): The name of the new column. If not provided, the original column will be replaced.

        Returns:
            DataFrame: A new DataFrame with the mapped column.
        """
        array = self.data[column]
        mapped_array = pa.array([func(value.as_py()) for value in array])
        if new_column_name:
            field_ = pa.field(new_column_name, mapped_array.type)
            return self.append_column(field_, mapped_array)
        else:
            field_ = pa.field(column, mapped_array.type)
            return self.drop_columns([column]).append_column(field_, mapped_array)

    def flatten(self):
        """
        Returns:
            DataFrame: A new DataFrame with flattened data.
        """
        new_data = self.data.flatten()
        return DataFrame(self.id, new_data)

    def to_pandas(self, **kwargs):
        """
        Args:
            **kwargs: Additional arguments for PyArrow to Pandas conversion.
        Returns:
            pandas.DataFrame: The converted Pandas DataFrame.
        """
        return self.data.to_pandas(**kwargs)

    def to_pydict(self):
        """
        Returns:
            dict: The table data as a dictionary.
        """
        return self.data.to_pydict()

    def to_string(self, head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False):
        """
        Args:
        Returns:
            str: The string representation of the table.
        """
        if display_all:
            return self.data.to_pandas().to_string()

        all_columns = self.data.column_names
        total_columns = len(all_columns)
        total_rows = self.data.num_rows

        # 确定要显示的列
        if total_columns <= (first_cols + last_cols):
            display_columns = all_columns
        else:
            display_columns = all_columns[:first_cols] + ['...'] + all_columns[-last_cols:]

        # 确保 head_rows 和 tail_rows 不超过总行数
        head_rows = min(head_rows, total_rows)
        tail_rows = min(tail_rows, total_rows)

        # 获取头部和尾部数据
        head_data = self.data.slice(0, head_rows)
        tail_data = self.data.slice(total_rows - tail_rows, tail_rows)

        # 准备表格数据
        table_data = []

        # 添加头部数据
        for i in range(head_rows):
            row = []
            for col in display_columns:
                if col == '...':
                    row.append('...')
                else:
                    col_index = all_columns.index(col)
                    row.append(str(head_data.column(col_index)[i].as_py()))
            table_data.append(row)

        # 添加省略行
        if total_rows > (head_rows + tail_rows):
            table_data.append(['...' for _ in display_columns])

        # 添加尾部数据
        for i in range(tail_rows):
            row = []
            for col in display_columns:
                if col == '...':
                    row.append('...')
                else:
                    col_index = all_columns.index(col)
                    row.append(str(tail_data.column(col_index)[i].as_py()))
            table_data.append(row)

        # 使用 tabulate 打印
        table_str = tabulate(table_data, headers=display_columns, tablefmt="plain")
        table_str += f"\n\n[{total_rows} rows x {total_columns} columns]"
        return table_str

    @staticmethod
    def from_pandas(pdf, schema=None):
        """从 pandas.DataFrame 转换为 Arrow Table"""
        data = pa.Table.from_pandas(pdf, schema=schema)
        return DataFrame(id="from_pandas", data=data)

    @staticmethod
    def from_pydict(mapping, schema=None, metadata=None):
        """从字典转换为 Arrow Table"""
        data = pa.Table.from_pydict(mapping, schema=schema, metadata=metadata)
        return DataFrame(id="from_pydict", data=data)

    def write(self, output_path: str, format: str = None):
        """
        将 DataFrame 写入文件，支持多种格式（netcdf, csv 等）

        Args:
            output_path (str): 输出文件路径。
            format (str, optional): 文件格式，如 'netcdf', 'csv'。默认自动根据扩展名推断。
        """
        if format is None:
            ext = os.path.splitext(output_path)[-1].lower()
            if ext in ('.nc', '.netcdf'):
                format = 'netcdf'
            elif ext in ('.csv',):
                format = 'csv'
            elif ext in ('.arrow', '.ipc'):
                format = 'arrow'
            else:
                raise ValueError(f"无法识别文件格式，请指定 format 参数，例如 'netcdf', 'csv'")

        if format == 'netcdf':
            from parser.nc_parser import NCParser
            NCParser().write(self.data, output_path)
        elif format == 'csv':
            import pyarrow.csv as csv
            csv.write_csv(self.data, output_path)
        elif format == 'arrow':
            with pa.OSFile(output_path, 'wb') as sink:
                with pa.ipc.new_file(sink, self.data.schema) as writer:
                    writer.write_table(self.data)
        else:
            supported = ['netcdf', 'csv', 'arrow']
            raise NotImplementedError(f"不支持的输出格式: {format}。当前支持: {', '.join(supported)}")


