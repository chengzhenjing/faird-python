import pyarrow as pa
import pyarrow.compute as pc

class DataFrame:
    def __init__(self, id: str, data: pa.Table):
        self.id = id
        self.data = data
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
        return self.to_string(preview_cols=5)

    def collect(self):
        """
        Returns:
            data (pyarrow.Table): The complete data as a PyArrow Table.
        """
        return self.data

    def get_stream(self, max_chunksize=None):
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

    def add_column(self, i: int, field_, column):
        """
        Args:
            i (int): Position to insert the column.
            field_ (pyarrow.Field): The field definition.
            column (pyarrow.Array): The column data.
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        new_data = self.data.add_column(i, field_, column)
        return DataFrame(self.id, new_data)

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

    def drop_columns(self, columns):
        """
        Args:
            columns (list or str): Column names to drop.
        Returns:
            DataFrame: A new DataFrame with the specified columns removed.
        """
        new_data = self.data.drop(columns)
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

    def to_string(self, preview_cols=0):
        """
        Args:
            preview_cols (int, default 0): Number of columns to preview in the string representation.
        Returns:
            str: The string representation of the table.
        """
        return self.data.to_string(preview_cols=preview_cols)

    @staticmethod
    def from_pandas(df, schema=None):
        """从 pandas.DataFrame 转换为 Arrow Table"""
        data = pa.Table.from_pandas(df, schema=schema)
        return DataFrame(id="from_pandas", data=data)

    @staticmethod
    def from_pydict(mapping, schema=None, metadata=None):
        """从字典转换为 Arrow Table"""
        data = pa.Table.from_pydict(mapping, schema=schema, metadata=metadata)
        return DataFrame(id="from_pydict", data=data)