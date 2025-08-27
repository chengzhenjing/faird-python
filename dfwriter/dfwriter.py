from dfwriter.csv_writer import CSVWriter
from dfwriter.default_writer import DefaultWriter
from dfwriter.nc_writer import NCWriter
from dfwriter.tif_writer import TIFWriter


class DfWriter:
    # 1. 创建一个类级别的注册表，用于存储格式名称和处理类的映射
    _format_registry = {}

    def __init__(self):
        self.output_target = None
        self.output_format = "arrow"  # 默认格式

        # 2. 在初始化时注册内置/默认支持的格式
        self.register_default_formats()

    @classmethod
    def register_format(cls, format_name: str, writer_class):
        """
        插件注册方法：向 DfWriter 注册一个新的输出格式。
        这是一个类方法,例如 DfWriter.register_format(...)
        """
        cls._format_registry[format_name.lower()] = writer_class
        print(f"✅ Format '{format_name.lower()}' registered with writer {writer_class.__name__}")

    @classmethod
    def register_default_formats(cls):
        """一个辅助方法，用于注册所有内置格式，避免重复代码"""
        if not cls._format_registry:  # 只在第一次初始化时注册
            cls.register_format("csv", CSVWriter)
            cls.register_format("nc", NCWriter)
            cls.register_format("tif", TIFWriter)
            cls.register_format("tiff", TIFWriter)
            cls.register_format("arrow", DefaultWriter)

    def output(self, target):
        self.output_target = target
        return self

    def format(self, fmt):
        self.output_format = fmt.lower()
        return self

    def write(self, df):
        # 3. 从类级别的注册表中查找写入器
        writer_class = self._format_registry.get(self.output_format)

        if not writer_class:
            supported_formats = ", ".join(self._format_registry.keys())
            raise ValueError(
                f"Unsupported format: '{self.output_format}'. "
                f"Supported formats are: [{supported_formats}]"
            )

        # 实例化并使用写入器
        writer_instance = writer_class()

        # 先执行 collect() 确保数据在本地
        df.collect()
        arrow_table = df.data

        writer_instance.write(table=arrow_table, output_path=self.output_target)
        print(f"🚀 Successfully wrote data to '{self.output_target}' in '{self.output_format}' format.")