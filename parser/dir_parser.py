import json
import pyarrow as pa
import pyarrow.ipc as ipc
import os
import uuid
from parser.abstract_parser import BaseParser
from services.datasource.services.metacat_mongo_service import MetaCatMongoService
from core.config import FairdConfigManager
from services.datasource.services.metacat_service import MetaCatService


class DirParser(BaseParser):

    def parse_dir(self, file_path: str, dataset_name: str) -> pa.Table:
        # Ensure the cache directory exists
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/dir/")
        os.makedirs(os.path.dirname(DEFAULT_ARROW_CACHE_PATH), exist_ok=True)

        arrow_file_name = str(uuid.uuid4()) + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # read from mongodb to get all file
        data_source_service = None;
        if FairdConfigManager.get_config().use_mongo == "true":
            data_source_service = MetaCatMongoService()
        else:
            data_source_service = MetaCatService()

        files_data = []
        all_files = data_source_service.list_dataframes_all_files(dataset_name)
        for file_dict in all_files:
            if file_dict["type"] == "dir":
                continue
            files_data.append({
                "name": file_dict["name"],
                "path": file_dict["path"],
                "suffix": file_dict["suffix"],
                "type": file_dict["type"],
                "size": file_dict["size"]
            })

        # 定义 Arrow 表的 schema
        schema = pa.schema([
            ("name", pa.string()),
            ("path", pa.string()),
            ("suffix", pa.string()),
            ("type", pa.string()),
            ("size", pa.int64())
        ])
        table = pa.Table.from_pydict({key: [file[key] for file in files_data] for key in schema.names}, schema=schema)

        # Save the table as a .arrow file
        with ipc.new_file(arrow_file_path, table.schema) as writer:
            writer.write_table(table)

        # Load the .arrow file into a pyarrow Table using zero-copy
        with pa.memory_map(arrow_file_path, "r") as source:
            return ipc.open_file(source).read_all()

    def parse(self, file_path: str) -> pa.Table:
        return None

    def write(self, table: pa.Table, output_path: str):
        raise NotImplementedError("DirParser.write() 尚未实现：当前不支持写回 Dir 文件")