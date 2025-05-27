import os

from core.config import FairdConfig, FairdConfigManager
from pymongo import MongoClient

if __name__ == "__main__":

    FairdConfigManager.load_config("/Users/yaxuan/rdcn_gitlab/faird/faird.conf")
    config = FairdConfigManager.get_config()
    mongo_client = MongoClient(config.mongo_db_url)
    dataset_file_collection = mongo_client['metacat']['dataset_file_2025']
    dataset_id = "1924752110897905664"
    root_path = config.storage_local_path
    cursor = dataset_file_collection.find({"datasetId": dataset_id})
    dataframes = []
    for file in cursor:
        df = {}
        df['id'] = file['_id']
        df['datasetId'] = file['datasetId']
        df['fId'] = file['fId']
        df['name'] = file['name']
        df['path'] = file['path']
        if file['path'].startswith(root_path):
            df['path'] = "/" + os.path.relpath(file['path'], root_path)
        df['size'] = file['size']
        df['suffix'] = file['suffix']
        df['type'] = file['type']
        df['dataframeName'] = f"dataset_name{df['path']}"
        dataframes.append(df)
    print("aaa")