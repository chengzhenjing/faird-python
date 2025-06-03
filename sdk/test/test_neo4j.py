import os

from neo4j import GraphDatabase

from core.config import FairdConfig, FairdConfigManager
from pymongo import MongoClient

if __name__ == "__main__":

    # FairdConfigManager.load_config("/Users/yaxuan/rdcn_gitlab/faird/faird.conf")
    # config = FairdConfigManager.get_config()
    # mongo_client = MongoClient(config.mongo_db_url)
    # dataset_file_collection = mongo_client['metacat']['dataset_file_2025']
    # dataset_id = "1924752110897905664"
    # root_path = config.storage_local_path
    # cursor = dataset_file_collection.find({"datasetId": dataset_id})
    # dataframes = []
    # for file in cursor:
    #     df = {}
    #     df['id'] = file['_id']
    #     df['datasetId'] = file['datasetId']
    #     df['fId'] = file['fId']
    #     df['name'] = file['name']
    #     df['path'] = file['path']
    #     if file['path'].startswith(root_path):
    #         df['path'] = "/" + os.path.relpath(file['path'], root_path)
    #     df['size'] = file['size']
    #     df['suffix'] = file['suffix']
    #     df['type'] = file['type']
    #     df['dataframeName'] = f"dataset_name{df['path']}"
    #     dataframes.append(df)

    # FairdConfigManager.load_config("/Users/yaxuan/rdcn_gitlab/faird/faird.conf")
    # config = FairdConfigManager.get_config()
    # neo4j_driver = GraphDatabase.driver(
    #     config.neo4j_url,
    #     auth=(config.neo4j_user, config.neo4j_password)
    # )
    # dataset_id = "1924752110897905664"
    # query = ('MATCH (n:DatasetFile{datasetId:"' + dataset_id +'"}) where n.isFile=true or n.type ="dir" '
    #          'RETURN n.datasetId as datasetId,n.name as name,n.suffix as suffix,n.type as type,n.path as path,n.size as size,n.time as time')
    # #query = 'MATCH (n:DatasetFile{datasetId:"1924752110897905664"}) where n.isFile=true or n.type ="dir" RETURN n.datasetId as datasetId,n.name as name,n.suffix as suffix,n.type as type,n.path as path,n.size as size,n.time as time'
    # dataframes = []
    # with neo4j_driver.session() as session:
    #     root_path = "/sharedata/dataset"
    #     result = session.run(query)
    #     for record in result:
    #         df = {
    #             # 'id': record.get('id'),
    #             'datasetId': record.get('datasetId'),
    #             'name': record.get('name'),
    #             'path': record.get('path'),
    #             'size': record.get('size'),
    #             'suffix': record.get('suffix'),
    #             'type': record.get('type'),
    #             'time': record.get('time'),
    #             'dataframeName': f"dataset_name{record.get('path')}"
    #         }
    #         if df['path'].startswith(root_path):
    #             df['path'] = "/" + os.path.relpath(df['path'], root_path)
    #         dataframes.append(df)
    #     print(dataframes)


    ## list all files
    FairdConfigManager.load_config("/Users/yaxuan/rdcn_gitlab/faird/faird.conf")
    config = FairdConfigManager.get_config()
    neo4j_driver = GraphDatabase.driver(
        config.neo4j_url,
        auth=(config.neo4j_user, config.neo4j_password)
    )
    dataset_id = "1924752110897905664"
    query = ('MATCH (n:DatasetFile{datasetId:"' + dataset_id + '"}) where n.isFile=true '
             'RETURN n.datasetId as datasetId,n.name as name,n.suffix as suffix,n.type as type,n.path as path,n.size as size,n.time as time')
    # query = 'MATCH (n:DatasetFile{datasetId:"1924752110897905664"}) where n.isFile=true or n.type ="dir" RETURN n.datasetId as datasetId,n.name as name,n.suffix as suffix,n.type as type,n.path as path,n.size as size,n.time as time'
    dataframes = []
    with neo4j_driver.session() as session:
        root_path = "/sharedata/dataset"
        result = session.run(query)
        for record in result:
            df = {
                # 'id': record.get('id'),
                'datasetId': record.get('datasetId'),
                'name': record.get('name'),
                'path': record.get('path'),
                'size': record.get('size'),
                'suffix': record.get('suffix'),
                'type': record.get('type'),
                'time': record.get('time'),
                'dataframeName': f"dataset_name{record.get('path')}"
            }
            if df['path'].startswith(root_path):
                df['path'] = "/" + os.path.relpath(df['path'], root_path)
            dataframes.append(df)
        print(dataframes)