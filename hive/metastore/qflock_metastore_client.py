from hive_metastore_client.builders import DatabaseBuilder
from hive_metastore_client.builders import TableBuilder
from hive_metastore_client.builders import StorageDescriptorBuilder

from thrift_files.libraries.thrift_hive_metastore_client.ttypes import Catalog
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import CreateCatalogRequest

from hive_metastore_client import HiveMetastoreClient

if __name__ == '__main__':
    # client = HiveMetastoreClient('dikehdfs', 9083).open()
    client = HiveMetastoreClient('localhost', 9090).open()

    db_name = 'database6_dca'
    catalog_name = 'spark6'

    cs = client.get_catalogs()
    print(cs)

    if catalog_name not in cs.names:
        catalog = Catalog(name=catalog_name, description='Description', locationUri='/opt/volume/metastore/metastore_db_DBA')
        client.create_catalog(CreateCatalogRequest(catalog))

    if db_name not in client.get_all_databases():
        print(f'Creating database {db_name}')
        database = DatabaseBuilder(name=db_name).build()
        client.create_database(database)
    else:
        print(f'Database {db_name} already exists')


    # storage_descriptor = StorageDescriptorBuilder()
    # table = TableBuilder(table_name='table1', db_name='database1_dca', storage_descriptor=storage_descriptor)

    d = client.get_all_databases()
    print(d)

    cs = client.get_catalogs()
    print(cs)

    c = client.get_database('database1_dca')
    print(c)

    client.close()


