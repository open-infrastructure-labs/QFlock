from hive_metastore_client import HiveMetastoreClient
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import Catalog
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import CreateCatalogRequest

# Inspired by https://thrift.apache.org/tutorial/py.html


class MetastoreClient:

    def __init__(self, ip, port=9083):
        self.client = HiveMetastoreClient(ip, port).open()

    def __del__(self):
        # Close!
        self.client.close()

    def create_catalog(self, name, description="catalog description",
                       locationUri='/opt/volume/metastore/metastore_db_DBA'):
        catalog = Catalog(name=name, description=description,
                          locationUri=locationUri)
        self.client.create_catalog(CreateCatalogRequest(catalog))


if __name__ == "__main__":

    client = MetastoreClient("172.18.0.2")
    catalog_names = ['spark_dca2']

    catalogs = client.client.get_catalogs()
    print(catalogs)
    for catalog_name in catalog_names:
        if catalog_name not in catalogs.names:
            client.create_catalog(name=catalog_name, description='Spark Catalog for a Data Center',
                                  locationUri='/opt/volume/metastore/metastore_db_DBA')
