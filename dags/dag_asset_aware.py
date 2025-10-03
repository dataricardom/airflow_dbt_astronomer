from airflow.sdk import Asset, dag, task
import os

asset_tabela = Asset(
    name=os.getenv("ASSET_NAME"), 
    uri=os.getenv("ASSET_URI"), 
    group='asset'

)


@dag(schedule=[asset_tabela])

def new_asset():
    @task
    def print_asset():
        print("Asset:", asset_tabela)
    print_asset()

new_asset()