from airflow.sdk import Asset, dag, task

asset_tabela = Asset(
    name='postgres://caboose.proxy.rlwy.net:43255/railway/public/mart_metricas_clientes',
    uri='postgres://caboose.proxy.rlwy.net:43255/railway/public/mart_metricas_clientes',
    group='asset'

)


@dag(schedule=[asset_tabela])

def new_asset():
    @task
    def print_asset():
        print("Asset:", asset_tabela)
    print_asset()

new_asset()