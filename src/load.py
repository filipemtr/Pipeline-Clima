# ET(L) process, loads the clean data in a new csv file in cleandata folder and LOAD it in a database.
from pandas import DataFrame
from sqlalchemy import create_engine

# Load function

def loaddata(dataframe: DataFrame):
    try:

        print('Carregando as informações no banco de dados . . .')

        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
        dataframe.to_sql(
            "dados_clima",
            engine,
            if_exists='append',
            index=False,
            method='multi',
        )

    except Exception as InvalidSQL:
        raise Exception(f'Método de conexão SQL inválido. Cheque os parâmetros da engine. {InvalidSQL}')
    
    finally:

        print('OK')
        print('FECHANDO . . .')
