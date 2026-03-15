from pandas import DataFrame
from datetime import datetime
# E(T)L process, TRANSFORMS the data.

def transformdata(dataframe: DataFrame):

    print('Iniciando padronização de colunas . . .')

    # Step by step cleaning the data indexes
    print('Padronizando a coluna "cidade"')
    dataframe['cidade'] = (
        dataframe['cidade']
        .str.lower()
    )

    print('Padronizando a coluna "temperatura"')
    dataframe['temperatura'] = (
        dataframe['temperatura']
        .round(1)

    )

    print('Padronizando a coluna "sensacao_termica"')
    dataframe['sensacao_termica'] = (
        dataframe['sensacao_termica']
        .round(1)
    )

    print('Padronizando a coluna "temperatura_minima"')
    dataframe['temperatura_minima'] = (
        dataframe['temperatura_minima']
        .round(1)
    )

    print('Padronizando a coluna "temperatura_maxima"')
    dataframe['temperatura_maxima'] = (
        dataframe['temperatura_maxima']
        .round(1)
    )
    
    print('OK')
    print('*'*20)

    dataframe['hora_consulta'] = datetime.now().replace(second=0, microsecond=0)

    return dataframe
