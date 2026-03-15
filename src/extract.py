import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# (E) TL process, extracts the data from the csv file and return it into a dataframe.

def extractdata():  

    print('Consumindo API e recebendo seus dados climáticos . . .')
    print('*'*20)

    # Receives the .env variables
    LAT = os.getenv('LAT')
    LON = os.getenv('LON')
    API_KEY = os.getenv('API_KEY')

    # HTTP request that receives the API data 
    request = requests.get(
        f'https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&lang=pt_br&units=metric'
    )

    # Verifying the status code for error checking
    if request.status_code != 200:
        raise Exception(f"Erro retornado pela API: {request.status_code}")

    # Transforms API data (as a json) in a Python object
    incompletejson = request.json()
    print(incompletejson)
    # Select fields to make a dict with useful columns
    jsondata = {
        "cidade": incompletejson['name'],
        "clima": incompletejson["weather"][0]["description"],
        "temperatura": incompletejson["main"]["temp"],
        "umidade": incompletejson["main"]["humidity"],
        "sensacao_termica": incompletejson["main"]["feels_like"],
        "temperatura_minima": incompletejson["main"]["temp_min"],
        "temperatura_maxima": incompletejson["main"]["temp_max"],
        "velocidade_vento": incompletejson["wind"]["speed"],
        "hora_consulta": 0
    }
    
    # Read the python object in a dataframe
    dataframe = pd.DataFrame([jsondata])

    return dataframe

if __name__ == '__main__':
    extractdata()