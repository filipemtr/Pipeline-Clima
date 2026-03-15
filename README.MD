## 🌦️ Climate ETL Pipeline

 Pipeline ETL automatizado desenvolvido em Python que consome dados climáticos de uma API, orquestrado automaticamente e executado a cada uma hora. Processa as informações e armazena os resultados em um banco de dados PostgreSQL visando futura análise!


- Visão Geral

Este projeto implementa um fluxo completo de Extração, Transformação e Carga (ETL):

# Extract
Consome dados climáticos da API de clima para uma localização definida no arquivo .env.

# Transform
Seleciona e trata apenas os dados relevantes, organizando-os em um formato estruturado.

# Load
Armazena os dados processados em um banco de dados PostgreSQL.

- Tech Stack

Python
Apache Airflow
PostgreSQL
Docker e Docker Compose
Pandas
Requests
SQLAlchemy


📂 Estrutura do Projeto
project
│
├── dags/
│   └── etl_pipeline.py
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── logs/
├── plugins/
├── docker-compose.yml
├── requirements.txt
└── .env


⚙️ Funcionalidades

✔ Consumo automático de API de clima
✔ Pipeline ETL modular em Python
✔ Execução automática a cada 1 hora
✔ Armazenamento em PostgreSQL
✔ Orquestração com Apache Airflow
✔ Containerização com Docker
✔ Tratamento de erros com envio de email em caso de falha


📊 Dados Armazenados

O pipeline salva informações como:

Cidade
Clima
Temperatura
Sensação térmica
Umidade
Temperatura mínima
Temperatura máxima
Velocidade do vento
Horário da consulta



🐳 Executando o Projeto

1️⃣ Clonar o repositório
git clone https://github.com/seu-usuario/seu-repo.git
cd seu-repo

2️⃣ Criar o arquivo .env seguindo o modelo do example.env

3️⃣ Subir os containers
docker compose up airflow-init
docker compose up 

4️⃣ Acessar o Airflow
http://localhost:8080

User: admin
Password: admin


Projeto desenvolvido para fins de estudo e prática em Engenharia de Dados.
⭐ Se este projeto te ajudou ou inspirou, considere dar uma estrela no repositório!
