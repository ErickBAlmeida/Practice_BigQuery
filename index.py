import os

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from rich import print

load_dotenv("private/.env")

SUCCESS = ":white_check_mark:"
FAIL  = ":x:"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

class DataIngestor:
    def ingest_API(self):
        endpoint = os.getenv("API_ENDPOINT")
        if not endpoint:
            print(f"{FAIL} API_ENDPOINT não definido nas variáveis de ambiente.")
            return None

        try:
            response = requests.get(endpoint, timeout=(5, 1000))
        except requests.RequestException as e:
            print(f"{FAIL} Erro na requisição HTTP: {e}")
            return None

        if response.status_code != 200:
            print(f"{FAIL} API request failed with status code {response.status_code}")
            return None

        print(f"{SUCCESS} API request successful.")
        try:
            return response.json()
        except ValueError as e:
            print(f"{FAIL} Resposta não é JSON válido: {e}")
            return None

    def transform_json_to_df(self, response):
        if response is None:
            print(f"{FAIL} Nenhum dado para transformar em DataFrame.")
            return None

        try:
            data = response
            df = pd.DataFrame(data)
            if df.empty:
                print(f"{FAIL} DataFrame resultante está vazio.")
                return None

            df.columns = (
                df.columns
                .str.replace("[:@]", "_", regex=True)  # remove ':' e '@'
            )
            
            print(f"{SUCCESS} JSON transformado em DataFrame com sucesso.")
            return df

        except Exception as e:
            print(f"{FAIL} Error ao transformar JSON em DataFrame: {e}")
            return None


class DataLoader:
    def load_to_bq(self, df):
        if df is None:
            print(f"{FAIL} DataFrame inválido para carregamento.")
            return False

        project = os.getenv("PROJECT_ID")
        dataset = os.getenv("DATASET_ID")
        table = os.getenv("TABLE_ID")

        if not all([project, dataset, table]):
            print(f"{FAIL} PROJECT_ID, DATASET_ID ou TABLE_ID não definidos.")
            return False

        client = bigquery.Client(project=project)
        table_ref = f"{project}.{dataset}.{table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        try:
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
        except Exception as e:
            print(f"{FAIL} Erro ao carregar para BigQuery: {e}")
            return False

        print(f"{SUCCESS} Loaded {job.output_rows} rows into {table_ref}.")
        return True


if __name__ == "__main__":

    ingestor = DataIngestor()
    response = ingestor.ingest_API()

    if response:
        df = ingestor.transform_json_to_df(response)
        loader = DataLoader()
        if loader.load_to_bq(df):
            print(f"{SUCCESS} Tabela criada com sucesso!")
        else:
            print(f"{FAIL} Falha ao carregar a tabela.")
    else:
        print(f"{FAIL} Falha ao extrair dados da API.")