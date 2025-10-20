# Exercícios de BigQuery

### Descrição
- Script simples para extrair JSON de uma API, transformar em `pandas.DataFrame` e carregar em uma tabela do BigQuery.

### Fonte
- Dados públicos do serviço social da cidade de Nova York
```
"https://data.cityofnewyork.us/api/v3/views/erm2-nwe9/query.json"
```

### Pré-requisitos
- Python 3.8+
- Conta de serviço GCP com permissão para inserir dados no BigQuery

### Principais dependências
- pandas
- requests
- python-dotenv
- google-cloud-bigquery
- rich

### Instalação (recomenda-se virtualenv)

```powershell
python -m venv .venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Como executar (Windows)

```powershell
python index.py
```

### To Do
- Sanitizar nomes de colunas e tipos do DataFrame antes do upload ao BigQuery.
- Tratar respostas vazias da API de forma explícita (`if response is not None` em vez de `if response`).