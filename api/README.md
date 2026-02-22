# Udemy Business ETL Pipeline 🚀

---

## 🇺🇸 English Version (US)

Data Engineering pipeline designed to extract and process Udemy Business metrics using a **Medallion Architecture** and **Snowflake** data modeling.

### 🏗️ Data Architecture
The system organizes data into three maturity layers:
1.  **Bronze (Raw):** Exhaustive extraction via API (REST) supporting Cursor pagination and a 7-day granular cache.
2.  **Silver (Cleansed):** Snowflake normalization, attribute translation to PT-BR, and compliance with data privacy (email anonymization via SHA-256).
3.  **Gold (Curated):** Generation of localized CSVs and a consolidated Excel (.xlsx) report for Data Analysts.



### 🛠️ Tech Stack
- **Language:** Python 3.13.3
- **Environment:** Poetry
- **Processing:** Pandas & Openpyxl
- **Security:** Hashlib (SHA-256) for data pseudonymization.

### 🚀 Usage
1.  **Setup:** `poetry install`
2.  **Credentials:** Create a `credencial.json` file in the root directory (protected by `.gitignore`).
    ```json
    [
      {
        "ACCOUNT_NAME": "your-org-name",
        "ACCOUNT_ID": 123456,
        "rest_client_id": "abc123clientid",
        "rest_client_secret": "xyz789clientsecret"
      }
    ]
    ```
3.  **Execute:** `poetry run python api_extractor.py`

### 🛡️ Privacy & Compliance
This project implements *Privacy by Design*. Sensitive data (emails) are converted into irreversible hashes in the Silver layer, enabling statistical analysis without exposing personal identities.

---

## 🇧🇷 Versão em Português (PT-BR)

Pipeline de Engenharia de Dados para extração e processamento de métricas da Udemy Business, utilizando uma **Arquitetura Medalhão** e modelagem **Snowflake**.

### 🏗️ Arquitetura de Dados
O sistema organiza os dados em três camadas de maturidade:
1.  **Bronze (Raw):** Extração exaustiva via API (REST) com suporte a paginação por Cursor e cache granular de 7 dias.
2.  **Silver (Cleansed):** Normalização Snowflake, tradução de atributos para PT-BR e conformidade com a LGPD (anonimização de e-mails via SHA-256).
3.  **Gold (Curated):** Geração de CSVs localizados e um relatório consolidado em Excel (.xlsx) para Analistas de Dados.



### 🛠️ Tecnologias
- **Linguagem:** Python 3.13.3
- **Ambiente:** Poetry
- **Processamento:** Pandas & Openpyxl
- **Segurança:** Hashlib (SHA-256) para pseudonimização.

### 🚀 Execução
1.  **Instalação:** `poetry install`
2.  **Credenciais:** Criar o arquivo `credencial.json` na pasta raiz (protegido pelo `.gitignore`).
    ```json
    [
      {
        "ACCOUNT_NAME": "nome-da-organizacao",
        "ACCOUNT_ID": 123456,
        "rest_client_id": "abc123clientid",
        "rest_client_secret": "xyz789clientsecret"
      }
    ]
    ```
3.  **Rodar:** `poetry run python api_extractor.py`

### 🛡️ Privacidade e LGPD
Este projeto segue o princípio de *Privacy by Design*. Dados sensíveis (e-mails) são convertidos em hashes irreversíveis na camada Silver, permitindo análises estatísticas sem expor a identidade dos alunos.

---

## 📊 Output Structure / Estrutura de Saída (Gold)

| Tab Name / Nome da Aba | Description / Descrição |
| :--- | :--- |
| **Alunos** | Anonymized student IDs and full names / IDs anonimizados e nomes completos. |
| **Cursos** | Translated catalog and metrics / Catálogo e métricas traduzidas. |
| **Progresso_Cursos** | Engagement facts and access dates / Fatos de engajamento e datas. |
| **Niveis_Dificuldade** | Normalized difficulty levels / Níveis de dificuldade normalizados. |
| **Categorias** | Business categories in PT-BR / Categorias de negócio em PT-BR. |