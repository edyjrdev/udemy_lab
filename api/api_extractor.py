import requests
import pandas as pd
import json
import os
import time
import shutil
import logging
import csv
import re
import hashlib
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO DE LOG E TELEMETRIA ---
LOG_DATE = datetime.now().strftime('%Y%m%d')
LOG_FILE = f"{LOG_DATE}_api.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE ROBUSTEZ ---
MAX_TEXT_LENGTH = 32000
DEFAULT_ENCODING = "utf-8"
OUTPUT_ENCODING = "utf-8-sig"
CACHE_EXPIRATION_DAYS = 7
API_TIMEOUT = 30

# --- ESTRUTURA DE PASTAS ---
BRONZE_DIR = "01_bronze"
SILVER_DIR = "02_silver"
GOLD_DIR = "03_gold"

COURSE_PAGE_DIR = f"{BRONZE_DIR}/00_course/00_pages"
ACTIVITY_PAGE_DIR = f"{BRONZE_DIR}/01_activity/00_pages"
COURSE_ACT_PAGE_DIR = f"{BRONZE_DIR}/02_user_course_activity/00_pages"

# --- MAPAS DE TRADUÇÃO E NOMES ---
TABLE_NAME_MAP = {
    "stg_students": "Alunos", "stg_courses": "Cursos", "stg_course_progress": "Progresso_Cursos",
    "stg_activity_items": "Atividades_Detalhadas", "stg_languages": "Idiomas_Detalhes",
    "stg_levels": "Niveis_Dificuldade", "stg_categories": "Categorias"
}

COLUMN_TRANSLATION_MAP = {
    "student_hash": "ID_Anonimizado_Aluno", "full_name_masked": "Nome_Camuflado",
    "course_id": "ID_Curso", "title_original": "Titulo_Original", "title_pt_br": "Titulo_Traduzido",
    "duracao_horas": "Carga_Horaria", "completion_ratio": "Percentual_Conclusao"
}

def setup_directories():
    folders = [COURSE_PAGE_DIR, ACTIVITY_PAGE_DIR, COURSE_ACT_PAGE_DIR, SILVER_DIR, GOLD_DIR]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

def clean_for_excel(text):
    if not isinstance(text, str): return text
    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
    return ILLEGAL_CHARACTERS_RE.sub("", text)

def anonymize_email(email):
    if not email: return "anon_unknown"
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()

class UdemyTransformer:
    """ETAPA 2: SILVER - Implementação de Máscara de Identidade (Aluno 01, 02...)."""
    def run(self):
        logger.info("🔄 [SILVER] Iniciando camuflagem de identidades e normalização...")
        
        with open(f"{BRONZE_DIR}/user_course_progress_consolidated.json", 'r', encoding=DEFAULT_ENCODING) as f:
            course_act = json.load(f)

        d_students = []
        # Dicionário temporário para garantir que o mesmo e-mail receba sempre o mesmo "Aluno XX"
        student_mask_map = {} 
        mask_counter = 1

        for act in course_act:
            email = act.get('user_email')
            if email:
                s_hash = anonymize_email(email)
                
                # Lógica de Camuflagem Sequencial
                if s_hash not in student_mask_map:
                    student_mask_map[s_hash] = f"Aluno {mask_counter:02d}"
                    mask_counter += 1
                
                d_students.append({
                    "student_hash": s_hash,
                    "full_name_masked": student_mask_map[s_hash],
                    "role": act.get('user_role'),
                    "is_deactivated": act.get('user_is_deactivated')
                })

        # Salva a dimensão de alunos já camuflada
        df_students = pd.DataFrame(d_students).drop_duplicates(subset=['student_hash'])
        path = f"{SILVER_DIR}/stg_students.json"
        df_students.to_json(path, orient='records', force_ascii=False, indent=4)
        logger.info(f"🥈 [SILVER] Identidades camufladas salvas em '{path}'")

        # [Omitido por brevidade: Restante da lógica de Cursos/Categorias similar à versão anterior]

class UdemyLoader:
    """ETAPA 3: GOLD - Exportação Final Sanitizada."""
    def run(self):
        logger.info("📤 [GOLD] Gerando arquivos finais com nomes camuflados...")
        excel_path = f"{GOLD_DIR}/udemy_consolidated_report.xlsx"
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for file in sorted(os.listdir(SILVER_DIR)):
                if file.endswith(".json"):
                    silver_name = file.replace(".json", "")
                    gold_name = TABLE_NAME_MAP.get(silver_name, silver_name)
                    df = pd.read_json(f"{SILVER_DIR}/{file}", encoding=DEFAULT_ENCODING)
                    
                    for col in df.select_dtypes(include=['object']).columns:
                        df[col] = df[col].apply(clean_for_excel)
                    
                    df_final = df.rename(columns=COLUMN_TRANSLATION_MAP)
                    df_final.to_excel(writer, sheet_name=gold_name[:31], index=False)
                    logger.info(f"🥇 [GOLD] Aba '{gold_name}' gerada com sucesso.")

if __name__ == "__main__":
    setup_directories()
    # Para testes, chame as classes na ordem Bronze -> Silver -> Gold
    # UdemyExtractor(creds).run()
    UdemyTransformer().run()
    UdemyLoader().run()