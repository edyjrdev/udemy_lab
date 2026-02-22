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
import tempfile
from datetime import datetime

# --- CONFIGURAÇÃO GLOBAL DE PRIVACIDADE (LGPD) ---
# True: Ativa Anonimização (Hash SHA-256 + Máscara) | False: Mantém Dados Reais
ANONYMIZE_STUDENTS = True

# --- SISTEMA DE TELEMETRIA E LOG EXAUSTIVO (Pasta logs + Micro-tempos) ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_DATE = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_FILE = os.path.join(LOG_DIR, f"{LOG_DATE}_udemy_full_audit.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE ROBUSTEZ E PERFORMANCE ---
MAX_TEXT_LENGTH = 32000
DEFAULT_ENCODING = "utf-8"  # Fix definitivo para UnicodeDecodeError (byte 0x81)
OUTPUT_ENCODING = "utf-8-sig"
API_TIMEOUT = 60  # Proteção contra Bad Gateway (502) observada nos logs

# --- ESTRUTURA DE PASTAS (ARQUITETURA MEDALHÃO) ---
BRONZE_DIR, SILVER_DIR, GOLD_DIR = "01_bronze", "02_silver", "03_gold"
COURSE_PAGE_DIR = f"{BRONZE_DIR}/00_course/00_pages"
ACTIVITY_PAGE_DIR = f"{BRONZE_DIR}/01_activity/00_pages"
COURSE_ACT_PAGE_DIR = f"{BRONZE_DIR}/02_user_course_activity/00_pages"

SUB_DIRS = [
    LOG_DIR,
    COURSE_PAGE_DIR,
    ACTIVITY_PAGE_DIR,
    COURSE_ACT_PAGE_DIR,
    SILVER_DIR,
    GOLD_DIR
]

# --- DICIONÁRIOS DE TRADUÇÃO EXAUSTIVA (ATRIBUTOS DE DIMENSÃO) ---
LANGUAGE_MAP = {
    "en": "Inglês", "es": "Espanhol", "pt": "Português", "fr": "Francês",
    "de": "Alemão", "it": "Italiano", "zh": "Chinês", "ja": "Japonês", "ko": "Coreano"
}

LEVEL_MAP = {
    "All Levels": "Todos os Níveis", "Beginner": "Iniciante",
    "Intermediate": "Intermediário", "Expert": "Especialista"
}

CATEGORY_MAP = {
    "Development": "Desenvolvimento", "Business": "Negócios", "IT & Software": "TI e Software",
    "Design": "Design", "Machine Learning": "Machine Learning", "Web Development": "Desenv. Web",
    "Analytics": "Análise de Dados", "Cloud Certifications": "Certificações em Nuvem"
}

# Mapeamento exaustivo de abas para a camada Gold
TABLE_NAME_MAP = {
    "stg_students": "Alunos", "stg_courses": "Cursos", "stg_course_progress": "Progresso_Cursos",
    "stg_activity_items": "Atividades_Detalhadas", "stg_instructors": "Instrutores",
    "stg_languages": "Idiomas", "stg_levels": "Niveis_Dificuldade",
    "stg_categories": "Categorias", "stg_sub_categories": "Subcategorias"
}

# Dicionário de Tradução de Cabeçalhos (PT-BR) com Métricas Completas
COLUMN_TRANSLATION_MAP = {
    "student_id": "ID_Estudante", "full_name": "Nome_Completo", "course_id": "ID_Curso",
    "title_original": "Titulo_Original", "title_pt_br": "Titulo_Traduzido",
    "duracao_horas": "Carga_Horaria", "completion_ratio": "Percentual_Conclusao",
    "num_quizzes": "Qtd_Quizzes", "num_practice_tests": "Qtd_Testes_Praticos",
    "has_closed_caption": "Possui_Legendas", "is_practice_test_course": "Eh_Simulado",
    "item_title": "Titulo_Aula_Item", "last_accessed": "Ultimo_Acesso",
    "instructor_name": "Nome_Instrutor", "level_pt_br": "Nivel_Traduzido",
    "cat_pt_br": "Categoria_Traduzida", "lang_pt_br": "Idioma_Traduzido"
}

# --- FUNÇÕES DE SEGURANÇA E ESCRITA ATÔMICA ---
def setup_directories():
    """Garante a validação exaustiva de diretórios com log de tempo."""
    start = time.perf_counter()
    logger.info("📂 [SISTEMA] Iniciando validação exaustiva de diretórios...")
    for folder in SUB_DIRS:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
            logger.info(f"📁 [SISTEMA] Diretório criado: {folder}")
    logger.info(f"✅ [SISTEMA] Infraestrutura validada em {time.perf_counter()-start:.4f}s")

def safe_save_json(data, final_path):
    """Escrita atômica em UTF-8: protege contra arquivos corrompidos."""
    temp_dir = os.path.dirname(final_path)
    fd, temp_path = tempfile.mkstemp(dir=temp_dir, suffix=".json.tmp")
    try:
        with os.fdopen(fd, 'w', encoding=DEFAULT_ENCODING) as tmp:
            json.dump(data, tmp, ensure_ascii=False, indent=4)
        os.replace(temp_path, final_path)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise IOError(f"🛑 [ERRO IO] Falha na escrita atômica JSON: {e}")

def clean_for_excel(text):
    """Filtra caracteres que causam IllegalCharacterError no openpyxl."""
    if not isinstance(text, str):
        return text
    return re.sub(r'[\000-\010]|[\013-\014]|[\016-\037]', "", text)

def process_student_id(email):
    """Gera Hash SHA-256 para IDs de estudantes se solicitado."""
    if not email:
        return "unknown"
    val = email.lower().strip()
    return hashlib.sha256(val.encode(DEFAULT_ENCODING)).hexdigest() if ANONYMIZE_STUDENTS else val

# --- CAMADA 1: BRONZE (EXTRAÇÃO COM TELEMETRIA PÁGINA A PÁGINA) ---
class UdemyExtractor:
    def __init__(self, creds):
        self.auth = (creds.get("rest_client_id"), creds.get("rest_client_secret"))
        self.base_url = (
            f"https://{creds.get('ACCOUNT_NAME')}.udemy.com/api-2.0/"
            f"organizations/{creds.get('ACCOUNT_ID')}"
        )

    def fetch_paginated(self, endpoint, label, page_dir):
        t_start_ext = time.perf_counter()
        logger.info(f"📥 [BRONZE] Extração iniciada: {label}")
        all_data, page_num = [], 1
        next_url = f"{self.base_url}/{endpoint}?page_size=200"

        while next_url:
            p_start = time.perf_counter()
            p_path = f"{page_dir}/pag_{page_num:03d}.json"

            if os.path.exists(p_path):
                with open(p_path, 'r', encoding=DEFAULT_ENCODING) as f:
                    p_data = json.load(f)
                logger.info(f"📄 [CACHE] Página {page_num} de {label} lida do disco (⏳ {time.perf_counter()-p_start:.4f}s)")
            else:
                logger.info(f"📡 [API] Solicitando página {page_num} de {label}...")
                resp = requests.get(next_url, auth=self.auth, timeout=API_TIMEOUT)
                resp.raise_for_status()
                p_data = resp.json()
                safe_save_json(p_data, p_path)
                logger.info(f"💾 [BRONZE] Página {page_num} baixada e salva (⏳ {time.perf_counter()-p_start:.2f}s)")
                time.sleep(0.5)

            current_res = p_data.get('results', [])
            all_data.extend(current_res)
            logger.info(f"📑 [METRICA] Página {page_num}: acumulado {len(all_data)} registros.")
            next_url, page_num = p_data.get('next'), page_num + 1

        consolidated_path = f"{BRONZE_DIR}/{label.lower().replace(' ', '_')}_consolidated.json"
        safe_save_json(all_data, consolidated_path)
        logger.info(f"📦 [BRONZE] Consolidação final de {label}: {len(all_data)} registros totais (⏳ {time.perf_counter()-t_start_ext:.2f}s)")

    def run(self):
        self.fetch_paginated("courses/list/", "Courses", COURSE_PAGE_DIR)
        self.fetch_paginated("analytics/user-activity/", "Activity Items", ACTIVITY_PAGE_DIR)
        self.fetch_paginated("analytics/user-course-activity/", "User Course Progress", COURSE_ACT_PAGE_DIR)

# --- CAMADA 2: SILVER (MODELAGEM SNOWFLAKE EXAUSTIVA) ---
class UdemyTransformer:
    def run(self):
        t_start_silver = time.perf_counter()
        logger.info(f"🔄 [SILVER] Transformação Snowflake Iniciada (Anonimização: {ANONYMIZE_STUDENTS})")

        def load_raw(name):
            with open(f"{BRONZE_DIR}/{name}.json", 'r', encoding=DEFAULT_ENCODING) as f:
                return json.load(f)

        courses = load_raw("courses_consolidated")
        course_act = load_raw("user_course_progress_consolidated")
        activities = load_raw("activity_items_consolidated")

        d_students, d_instructors, d_langs, d_levels, d_cats, d_subcats = [], [], [], [], [], []
        f_courses, f_course_progress, f_activity_items = [], [], []
        student_mask_map, mask_counter = {}, 1

        logger.info("👥 [PRIVACIDADE] Mapeando alunos e aplicando camuflagem sequencial...")
        for act in course_act:
            email = act.get('user_email')
            if email:
                s_id = process_student_id(email)
                if ANONYMIZE_STUDENTS and s_id not in student_mask_map:
                    student_mask_map[s_id] = f"Aluno {mask_counter:02d}"
                    mask_counter += 1
                s_name = (
                    student_mask_map[s_id] if ANONYMIZE_STUDENTS
                    else f"{act.get('user_name', '')} {act.get('user_surname', '')}".strip()
                )
                d_students.append({"student_id": s_id, "full_name": s_name, "status": act.get('user_is_deactivated')})
                f_course_progress.append({
                    "course_id": act.get('course_id'),
                    "student_id": s_id,
                    "completion_ratio": act.get('completion_ratio'),
                    "last_accessed": act.get('course_last_accessed_date')
                })

        logger.info("🎓 [CURSOS] Extraindo dimensões e métricas completas de engajamento...")
        for c in courses:
            c_id, loc = c.get('id'), c.get('locale', {})
            prefix = loc.get('locale', 'en').split('_')[0]

            # Modelagem Snowflake: Dimensões Exaustivas
            d_langs.append({"language_id": loc.get('locale'), "lang_pt_br": LANGUAGE_MAP.get(prefix, prefix)})
            raw_lvl = c.get('level', 'All Levels')
            d_levels.append({"level_id": raw_lvl, "level_pt_br": LEVEL_MAP.get(raw_lvl, raw_lvl)})
            p_cat, p_sub = c.get('primary_category') or {}, c.get('primary_subcategory') or {}
            if p_cat.get('title'):
                d_cats.append({
                    "cat_id": p_cat.get('id'),
                    "cat_pt_br": CATEGORY_MAP.get(p_cat.get('title'), p_cat.get('title'))
                })
            if p_sub.get('title'):
                d_subcats.append({"sub_id": p_sub.get('id'), "sub_original": p_sub.get('title')})
            for inst in c.get('visible_instructors', []):
                d_instructors.append({
                    "instructor_id": inst.get('id'),
                    "instructor_name": inst.get('display_name')
                })

            # Fato Cursos: Todas as métricas disponíveis
            f_courses.append({
                "course_id": c_id,
                "title_original": c.get('title'),
                "duracao_horas": round((c.get('estimated_content_length_video') or 0) / 60, 2),
                "num_quizzes": c.get('num_quizzes', 0),
                "num_practice_tests": c.get('num_practice_tests', 0),
                "has_closed_caption": c.get('has_closed_caption', False),
                "is_practice_test_course": c.get('is_practice_test_course', False)
            })

        for item in activities:
            f_activity_items.append({
                "student_id": process_student_id(item.get('user_email')),
                "course_id": item.get('course_id'),
                "item_title": item.get('item_title') or "Item sem Título"
            })

        datasets = {
            "stg_students": d_students, "stg_instructors": d_instructors, "stg_courses": f_courses,
            "stg_course_progress": f_course_progress, "stg_activity_items": f_activity_items,
            "stg_languages": d_langs, "stg_levels": d_levels, "stg_categories": d_cats,
            "stg_sub_categories": d_subcats
        }
        for name, data in datasets.items():
            pd.DataFrame(data).drop_duplicates().to_json(
                f"{SILVER_DIR}/{name}.json", orient='records',
                force_ascii=False, indent=4
            )
        logger.info(f"🥈 [SILVER] {len(datasets)} datasets auditados (⏳ {time.perf_counter()-t_start_silver:.2f}s)")

# --- CAMADA 3: GOLD (EXPORTAÇÃO CSV + XLSX COM TELEMETRIA POR ABA) ---
class UdemyLoader:
    def run(self):
        t_start_gold = time.perf_counter()
        logger.info("📤 [GOLD] Iniciando exportação final exaustiva...")
        if os.path.exists(GOLD_DIR):
            shutil.rmtree(GOLD_DIR)
        os.makedirs(GOLD_DIR)

        final_excel = f"{GOLD_DIR}/udemy_consolidated_report.xlsx"
        temp_excel = final_excel + ".tmp.xlsx"

        try:
            with pd.ExcelWriter(temp_excel, engine='openpyxl') as writer:
                for file in sorted(os.listdir(SILVER_DIR)):
                    if file.endswith(".json"):
                        t_aba = time.perf_counter()
                        silver_name = file.replace(".json", "")
                        gold_name = TABLE_NAME_MAP.get(silver_name, silver_name)
                        df = pd.read_json(f"{SILVER_DIR}/{file}", encoding=DEFAULT_ENCODING)

                        for col in df.select_dtypes(include=['object']).columns:
                            df[col] = df[col].apply(clean_for_excel)

                        df_final = df.rename(columns=COLUMN_TRANSLATION_MAP)
                        df_final.to_excel(writer, sheet_name=gold_name[:31], index=False)
                        df_final.to_csv(f"{GOLD_DIR}/{gold_name}.csv", index=False, encoding="utf-8-sig")
                        logger.info(f"🥇 [GOLD] Aba/CSV '{gold_name}' gerada (⏳ {time.perf_counter()-t_aba:.4f}s) | Linhas: {len(df_final)}")
            os.replace(temp_excel, final_excel)
            logger.info(f"✨ [GOLD] Relatório Final consolidado em {time.perf_counter()-t_start_gold:.2f}s")
        except Exception as e:
            if os.path.exists(temp_excel):
                os.remove(temp_excel)
            raise e

if __name__ == "__main__":
    t_global = time.perf_counter()
    logger.info("🚀 [START] Iniciando Pipeline de Dados Udemy Business v3.13.3")
    try:
        setup_directories()
        with open("credencial.json", 'r', encoding=DEFAULT_ENCODING) as f:
            creds_data = json.load(f)[0]

        extractor = UdemyExtractor(creds_data)
        extractor.run()

        transformer = UdemyTransformer()
        transformer.run()

        loader = UdemyLoader()
        loader.run()
        logger.info(f"🏁 [FINISH] Pipeline finalizado com SUCESSO TOTAL (⏳ {time.perf_counter()-t_global:.2f}s)")
    except Exception as e:
        logger.critical(f"🛑 [ERRO FATAL] {e}", exc_info=True)