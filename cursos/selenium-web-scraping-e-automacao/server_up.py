"""
python -m http.server -d dist/ 8000

Web: https://curso-web-scraping.pages.dev/
"""
## Gerado com Gemini - Vibe Coding
import subprocess
import os
import sys

def iniciar_servidor(diretorio="dist", porta=8000):
    # Verifica se a pasta existe antes de tentar rodar
    if not os.path.exists(diretorio):
        print(f"Erro: O diretório '{diretorio}' não foi encontrado.")
        return

    print(f"Iniciando servidor em: http://localhost:{porta}")
    print(f"Servindo arquivos de: {os.path.abspath(diretorio)}")
    print("Pressione Ctrl+C para encerrar.")

    try:
        # Comando: python -m http.server -d dist/ 8000
        subprocess.run([sys.executable, "-m", "http.server", str(porta), "-d", diretorio])
    except KeyboardInterrupt:
        print("\nServidor encerrado pelo usuário.")

if __name__ == "__main__":
    iniciar_servidor()