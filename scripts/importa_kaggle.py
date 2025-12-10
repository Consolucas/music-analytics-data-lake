import kagglehub
import shutil
import os

# 1. Faz o download para o cache do Kaggle
cached_path = kagglehub.dataset_download("salvatorerastelli/spotify-and-youtube")
print(f"Baixado no cache temporário: {cached_path}")

# 2. Define o caminho relativo
# os.getcwd() pega a pasta atual do seu projeto (onde você abriu o VS Code)
# Isso cria o caminho: .../seu-projeto/data
destination_path = os.path.join(os.getcwd(), "data")

# 3. Cria a pasta se ela não existir
os.makedirs(destination_path, exist_ok=True)
print(f"Pasta de destino configurada: {destination_path}")

# 4. Move os arquivos da pasta de cache para a sua pasta
# Listamos todos os arquivos na pasta baixada e movemos um por um
for filename in os.listdir(cached_path):
    source_file = os.path.join(cached_path, filename)
    destination_file = os.path.join(destination_path, filename)
    
    # Verifica se é um arquivo (para evitar mover subpastas se houver)
    if os.path.isfile(source_file):
        shutil.copy(source_file, destination_file)
        print(f"Arquivo copiado: {filename}")

print("-" * 30)
print(f"Sucesso! Arquivos salvos em: {destination_path}")