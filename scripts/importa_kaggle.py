import kagglehub
import shutil
import os
import boto3
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env (suas chaves da AWS)
load_dotenv()

print("--- 1. INICIANDO DOWNLOAD DO KAGGLE ---")

# 1. Faz o download para o cache do Kaggle
cached_path = kagglehub.dataset_download("salvatorerastelli/spotify-and-youtube")
print(f"Baixado no cache temporário: {cached_path}")

# 2. Define o caminho local (Backup no seu PC)
destination_path = os.path.join(os.getcwd(), "data")
os.makedirs(destination_path, exist_ok=True)
print(f"Pasta local configurada: {destination_path}")

# 3. Configura o cliente da AWS S3
# Ele vai ler as chaves que você colocou no .env
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
bucket_name = os.getenv('BUCKET_NAME')

print("\n--- 2. PROCESSANDO ARQUIVOS E SUBINDO PARA AWS ---")

# 4. Loop: Move para pasta local E faz upload para o S3
for filename in os.listdir(cached_path):
    source_file = os.path.join(cached_path, filename)
    local_destination = os.path.join(destination_path, filename)
    
    if os.path.isfile(source_file):
        # A) Copia para sua pasta local 'data'
        shutil.copy(source_file, local_destination)
        print(f"💾 Salvo localmente: {filename}")
        
        # B) Faz upload para o S3 (Camada Bronze)
        # O 'Key' é o caminho/nome que o arquivo terá dentro do Bucket
        s3_path = f"bronze/{filename}"
        
        try:
            print(f"☁️  Subindo para S3: s3://{bucket_name}/{s3_path}...")
            s3_client.upload_file(local_destination, bucket_name, s3_path)
            print("   ✅ Upload OK!")
        except Exception as e:
            print(f"   ❌ Erro no upload: {e}")

print("-" * 30)
print("FIM DO PROCESSO: Dados sincronizados no PC e na Nuvem AWS.")