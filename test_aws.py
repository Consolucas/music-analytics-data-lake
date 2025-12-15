import duckdb
import os
from dotenv import load_dotenv

# Carrega as senhas do arquivo .env
load_dotenv()

# Conecta no DuckDB (em memória mesmo, só pra testar)
con = duckdb.connect()

# 1. Instala a extensão httpfs (que permite falar com S3)
con.sql("INSTALL httpfs; LOAD httpfs;")

# 2. Configura as credenciais da AWS no DuckDB
secret_query = f"""
    CREATE SECRET secret1 (
        TYPE S3,
        KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID')}',
        SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY')}',
        REGION '{os.getenv('AWS_REGION')}'
    );
"""
con.sql(secret_query)

print("✅ Conectado à AWS com sucesso!")

# 3. Tenta listar o que tem no seu bucket S3 (vai vir vazio ou com as pastas)
bucket = os.getenv('BUCKET_NAME')
try:
    print(f"Tentando ler: s3://{bucket}/")
    # O comando GLOB olha arquivos no S3
    result = con.sql(f"SELECT * FROM glob('s3://{bucket}/*')").show()
    print("Se apareceu uma tabela (mesmo vazia), funcionou!")
except Exception as e:
    print(f"❌ Erro: {e}")