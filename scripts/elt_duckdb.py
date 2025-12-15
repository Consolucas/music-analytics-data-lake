import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

# 1. Configura conexão com S3
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql(f"""
    CREATE SECRET secret_aws (
        TYPE S3,
        KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID')}',
        SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY')}',
        REGION '{os.getenv('AWS_REGION')}'
    );
""")

bucket = os.getenv('BUCKET_NAME')

# ==========================================
# CAMADA BRONZE -> SILVER (Limpeza)
# ==========================================
print("Transformando Bronze (S3) -> Silver (S3)...")

# Note que agora lemos de 's3://...' e escrevemos em 's3://...'
query_silver = f"""
    COPY (
        SELECT 
            Artist AS artista,
            Track AS musica,
            Album AS album,
            -- Exemplo de limpeza: convertendo ms para minutos
            (Duration_ms / 60000.0) AS duracao_min,
            Stream AS stream_count
        FROM read_csv_auto('s3://{bucket}/bronze/*.csv')
    ) TO 's3://{bucket}/silver/songs_clean.parquet' (FORMAT PARQUET);
"""
con.sql(query_silver)

# ==========================================
# CAMADA SILVER -> GOLD (Agregação)
# ==========================================
print("Transformando Silver (S3) -> Gold (S3)...")

# Lemos o arquivo parquet que acabamos de gerar no S3
query_gold = f"""
    COPY (
        SELECT 
            artista,
            AVG(duracao_min) as media_duracao,
            SUM(stream_count) as total_streams
        FROM read_parquet('s3://{bucket}/silver/songs_clean.parquet')
        GROUP BY artista
        ORDER BY total_streams DESC
        LIMIT 10
    ) TO 's3://{bucket}/gold/top_artistas.parquet' (FORMAT PARQUET);
"""
con.sql(query_gold)

print("✅ Pipeline Nuvem finalizado com sucesso!")