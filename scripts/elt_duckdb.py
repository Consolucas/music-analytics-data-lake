import duckdb
import os
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/')
DATA_DIR = f'{BASE_DIR}/data'
LAKE_DIR = f'{BASE_DIR}/datalake'

print("--- INICIANDO DATA LAKE (VERSÃO BLINDADA) ---")

# Criando pastas
if os.path.exists(LAKE_DIR):
    try:
        shutil.rmtree(LAKE_DIR)
    except:
        print("Aviso: Pasta em uso pelo OneDrive. Atualizando arquivos existentes...")

os.makedirs(f"{LAKE_DIR}/bronze", exist_ok=True)
os.makedirs(f"{LAKE_DIR}/silver", exist_ok=True)
os.makedirs(f"{LAKE_DIR}/gold", exist_ok=True)

con = duckdb.connect()

# ==============================================================================
# CAMADA BRONZE
# ==============================================================================
print("1. Criando Bronze (CSV -> Parquet)...")

con.sql(f"COPY (SELECT * FROM read_csv('{DATA_DIR}/Spotify_Youtube.csv', auto_detect=true)) TO '{LAKE_DIR}/bronze/songs.parquet' (FORMAT PARQUET)")


# ==============================================================================
# CAMADA SILVER (A Correção está aqui)
# ==============================================================================
print("2. Criando Silver (Limpeza)...")

query_songs = f"""
    COPY(
        SELECT column00 AS ID,
               Artist AS Artista,
               Track AS Nome_da_musica,
               Album_type AS Tipo_de_album,
               Danceability AS Dancante,
               Energy AS Energetica,
               Speechiness AS '%_falas',
               Acousticness AS '%_Acustico',
               Instrumentalness AS '%_Instrumental',
               Liveness AS Presenca_de_publico,
               Valence AS Astral,
               ROUND(Duration_ms / 1000.0, 0) AS duracao_segundos,
               Url_youtube
          FROM read_parquet('{LAKE_DIR}/bronze/songs.parquet')   
        ) TO '{LAKE_DIR}/silver/songs_clean.parquet' (FORMAT PARQUET)
"""

con.sql(query_songs)

# ==============================================================================
# CAMADA GOLD 
# ==============================================================================
print("3. Criando Gold (KPI's)...")
query_kpi = f"""
    COPY(
        SELECT
            Artista,
            ROUND(AVG(Dancante), 2) AS media_dancante,
            ROUND(AVG(Energetica), 2) AS media_energia,
            ROUND(AVG(Astral), 2) AS media_astral,
            ROUND(AVG("%_Acustico"), 2) AS media_acustico,
            ROUND(AVG("%_Instrumental"), 2) AS media_instrumental,
            ROUND(AVG("%_falas"), 2) AS media_letras    
        FROM read_parquet('{LAKE_DIR}/silver/songs_clean.parquet')
        GROUP BY Artista
        ) TO '{LAKE_DIR}/gold/perfil_bandas.parquet' (FORMAT PARQUET) 
"""

con.sql(query_kpi)

print("\n--- SUCESSO TOTAL! ---")
print("Top 5 bandas dancantes:")
con.sql(f"SELECT * FROM read_parquet('{LAKE_DIR}/gold/perfil_bandas.parquet') ORDER BY media_dancante DESC LIMIT 5").show()

con.close()