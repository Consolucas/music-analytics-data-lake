import duckdb
import pandas as pd
import os
from sklearn.neighbors import NearestNeighbors
import warnings

warnings.filterwarnings("ignore")

# Configuração de Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/')
LAKE_DIR = f"{BASE_DIR}/datalake"

print("--- 🎵 DJ IA: INICIANDO SISTEMA DE RECOMENDAÇÃO ---")

# 1. Carregar dados da SILVER (Detalhe por Música)
# Precisamos da Silver porque a Gold está agrupada por Artista, e queremos recomendar MÚSICAS.
print("1. Carregando acervo musical...")
df = duckdb.query(f"SELECT * FROM read_parquet('{LAKE_DIR}/silver/songs_clean.parquet')").to_df()

# Cria uma coluna "ID Visual" para facilitar a busca (Musica - Artista)
df['display_name'] = df['Nome_da_musica'] + " - " + df['Artista']

# Remove duplicatas (caso tenha a mesma música duas vezes)
df = df.drop_duplicates(subset=['display_name']).reset_index(drop=True)

print(f"   -> {len(df)} músicas carregadas.")

# 2. Selecionar as Features (O "DNA" da música)
# Essas são as colunas numéricas que definem o "som"
features = [
    'Dancante', 
    'Energetica', 
    '%_falas', 
    '%_Acustico', 
    '%_Instrumental', 
    'Presenca_de_publico', 
    'Astral'
]

# Matriz de Features (X)
X = df[features]
# Preenche vazios com 0 para não quebrar o cálculo
X = X.fillna(0)

# 3. Treinar o Modelo (Nearest Neighbors)
print("2. Treinando o ouvido da IA (Calculando distâncias)...")

# metric='cosine': Calcula o ângulo entre os vetores (melhor para similaridade)
# algorithm='brute': Força bruta (preciso para datasets pequenos/médios)
model = NearestNeighbors(n_neighbors=6, metric='cosine', algorithm='brute')
model.fit(X)

print("   -> Modelo treinado!")

# 4. Função de Recomendação
def recomendar(nome_parcial):
    # Busca músicas que CONTENHAM o texto digitado (ex: "Feel" acha "Feel Good Inc.")
    matches = df[df['display_name'].str.contains(nome_parcial, case=False, na=False)]
    
    if len(matches) == 0:
        return None
    
    # Pega o primeiro resultado encontrado
    musica_escolhida = matches.iloc[0]
    index_musica = matches.index[0]
    
    print(f"\n🔎 Baseado em: {musica_escolhida['display_name']}")
    
    # Extrai o vetor de características dessa música
    vetor_musica = X.iloc[index_musica].values.reshape(1, -1)
    
    # Pede pro modelo: "Quem são os 6 vizinhos mais próximos desse vetor?"
    distances, indices = model.kneighbors(vetor_musica)
    
    # O resultado vem como uma lista de índices. Vamos buscar os nomes.
    # O primeiro vizinho (índice 0) é a própria música, então pulamos ele.
    vizinhos_indices = indices.flatten()[1:]
    vizinhos_distancias = distances.flatten()[1:]
    
    recomendacoes = df.iloc[vizinhos_indices][['display_name', 'Artista', 'Url_youtube']].copy()
    
    # Adiciona a "similaridade" (quanto menor a distância, mais parecido)
    recomendacoes['distancia'] = vizinhos_distancias
    
    return recomendacoes

# 5. Loop Interativo
while True:
    print("\n" + "="*50)
    busca = input("Digite o nome de uma música (ou 'sair'): ")
    
    if busca.lower() == 'sair':
        break
        
    resultado = recomendar(busca)
    
    if resultado is None:
        print("❌ Música não encontrada. Tente outro nome.")
    else:
        print("🎧 Recomendações parecidas:")
        for idx, row in resultado.iterrows():
            # Mostra o nome e um gráfico de barrinha da similaridade inversa
            score = 1 - row['distancia'] # Transforma distância em % de similaridade
            print(f"   --> {row['display_name']} ({int(score*100)}% match)")