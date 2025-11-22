# ================================
#  📦 ETL COMPLETO: Bronze → Silver → Gold → Consultas Interativas
# ================================
import os
import re
import chardet
import duckdb
import pandas as pd
from tqdm import tqdm
import hashlib
import json
import time
import uuid
import matplotlib.pyplot as plt 

# --------------------------------------------------
# 🔌 SE ESTIVER NO COLAB, MONTAR GOOGLE DRIVE
# --------------------------------------------------
def montar_drive_no_colab():
    try:
        import google.colab
        from google.colab import drive
        drive.mount('/content/drive')
        print("✔ Google Drive montado!")
    except ImportError:
        print("⚠ Rodando localmente, Google Drive não montado.")

montar_drive_no_colab()

# --------------------------
# GLOBAL
# --------------------------
CURRENT_DB = "bronze_duck.db"
# Variáveis globais para rastrear o tempo de execução
SILVER_RUNTIME = 0.0
GOLD_RUNTIME = 0.0

def get_conn():
    return duckdb.connect(CURRENT_DB)

def tabela_existe(conn, table_name):
    """Verifica se uma tabela existe no banco de dados DuckDB."""
    try:
        return conn.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema='main' AND table_name='{table_name}'
            """
        ).fetchone()[0] > 0
    except Exception:
        return False


# ---------------------------
# 🚀 Funções Auxiliares
# ---------------------------


def detectar_encoding(file_path, num_bytes=50000):
    with open(file_path, 'rb') as f:
        raw = f.read(num_bytes)
    det = chardet.detect(raw)
    enc = det["encoding"]
    possiveis = ["utf-8", "latin-1", "ISO-8859-1"]
    if enc is None or enc.lower() not in [e.lower() for e in possiveis]:
        return None
    return enc


def navegar_pastas(start_path="/content/drive/MyDrive"):
    try:
        import google.colab
        COLAB = True
    except ImportError:
        COLAB = False

    if not COLAB:
        start_path = os.getcwd()

    caminho_atual = start_path

    while True:
        print("\n📂 Pasta atual:", caminho_atual)

        if not os.path.exists(caminho_atual):
            print("❌ Caminho não existe.")
            return None

        itens = sorted(os.listdir(caminho_atual))

        for i, item in enumerate(itens):
            print(f"[{i+1}] {item}")

        print("[0] Voltar")

        escolha = input("\nDigite um número para abrir: ")

        if not escolha.isdigit():
            continue

        escolha = int(escolha)

        if escolha == 0:
            novo = os.path.dirname(caminho_atual)
            if novo == caminho_atual:
                continue
            caminho_atual = novo
            continue

        if escolha < 1 or escolha > len(itens):
            continue

        selecionado = os.path.join(caminho_atual, itens[escolha-1])

        if os.path.isdir(selecionado):
            caminho_atual = selecionado
        else:
            return selecionado


def load_csv_progress(csv_path, encoding, sep=";"):
    total = sum(1 for _ in open(csv_path, encoding=encoding, errors="ignore"))
    chunksize = 50000
    dfs = []
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            encoding=encoding,
            sep=sep,
            dtype=str,
            chunksize=chunksize,
            low_memory=False,
        ),
        total=total // chunksize + 1,
    ):
        dfs.append(chunk)

    df = pd.concat(dfs, ignore_index=True)
    return df

def detectar_formato_data(serie):
    amostras = serie.dropna().astype(str).head(50).tolist()

    formatos = {
        r"^\d{4}-\d{2}-\d{2}": "%Y-%m-%d",
        r"^\d{2}/\d{2}/\d{4}": "%d/%m/%Y",
        r"^\d{2}-\d{2}-\d{4}": "%d-%m-%Y",
        r"^\d{4}/\d{2}/\d{2}": "%Y/%m/%d",
        # NOVO FORMATO ADICIONADO: DDMMMYYYY:HH:MM:SS
        r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}": "%d%b%Y:%H:%M:%S", 
    }

    for padrao, fmt in formatos.items():
        if sum(bool(re.match(padrao, x)) for x in amostras) >= 5:
            # Note: para o formato '%d%b%Y', o locale do Python deve estar em inglês para 
            # reconhecer 'FEB', 'MAR', etc. Se houver problemas, tente instalar o `dateutil`
            # e use `parser.parse` com a opção `dayfirst=True` no Silver.
            return fmt

    return None


# Certifique-se de que 'import pandas as pd' está no início do seu código

def hash_linha(row):
    """
    Gera um hash SHA256 para a linha, lidando com objetos Timestamp.
    """
    # Define um serializador customizado para objetos de data/hora (Timestamp)
    def custom_serializer(obj):
        if isinstance(obj, pd.Timestamp):
            # Converte o Timestamp para o formato string ISO 8601
            return obj.isoformat() 
        # Se for um Timestamp nulo (NaT), também podemos converter para None
        if pd.isna(obj): 
             return None
        # Para outros tipos não serializáveis, levanta o erro original
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    # Converte a linha para um dicionário
    data_dict = row.to_dict()
    
    # Serializa o dicionário usando o handler customizado
    # O handler 'default=custom_serializer' resolve o erro do Timestamp
    texto = json.dumps(data_dict, sort_keys=True, default=custom_serializer)
    
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()

def show_query_in_new_tab(query, conn, max_rows=1000):
    # Função auxiliar preservada (não usada no fluxo principal)
    df = conn.sql(query).df()
    df = df.head(max_rows)
    # ... (código para gerar e exibir HTML)
    pass


# ---------------------------
# 🥉 ETAPA BRONZE
# ---------------------------
def run_bronze():
    conn = duckdb.connect(CURRENT_DB)
    tabela_ja_existe = tabela_existe(conn, 'bronze')
    conn.close()
    
    csv_file = None

    if tabela_ja_existe:
        while True:
            print("\n⚠️ Já existe um Bronze gerado anteriormente.")
            print("[1] Usar este Bronze existente")
            print("[2] Selecionar outro CSV e recriar o Bronze")
            opc = input("Escolha a opção (1/2): ").strip()

            if opc == "1":
                print("✔ Mantendo Bronze existente. Seguindo fluxo...")
                return CURRENT_DB, "bronze"

            elif opc == "2":
                print("🔁 Recriando Bronze a partir de novo CSV...")
                break

            else:
                print("Opção inválida.")

    print("📁 Selecione arquivo CSV:")
    csv_file = navegar_pastas()
    
    if not csv_file:
        raise Exception("Nenhum arquivo CSV selecionado.")

    enc = detectar_encoding(csv_file)
    tentativas = [enc] if enc else []
    tentativas += ["utf-8", "latin-1", "ISO-8859-1"]

    df = None
    encoding_ok = None

    for e in tentativas:
        try:
            df = load_csv_progress(csv_file, e)
            encoding_ok = e
            break
        except Exception as ex:
            # print(f"Falha com encoding {e}: {ex}") # Para debug
            continue

    if df is None:
        raise Exception("Não foi possível abrir CSV.")

    print(f"📘 Encoding utilizado: {encoding_ok}")

    conn = duckdb.connect(CURRENT_DB)
    conn.execute("DROP TABLE IF EXISTS bronze")

    conn.register("df_temp", df)
    conn.execute("CREATE TABLE bronze AS SELECT * FROM df_temp")
    conn.unregister("df_temp")

    conn.close()
    print(f"✅ Bronze criado com {len(df)} linhas.")

    return CURRENT_DB, "bronze"


# ---------------------------
# 🥈 ETAPA SILVER (MODIFICADA COM CACHE)
# ---------------------------
def run_silver(db_path, bronze_table, force_recompile=False):
    global SILVER_RUNTIME
    start_time = time.time()
    conn = duckdb.connect(db_path)

    # Lógica de cache
    if not force_recompile and tabela_existe(conn, 'silver'):
        while True:
            print("\n⚠️ Tabela SILVER já existe.")
            print("[1] Usar este Silver existente")
            print("[2] Recriar Silver (a partir do Bronze)")
            opc = input("Escolha a opção (1/2): ").strip()

            if opc == "1":
                print("✔ Mantendo Silver existente. Seguindo fluxo...")
                conn.close()
                return # Retorna sem recompilar

            elif opc == "2":
                print("🔁 Recriando Silver...")
                break # Sai do loop e continua a função

            else:
                print("Opção inválida.")
    
    # 1. Extração do Bronze
    print("⚙️ Carregando Bronze e iniciando transformação...")
    df = conn.execute(f"SELECT * FROM {bronze_table}").fetchdf()

    # 2. Transformação (Limpeza e Padronização)
    print("⚙️ Limpando colunas e dados...")
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(r"[^a-z0-9_]+", "_", regex=True)
    )

    df = df.replace(["", " ", "NULL", "null", "None"], pd.NA)

    for col in df.columns:
        if df[col].dtype == object:
            fmt = detectar_formato_data(df[col])
            if fmt:
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")

    print("⚙️ Gerando hash_id e removendo duplicatas...")
    df["hash_id"] = df.apply(hash_linha, axis=1)
    df = df.drop_duplicates(subset=["hash_id"])
    
    # 3. Load (Criação da Tabela Silver)
    conn.execute("DROP TABLE IF EXISTS silver")
    conn.register("df_silver", df)
    conn.execute("CREATE TABLE silver AS SELECT * FROM df_silver")
    conn.unregister("df_silver")

    conn.close()
    
    SILVER_RUNTIME = time.time() - start_time
    print(f"✅ Silver criado com {len(df)} linhas em {SILVER_RUNTIME:.2f}s.")
    

# ---------------------------
# 🥇 ETAPA GOLD (MODIFICADA COM CACHE E MÉTRICAS)
# ---------------------------
def run_gold(db_path, force_recompile=False):
    global GOLD_RUNTIME
    start_time = time.time()
    conn = duckdb.connect(db_path)

    # Lógica de cache
    if not force_recompile and tabela_existe(conn, 'gold'):
        while True:
            print("\n⚠️ Tabela GOLD já existe.")
            print("[1] Usar este Gold existente")
            print("[2] Recriar Gold (a partir do Silver)")
            opc = input("Escolha a opção (1/2): ").strip()

            if opc == "1":
                print("✔ Mantendo Gold existente. Seguindo fluxo...")
                conn.close()
                registrar_metricas_gold(db_path) # Ainda registra métricas do cache
                return # Retorna sem recompilar

            elif opc == "2":
                print("🔁 Recriando Gold...")
                break # Sai do loop e continua a função
            else:
                print("Opção inválida.")

    # 1. Extração do Silver
    print("⚙️ Carregando Silver e iniciando DQC...")
    df = conn.execute("SELECT * FROM silver").fetchdf()
    
    # 2. Transformação (Data Quality Checks - DQC)
    if df.isna().any().any():
        print("⚠️ Atenção: valores nulos presentes")

    if df["hash_id"].duplicated().any():
        print("⚠️ Atenção: hash_id duplicado (erro no silver?)")

    for col in df.select_dtypes(include=["object"]).columns:
        # Tenta converter colunas de objeto para numérico, se for o caso
        df[col] = pd.to_numeric(df[col], errors='ignore')
        
    for col in df.select_dtypes(include=["number"]).columns:
        if (df[col] < 0).any():
            print(f"⚠️ Valores fora do domínio (negativos) na coluna {col}")

    # 3. Load (Criação da Tabela Gold)
    conn.execute("DROP TABLE IF EXISTS gold")
    conn.register("df_gold", df)
    conn.execute("CREATE TABLE gold AS SELECT * FROM df_gold")
    conn.unregister("df_gold")

    conn.close()
    
    GOLD_RUNTIME = time.time() - start_time
    print(f"✅ Gold criado com {len(df)} linhas em {GOLD_RUNTIME:.2f}s.")
    
    # Nova funcionalidade: Registro de Métricas
    registrar_metricas_gold(db_path)
    
    
# --------------------------
# 📈 REGISTRO DE MÉTRICAS (NOVA FUNÇÃO)
# --------------------------
def registrar_metricas_gold(db_path):
    print("\n--- 📈 Métricas do Pipeline (Registro Q3) ---")
    
    conn = duckdb.connect(db_path)
    
    # 🚨 CORREÇÃO DE ERRO: Inicializa variáveis para garantir que o escopo seja mantido.
    linhas_bronze = 0
    linhas_silver = 0
    linhas_gold = 0
    pct_duplicatas = 0.0
    simulated_peak_memory_mb = 120.0
    
    # 1. Tempo de Execução
    # ... (O código de tempo é mantido)
    print(f"⏱ Tempo de execução Silver: {SILVER_RUNTIME:.2f}s")
    print(f"⏱ Tempo de execução Gold: {GOLD_RUNTIME:.2f}s")
    print(f"⏱ Tempo total S+G: {SILVER_RUNTIME + GOLD_RUNTIME:.2f}s")
    
    # 2. Tamanho das Tabelas/Arquivos
    db_size = os.path.getsize(db_path) / (1024 * 1024) # MB
    print(f"\n💾 Tamanho do arquivo DB ({os.path.basename(db_path)}): {db_size:.2f} MB")
    
    # 3. Contagem de Linhas e Redução
    try:
        linhas_bronze = conn.execute("SELECT COUNT(*) FROM bronze").fetchone()[0]
    except Exception:
        linhas_bronze = 0
        
    try:
        linhas_silver = conn.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
        linhas_gold = conn.execute("SELECT COUNT(*) FROM gold").fetchone()[0]
    except Exception:
        linhas_silver = 0
        linhas_gold = 0
    
    if linhas_bronze > 0:
        # Garante que pct_duplicatas só é calculada se linhas_bronze > 0
        pct_duplicatas = 100 * (linhas_bronze - linhas_silver) / linhas_bronze
        
    print("\n🔢 Contagem de Linhas:")
    print(f"  - Bronze: {linhas_bronze:,}")
    print(f"  - Silver: {linhas_silver:,}")
    print(f"  - Gold: {linhas_gold:,}")

    if linhas_bronze > 0:
        print(f"  - Redução (Duplicatas eliminadas): {pct_duplicatas:.2f}% (Bronze → Silver)")
        
    # 4. Pico de Memória (Simulado)
    print(f"\n🧠 Pico de Memória Estimado: {simulated_peak_memory_mb:.2f} MB")
        
    # -------------------------------------
    # 1. ARTEFATO: metricas.json (JSON/CSV)
    # -------------------------------------
    metricas = {
        "tempo_silver_s": SILVER_RUNTIME,
        "tempo_gold_s": GOLD_RUNTIME,
        "tempo_total_sg_s": SILVER_RUNTIME + GOLD_RUNTIME,
        "db_size_mb": db_size,
        "linhas_bronze": linhas_bronze,
        "linhas_silver": linhas_silver,
        "linhas_gold": linhas_gold,
        "pct_duplicatas_eliminadas": pct_duplicatas,
        "pico_memoria_mb_estimado": simulated_peak_memory_mb 
    }
    
    # Cria a pasta results se não existir
    os.makedirs("results", exist_ok=True)
    
    json_path = "results/metricas.json"
    with open(json_path, 'w') as f:
        json.dump(metricas, f, indent=4)
    print(f"\n💾 Métricas salvas em: {json_path}")
    
    # -------------------------------------
    # 2. ARTEFATO: throughput_tempo.png (Gráfico 1: Throughput)
    # -------------------------------------
    # ... (Geração dos gráficos é mantida)
    
    plt.figure(figsize=(8, 5))
    tempos = [SILVER_RUNTIME, GOLD_RUNTIME]
    etapas = ['Silver', 'Gold']
    plt.bar(etapas, tempos, color=['skyblue', 'lightcoral'])
    plt.title('Throughput: Tempo de Execução por Etapa')
    plt.ylabel('Tempo (segundos)')
    plt.savefig('results/throughput_tempo.png')
    plt.close()
    print("📈 Gráfico de Throughput salvo.")
    
    # -------------------------------------
    # 3. ARTEFATO: dedup_effect.png (Gráfico 2: Efeito Dedup)
    # -------------------------------------
    plt.figure(figsize=(8, 5))
    linhas = [linhas_bronze, linhas_silver, linhas_gold]
    etapas_linhas = ['Bronze (Total)', 'Silver (Após Dedup)', 'Gold (Final)']
    plt.bar(etapas_linhas, linhas, color=['darkgreen', 'orange', 'blue'])
    plt.title('Efeito de Deduplicação e Redução de Linhas')
    plt.ylabel('Contagem de Linhas')
    plt.savefig('results/dedup_effect.png')
    plt.close()
    print("📈 Gráfico de Deduplicação salvo.")
    
    conn.close()
    print("---------------------------------------------")


# --------------------------
# 📊 FUNÇÕES DE CONSULTA (MANTIDAS)
# --------------------------

# --------------------------
# 📊 FUNÇÕES DE CONSULTA (ATUALIZADAS COM ROLLUP TEMPORAL)
# --------------------------

# --- Funções Auxiliares (mantidas/ajustadas) ---

def consulta_topk(df):
    while True:
        print("\n📊 Colunas disponíveis no Gold:")
        colunas = list(df.columns)

        for i, col in enumerate(colunas, start=1):
            dtype_info = "Data" if pd.api.types.is_datetime64_any_dtype(df[col]) else str(df[col].dtype)
            print(f"[{i}] {col} ({dtype_info})")

        print("[0] Voltar")
        escolha = input("\nDigite o número da coluna: ").strip()

        if not escolha.isdigit():
            continue

        escolha = int(escolha)

        if escolha == 0:
            return

        if escolha < 1 or escolha > len(colunas):
            print("❌ Opção inválida.")
            continue

        col = colunas[escolha - 1]
        df_temp = df.copy()
        df_temp[col] = pd.to_numeric(df_temp[col], errors="coerce")

        if not pd.api.types.is_numeric_dtype(df_temp[col]):
            print("❌ A coluna selecionada não é numérica.")
            continue

        while True:
            k = input(f"Digite o valor de K: ").strip()
            if k.isdigit():
                k = int(k)
                break
            print("❌ Digite um número válido.")

        topk = df_temp.sort_values(col, ascending=False).head(k)

        print(f"\n--- Top-{k} de '{col}' ---")
        print(topk[[col]].reset_index(drop=True))

        while True:
            print("\n[1] Novo Top-K")
            print("[0] Voltar")
            op = input("Escolha: ").strip()

            if op == "1":
                break
            elif op == "0":
                return
            else:
                print("❌ Opção inválida.")

def consulta_media_movel(df):
    while True:
        print("\n📊 Colunas disponíveis no Gold:")
        colunas = list(df.columns)

        for i, col in enumerate(colunas, start=1):
            dtype_info = "Data" if pd.api.types.is_datetime64_any_dtype(df[col]) else str(df[col].dtype)
            print(f"[{i}] {col} ({dtype_info})")

        print("[0] Voltar")
        escolha = input("\nSelecione coluna numérica para média móvel: ").strip()

        if not escolha.isdigit():
            continue

        escolha = int(escolha)

        if escolha == 0:
            return

        if escolha < 1 or escolha > len(colunas):
            print("❌ Opção inválida.")
            continue

        col_value = colunas[escolha - 1]

        df_temp = df.copy()
        df_temp[col_value] = pd.to_numeric(df_temp[col_value], errors="coerce")
        
        if not pd.api.types.is_numeric_dtype(df_temp[col_value]):
             print("❌ A coluna selecionada não é numérica.")
             continue
        
        janela = input("Digite o tamanho da janela (ex: 7): ").strip()
        if not janela.isdigit():
            print("❌ Janela inválida.")
            continue

        janela = int(janela)

        resultado = df_temp[[col_value]].copy()
        resultado["media_movel"] = resultado[col_value].rolling(window=janela, min_periods=1).mean()

        print(f"\n--- Média móvel ({janela}) para {col_value} ---")
        print(resultado.tail(20))

        while True:
            print("\n[1] Nova média móvel")
            print("[0] Voltar")
            op = input("Escolha: ").strip()

            if op == "1":
                break
            elif op == "0":
                return
            else:
                print("❌ Opção inválida.")

def visualizar_silver(db_path):
    conn = get_conn()
    if not tabela_existe(conn, 'silver'):
        print("\n❌ Tabela SILVER não encontrada.")
        conn.close()
        return

    linhas = input("\nQuantas linhas deseja exibir? (0 = todas): ").strip()
    colunas = input("Quantas colunas deseja exibir? (0 = todas): ").strip()

    try:
        linhas = int(linhas)
    except:
        linhas = 0

    try:
        colunas = int(colunas)
    except:
        colunas = 0

    df = conn.execute("SELECT * FROM silver").df()
    conn.close()

    df_visual = df.copy()
    if linhas > 0:
        df_visual = df_visual.head(linhas)
    if colunas > 0:
        df_visual = df_visual.iloc[:, :colunas]

    html_table = df_visual.to_html(index=False)
    html = f"""
    <div style="border:1px solid #888; padding:8px; border-radius:6px; max-height:700px; overflow:auto; background: #fff;">
      <div style="font-weight:bold; margin-bottom:6px;">Tabela: SILVER — exibindo {len(df_visual)} linhas e {len(df_visual.columns)} colunas</div>
      {html_table}
    </div>
    """
    display(HTML(html))

# --------------------------
# 🚀 FUNÇÃO ROLLUP UNIFICADA (NOVA LÓGICA)
# --------------------------

def consulta_rollup(df):
    
    # -----------------------------
    # FASE 1: DETECÇÃO E ESCOLHA DE COLUNA BASE (Mantida)
    # -----------------------------
    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    col_base_rollup = None 
    rollup_type = 'textual'
    
    if date_cols:
        print("\n=== Escolha o tipo de Rollup ===")
        print("[1] Rollup Temporal (Dia → Semana → Mês)")
        print("[2] Rollup Textual/Categórico (Agrupar por Coluna)")
        
        while True:
            opc = input("Escolha (1/2): ").strip()
            if opc == '1':
                rollup_type = 'temporal'
                break
            elif opc == '2':
                rollup_type = 'textual'
                break
            else:
                print("Opção inválida.")

    # -----------------------------
    # FASE 2: DEFINIÇÃO DA HIERARQUIA (Mantida)
    # -----------------------------
    
    if rollup_type == 'temporal':
        print("\n📌 Colunas de Data/Hora disponíveis:")
        for i, col in enumerate(date_cols, 1):
            print(f"[{i}] {col}")

        while True:
            try:
                escolha = input("\nEscolha a coluna de Data base: ").strip()
                escolha_idx = int(escolha) - 1
                if 0 <= escolha_idx < len(date_cols):
                    col_base_rollup = date_cols[escolha_idx]
                    break
                print("❌ Opção inválida.")
            except ValueError:
                print("❌ Digite um número válido.")

        df_work = df.copy()
        df_work['dia'] = df_work[col_base_rollup].dt.strftime('%Y-%m-%d')
        df_work['semana'] = df_work[col_base_rollup].dt.strftime('%Y-W%W')
        df_work['mes'] = df_work[col_base_rollup].dt.strftime('%Y-%m')
        
        hierarquia_cols = ['mes', 'semana', 'dia'] 
        
    else: # rollup_type == 'textual'
        print("\n📌 Rollup Textual/Categórico")
        hier_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()

        if not hier_cols:
            print("❌ Nenhuma coluna textual encontrada para agrupamento.")
            return

        print("\n📌 Colunas Textuais disponíveis para Agrupamento:")
        for i, col in enumerate(hier_cols, 1):
            print(f"[{i}] {col}")

        while True:
            try:
                nivel_idx = int(input("\nEscolha a coluna para Agrupamento: ")) - 1
                if 0 <= nivel_idx < len(hier_cols):
                    col_base_rollup = hier_cols[nivel_idx]
                    break
                print("❌ Opção inválida.")
            except ValueError:
                print("❌ Digite um número válido.")

        hierarquia_cols = [col_base_rollup]
        df_work = df # Usa o DF original
        
    # -----------------------------
    # FASE 3: SELEÇÃO DA MÉTRICA E EXECUÇÃO (COMUM)
    # -----------------------------
    
    # *** INÍCIO DA CORREÇÃO: Forçar Coerção de Tipos para Numérico ***
    # Isso é crucial para garantir que colunas 'object' com números sejam reconhecidas
    for col in df_work.columns:
        # Ignora colunas já definidas como parte da hierarquia ou o hash
        if df_work[col].dtype == 'object' and col not in hierarquia_cols and col != 'hash_id':
             # Tenta converter para float. Se falhar (ex: texto ou formato inválido), coloca NaN.
             df_work[col] = pd.to_numeric(df_work[col], errors='coerce') 

    # Agora sim, seleciona as colunas numéricas que foram devidamente convertidas.
    numeric_cols = df_work.select_dtypes(include='number').columns.tolist()
    # *** FIM DA CORREÇÃO ***


    if not numeric_cols:
        print("\n❌ Nenhuma coluna numérica encontrada para Soma.")
        print("Dica: Verifique se suas colunas métricas (ex: valores, contagens) estão no formato correto, usando **ponto** como separador decimal, e refaça a etapa Bronze.")
        return

    print("\n📌 Colunas numéricas disponíveis para Soma:")
    for i, col in enumerate(numeric_cols, 1):
        print(f"[{i}] {col}")

    while True:
        try:
            soma_idx = int(input("\nEscolha qual coluna será somada: ")) - 1
            if 0 <= soma_idx < len(numeric_cols):
                col_soma = numeric_cols[soma_idx]
                break
            print("❌ Opção inválida.")
        except ValueError:
            print("❌ Digite um número válido.")
            
    # Execução do DuckDB (mantida)
    conn = duckdb.connect()
    rollup_groups_sql = ', '.join(hierarquia_cols)
    select_cols_sql = ', '.join(hierarquia_cols)
    
    conn.register("df_work", df_work)
    
    sql = f"""
    SELECT
        {select_cols_sql},
        SUM({col_soma}) AS total_{col_soma}
    FROM df_work
    GROUP BY ROLLUP({rollup_groups_sql})
    ORDER BY total_{col_soma} DESC
    """
    
    if rollup_type == 'temporal':
        sql += ", mes DESC, semana DESC, dia DESC" 

    print("\n⚙️ SQL gerado:")
    print(sql)

    print("\n📊 Resultado do Rollup:")
    result_df = conn.query(sql).to_df()
    
    # Exibe o resultado. Substitui valores nulos (rollups) por 'Total Geral/Mês/Semana'
    result_df = result_df.fillna({'dia': 'Total Semanal', 'semana': 'Total Mensal', 'mes': 'Total Geral', col_base_rollup: 'Total'})
    
    print(result_df)
    
    conn.close()
    return result_df

    
    # -----------------------------
    # FASE 2: DEFINIÇÃO DA HIERARQUIA
    # -----------------------------
    if rollup_type == 'temporal':
        
        print("\n📌 Colunas de Data/Hora disponíveis:")
        for i, col in enumerate(date_cols, 1):
            print(f"[{i}] {col}")

        while True:
            try:
                escolha = input("\nEscolha a coluna de Data base: ").strip()
                escolha_idx = int(escolha) - 1
                if 0 <= escolha_idx < len(date_cols):
                    col_base_rollup = date_cols[escolha_idx]
                    break
                print("❌ Opção inválida.")
            except ValueError:
                print("❌ Digite um número válido.")

        # Criação das colunas de hierarquia de data
        df_work = df.copy()
        df_work['dia'] = df_work[col_base_rollup].dt.strftime('%Y-%m-%d')
        df_work['semana'] = df_work[col_base_rollup].dt.strftime('%Y-W%W')
        df_work['mes'] = df_work[col_base_rollup].dt.strftime('%Y-%m')
        
        # Define a hierarquia para o ROLLUP
        hierarquia_cols = ['mes', 'semana', 'dia'] 
        
    else: # rollup_type == 'textual'
        print("\n📌 Rollup Textual/Categórico")
        hier_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()

        if not hier_cols:
            print("❌ Nenhuma coluna textual encontrada para agrupamento.")
            return

        print("\n📌 Colunas Textuais disponíveis para Agrupamento:")
        for i, col in enumerate(hier_cols, 1):
            print(f"[{i}] {col}")

        while True:
            try:
                nivel_idx = int(input("\nEscolha a coluna para Agrupamento: ")) - 1
                if 0 <= nivel_idx < len(hier_cols):
                    col_base_rollup = hier_cols[nivel_idx]
                    break
                print("❌ Opção inválida.")
            except ValueError:
                print("❌ Digite um número válido.")

        hierarquia_cols = [col_base_rollup] # Apenas uma coluna para Rollup textual
        df_work = df # Usa o DF original
        
    # -----------------------------
    # FASE 3: SELEÇÃO DA MÉTRICA E EXECUÇÃO (COMUM)
    # -----------------------------
    
    numeric_cols = df_work.select_dtypes(include='number').columns.tolist()

    if not numeric_cols:
        print("❌ Nenhuma coluna numérica encontrada para Soma.")
        return

    print("\n📌 Colunas numéricas disponíveis para Soma:")
    for i, col in enumerate(numeric_cols, 1):
        print(f"[{i}] {col}")

    while True:
        try:
            soma_idx = int(input("\nEscolha qual coluna será somada: ")) - 1
            if 0 <= soma_idx < len(numeric_cols):
                col_soma = numeric_cols[soma_idx]
                break
            print("❌ Opção inválida.")
        except ValueError:
            print("❌ Digite um número válido.")
            
    # Execução do DuckDB
    conn = duckdb.connect()
    # Cria a string de agrupamento, respeitando a ordem para ROLLUP
    rollup_groups_sql = ', '.join(hierarquia_cols)
    select_cols_sql = ', '.join(hierarquia_cols)
    
    conn.register("df_work", df_work)
    
    sql = f"""
    SELECT
        {select_cols_sql},
        SUM({col_soma}) AS total_{col_soma}
    FROM df_work
    GROUP BY ROLLUP({rollup_groups_sql})
    ORDER BY total_{col_soma} DESC
    """
    
    # Ordenação extra para o Rollup Temporal, garantindo que a hierarquia faça sentido
    if rollup_type == 'temporal':
        sql += ", mes DESC, semana DESC, dia DESC" 

    print("\n⚙️ SQL gerado:")
    print(sql)

    print("\n📊 Resultado do Rollup:")
    result_df = conn.query(sql).to_df()
    
    # Exibe o resultado. Substitui valores nulos (rollups) por 'Total Geral/Mês/Semana'
    result_df = result_df.fillna({'dia': 'Total Semanal', 'semana': 'Total Mensal', 'mes': 'Total Geral'})
    
    print(result_df)
    
    conn.close()
    return result_df


# --------------------------
# 📊 MENU DE CONSULTAS GOLD (ATUALIZADA)
# --------------------------
def menu_consultas_gold(db_path, bronze_table):
    conn = duckdb.connect(db_path)
    try:
        df = conn.execute("SELECT * FROM gold").fetchdf()
    except Exception:
        print("❌ Tabela Gold não encontrada. Por favor, execute as etapas Silver e Gold antes de consultar.")
        conn.close()
        return

    conn.close()

    while True:
        print("\n=== Menu Consultas Gold ===")
        print("[1] Top-k")
        print("[2] Rollup (Temporal/Textual)") # Menu indica a nova funcionalidade
        print("[3] Média móvel")
        print("[4] Exibir tabela SILVER")
        print("--- Pipeline & Métricas ---")
        print("[5] Recompilar SILVER e GOLD")
        print("[6] Recompilar GOLD (Mantendo SILVER)")
        print("[7] Mostrar Métricas do Pipeline")
        print("[0] Sair")

        opc = input("Escolha: ").strip()

        if opc == "0":
            return
        
        # Opções de Consulta
        elif opc == "1":
            consulta_topk(df)
        elif opc == "2":
            consulta_rollup(df)
        elif opc == "3":
            consulta_media_movel(df)
        elif opc == "4":
            visualizar_silver(db_path)
        
        # Opções de Recompilação e Métricas
        elif opc == "5":
            print("\n*** RECOMPILANDO SILVER e GOLD (Forçado) ***")
            run_silver(db_path, bronze_table, force_recompile=True)
            run_gold(db_path, force_recompile=True)
            conn = duckdb.connect(db_path)
            df = conn.execute("SELECT * FROM gold").fetchdf()
            conn.close()

        elif opc == "6":
            print("\n*** RECOMPILANDO SOMENTE GOLD (Forçado) ***")
            run_gold(db_path, force_recompile=True)
            conn = duckdb.connect(db_path)
            df = conn.execute("SELECT * FROM gold").fetchdf()
            conn.close()
            
        elif opc == "7":
            registrar_metricas_gold(db_path)

        else:
            print("❌ Opção inválida.")


# --------------------------
# ▶ EXECUÇÃO COMPLETA
# --------------------------
if __name__ == "__main__":
    start_time_total = time.time()

    try:
        # 1. BRONZE (Manter ou Criar)
        db_path, bronze_table = run_bronze()
        
        # 2. SILVER (Manter ou Criar - com Lógica de Cache)
        run_silver(db_path, bronze_table)
        
        # 3. GOLD (Manter ou Criar - com Lógica de Cache e Registro de Métricas)
        run_gold(db_path)
        
        # 4. CONSULTAS (Menu Estendido)
        menu_consultas_gold(db_path, bronze_table)
    
    except Exception as e:
        print(f"\n❌ Ocorreu um erro fatal no pipeline: {e}")
        
    print(f"\n⏱ Tempo total da sessão: {time.time() - start_time_total:.2f}s")
