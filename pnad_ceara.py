"""
PNAD Contínua - Microdados do Ceará (2023–2025)
================================================
O arquivo .txt da PNAD é formato de LARGURA FIXA (FWF).
As posições das variáveis foram extraídas do dicionário oficial do IBGE:
  dicionario_PNADC_microdados_trimestral.xls

Melhorias:
  1. Leitura FWF com pd.read_fwf() em chunks → filtra UF=23 sem carregar tudo na RAM
  2. Limpeza automática → apaga ZIP e pasta bruta após cada trimestre
  3. Banco SQLite → armazenamento longitudinal consultável

Dependências:
    python -m pip install pandas requests tqdm

Uso:
    python pnad_ceara.py
"""

import sys
import sqlite3
import zipfile
import shutil
import requests
import pandas as pd
from pathlib import Path
from typing import Optional, List, Tuple
from tqdm import tqdm
from html.parser import HTMLParser

# ─────────────────────────────────────────────────
#  CONFIGURAÇÕES
# ─────────────────────────────────────────────────

ANO_INICIO = 2023
ANO_FIM    = 2025
TRIMESTRES = [1, 2, 3, 4]

UF_CEARA = "23"

PASTA_SAIDA = Path("dados_pnad_ce")
DB_PATH     = PASTA_SAIDA / "pnad_ceara.db"
TABELA_DB   = "pnad_ce"
CHUNK_SIZE  = 100_000

BASE_FTP = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento"
    "/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua"
    "/Trimestral/Microdados"
)

# ─────────────────────────────────────────────────
#  POSIÇÕES FWF — extraídas do dicionário oficial IBGE
#  dicionario_PNADC_microdados_trimestral.xls
#
#  Formato: (inicio, fim) em base 0 (Python)
#  Posição SAS (base 1) → Python: inicio = pos_sas - 1
# ─────────────────────────────────────────────────

# Cada entrada: (nome_variavel, (inicio, fim))
SPECS_FWF: List[Tuple[str, Tuple[int, int]]] = [
    # ── Identificação e Controle ─────────────────
    ("Ano",        (0,    4)),    # Ano de referência
    ("Trimestre",  (4,    5)),    # Trimestre de referência
    ("UF",         (5,    7)),    # Unidade da Federação (23 = Ceará)
    ("Capital",    (7,    9)),    # Município da Capital
    ("RM_RIDE",    (9,   11)),    # Região Metropolitana e RIDE
    ("UPA",        (11,  20)),    # Unidade Primária de Amostragem
    ("Estrato",    (20,  27)),    # Estrato
    ("V1008",      (27,  29)),    # Número de seleção do domicílio
    ("V1014",      (29,  31)),    # Painel
    ("V1016",      (31,  32)),    # Número da entrevista no domicílio
    ("V1022",      (32,  33)),    # Situação do domicílio
    ("V1023",      (33,  34)),    # Tipo de área
    ("V1027",      (34,  49)),    # Peso do domicílio e das pessoas (sem calibração)
    ("V1028",      (49,  64)),    # Peso do domicílio e das pessoas (com calibração)
    ("V1029",      (64,  73)),    # Projeção da população por níveis geográficos
    ("V1033",      (73,  82)),    # Projeção da população por sexo e idade
    ("posest",     (82,  85)),    # Domínios de projeção geográficos
    ("posest_sxi", (85,  88)),    # Domínios de projeção por sexo e idade

    # ── Características dos Moradores ────────────
    ("V2001",      (88,  90)),    # Número de pessoas no domicílio
    ("V2007",      (94,  95)),    # Sexo
    ("V2009",      (103, 106)),   # Idade do morador na data de referência
    ("V2010",      (106, 107)),   # Cor ou raça

    # ── Educação ─────────────────────────────────
    ("V3009A",     (124, 126)),   # Curso mais elevado frequentado anteriormente
    ("V3014",      (134, 135)),   # Conclusão do curso anterior

    # ── Trabalho — Trabalho Principal ────────────
    ("V4002",      (136, 137)),   # Trabalho remunerado em produtos/mercadorias/moradia/alimentação
    ("V4012",      (155, 156)),   # Posição no trabalho principal (era empregado, conta própria etc.)
    ("V4013",      (157, 162)),   # Código da principal atividade (CNAE)
    ("V40132A",    (163, 164)),   # Seção da atividade (CNAE — letra)
    ("V4015",      (165, 166)),   # Teve trabalhador não remunerado no trabalho principal
    ("V40151",     (166, 167)),   # Quantos trabalhadores não remunerados (1–5)
    ("V401511",    (167, 168)),   # 1 a 5 trabalhadores não remunerados
    ("V401512",    (168, 170)),   # 6 a 10 trabalhadores não remunerados
    ("V4016",      (170, 171)),   # Número de empregados no negócio/empresa
    ("V40161",     (171, 172)),   # 1 a 5 empregados
    ("V40162",     (172, 174)),   # 6 a 10 empregados
    ("V40163",     (174, 176)),   # 11 a 50 empregados
    ("V4017",      (176, 177)),   # Tinha sócio que trabalhava no negócio
    ("V40171",     (177, 178)),   # Quantos sócios
    ("V401711",    (178, 179)),   # 1 a 5 sócios
    ("V4018",      (179, 180)),   # Total de pessoas no negócio (incluindo o entrevistado)
    ("V40181",     (180, 181)),   # 1 a 5 pessoas
    ("V40182",     (181, 183)),   # 6 a 10 pessoas
    ("V40183",     (183, 185)),   # 11 a 50 pessoas
    ("V4019",      (185, 186)),   # Negócio registrado no CNPJ
    ("V4020",      (186, 187)),   # Tipo de local onde funcionava o negócio
    ("V4021",      (187, 188)),   # Exercia o trabalho em estabelecimento do negócio
    ("V4022",      (188, 189)),   # Local onde exercia normalmente o trabalho
    ("V4024",      (189, 190)),   # Prestava serviço doméstico em mais de um domicílio
    ("V4025",      (190, 191)),   # Era contratado como empregado temporário
    ("V4026",      (191, 192)),   # Contratado somente pelo responsável do negócio
    ("V4027",      (192, 193)),   # Contratado somente por intermediário
    ("V4028",      (193, 194)),   # Servidor público estatutário
    ("V4029",      (194, 195)),   # Carteira de trabalho assinada
    ("V4032",      (195, 196)),   # Contribuinte de previdência (trabalho principal)
    ("V4033",      (196, 197)),   # Rendimento bruto mensal habitual — variável auxiliar
    ("V40331",     (197, 198)),   # Recebia rendimento/retirada em dinheiro (hab.)
    ("V403312",    (199, 207)),   # Valor em dinheiro do rendimento habitual (trabalho principal)
    ("V403322",    (209, 217)),   # Valor estimado em produtos/mercadorias — habitual (trab. princ.)
    ("V4034",      (219, 220)),   # Rendimento bruto mês de referência — variável auxiliar
    ("V403412",    (222, 230)),   # Valor em dinheiro — mês de referência (trabalho principal)
    ("V403422",    (232, 240)),   # Valor estimado em produtos/mercadorias — mês ref. (trab. princ.)
    ("V4039",      (240, 243)),   # Horas normalmente trabalhadas por semana (trab. principal)
    ("V4039C",     (243, 246)),   # Horas efetivamente trabalhadas na semana de ref. (trab. princ.)

    # ── Trabalho — Trabalho Secundário ───────────
    ("V4043",      (257, 258)),   # Posição no trabalho secundário
    ("V4044",      (259, 264)),   # Código da principal atividade — trabalho secundário
    ("V4045",      (264, 265)),   # Área do trabalho secundário
    ("V4046",      (265, 266)),   # Negócio secundário registrado no CNPJ
    ("V4048",      (267, 268)),   # Carteira assinada — trabalho secundário
    ("V4050",      (269, 270)),   # Rendimento bruto habitual — trabalho secundário (aux.)
    ("V40501",     (270, 271)),   # Recebia em dinheiro habitualmente — trab. secundário
    ("V405012",    (272, 280)),   # Valor em dinheiro do rendimento habitual — trab. secundário
    ("V4051",      (292, 293)),   # Rendimento bruto mês de referência — trab. secundário (aux.)
    ("V40511",     (293, 294)),   # Recebeu em dinheiro no mês de ref. — trab. secundário
    ("V405112",    (295, 303)),   # Valor em dinheiro — mês de ref. (trab. secundário)
    ("V405122",    (305, 313)),   # Valor estimado em produtos/mercadorias — mês ref. (trab. sec.)

    # ── Trabalho — Outros Trabalhos ──────────────
    ("V405912",    (347, 355)),   # Valor em dinheiro — mês de ref. (outros trabalhos)
    ("V405922",    (357, 365)),   # Valor estimado em produtos/mercadorias — mês ref. (outros trab.)
]

# Separar colspecs e nomes para o pd.read_fwf()
COLSPECS = [spec[1] for spec in SPECS_FWF]
NOMES    = [spec[0] for spec in SPECS_FWF]

# Variáveis numéricas (rendimentos, pesos, horas, idade)
VARS_NUMERICAS = [
    "V1027", "V1028", "V1029", "V1033",
    "V2009",
    "V403312", "V403322", "V403412", "V403422",
    "V405012", "V405112", "V405122",
    "V405912", "V405922",
    "V4039",   "V4039C",
]

# ─────────────────────────────────────────────────


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "a":
            for name, val in attrs:
                if name == "href" and val:
                    self.links.append(val)


# ── 1. DESCOBERTA DE URL ──────────────────────────

def descobrir_url(ano: int, trimestre: int) -> Optional[str]:
    """Descobre a URL exata do ZIP no FTP do IBGE."""
    pasta   = f"{BASE_FTP}/{ano}/"
    prefixo = f"PNADC_{trimestre:02d}{ano}"

    try:
        resp = requests.get(pasta, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [ERRO] Não foi possível acessar: {pasta}\n         {e}")
        return None

    parser = LinkParser()
    parser.feed(resp.text)

    for link in parser.links:
        nome = link.split("/")[-1]
        if nome.startswith(prefixo) and nome.endswith(".zip"):
            return link if link.startswith("http") else pasta + nome

    return None


# ── 2. DOWNLOAD ───────────────────────────────────

def baixar_arquivo(url: str, destino: Path) -> Optional[Path]:
    print(f"\n  [DOWNLOAD] {url.split('/')[-1]}")
    print("  Isso pode levar alguns minutos (~200 MB)...")
    try:
        resp = requests.get(url, stream=True, timeout=600)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [ERRO] Falha no download: {e}")
        return None

    tamanho = int(resp.headers.get("content-length", 0))
    destino.parent.mkdir(parents=True, exist_ok=True)

    with open(destino, "wb") as f, tqdm(
        total=tamanho, unit="B", unit_scale=True, desc=destino.name
    ) as barra:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)
            barra.update(len(chunk))

    print(f"  Download concluído: {destino.stat().st_size / 1e6:.1f} MB")
    return destino


# ── 3. EXTRAÇÃO ───────────────────────────────────

def extrair_zip(caminho_zip: Path, pasta_destino: Path) -> None:
    print(f"\n  [EXTRAÇÃO] {caminho_zip.name}")
    pasta_destino.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(caminho_zip, "r") as z:
        for membro in z.namelist():
            z.extract(membro, pasta_destino)
            caminho = pasta_destino / membro
            if caminho.is_file():
                print(f"    • {membro}  ({caminho.stat().st_size / 1e6:.1f} MB)")


# ── 4. LEITURA FWF + FILTRO CEARÁ ────────────────

def ler_microdados_ceara(pasta: Path) -> pd.DataFrame:
    """
    Lê o arquivo .txt de LARGURA FIXA (FWF) em chunks de 100k linhas.
    Filtra apenas registros do Ceará (UF == '23').
    O arquivo inteiro nunca é carregado na memória.
    """
    candidatos = list(pasta.rglob("*.txt"))
    if not candidatos:
        raise FileNotFoundError(f"Nenhum arquivo .txt em: {pasta}")

    arquivo = max(candidatos, key=lambda p: p.stat().st_size)
    print(f"\n  [LEITURA FWF] {arquivo.name}  ({arquivo.stat().st_size / 1e6:.1f} MB)")
    print(f"  Total de variáveis: {len(NOMES)}")
    print(f"  Chunks de {CHUNK_SIZE:,} linhas — filtrando UF={UF_CEARA} (Ceará)...")

    chunks_ce  = []
    total_lido = 0

    for chunk in pd.read_fwf(
        arquivo,
        colspecs=COLSPECS,
        names=NOMES,
        dtype=str,
        encoding="latin-1",
        chunksize=CHUNK_SIZE,
        header=None,
    ):
        total_lido += len(chunk)

        # Remove espaços do padding FWF e filtra Ceará
        chunk["UF"] = chunk["UF"].str.strip()
        ce = chunk[chunk["UF"] == UF_CEARA]

        if not ce.empty:
            chunks_ce.append(ce.copy())

    print(f"  Total de linhas lidas no arquivo: {total_lido:,}")

    if not chunks_ce:
        raise ValueError(
            f"Nenhum registro com UF='{UF_CEARA}' (Ceará) encontrado.\n"
            f"Verifique o arquivo extraído manualmente."
        )

    df = pd.concat(chunks_ce, ignore_index=True)
    print(f"  ✅ Ceará: {len(df):,} registros extraídos")
    return df


# ── 5. LABELS LEGÍVEIS ────────────────────────────

def aplicar_labels(df: pd.DataFrame) -> pd.DataFrame:
    mapa = {
        "V1022": {"1": "Urbana", "2": "Rural"},
        "V1023": {
            "1": "Capital", "2": "Resto da RM/RIDE sem Capital",
            "3": "Resto da UF sem RM/RIDE",
        },
        "V2007": {"1": "Homem", "2": "Mulher"},
        "V2010": {
            "1": "Branca", "2": "Preta",   "3": "Amarela",
            "4": "Parda",  "5": "Indígena", "9": "Ignorado",
        },
        "V4002": {
            "1": "Sim — produtos/mercadorias/moradia/alimentação",
            "2": "Não",
        },
        "V4012": {
            "1": "Empregado do setor privado",
            "2": "Empregado do setor público",
            "3": "Empregado doméstico",
            "4": "Conta própria",
            "5": "Empregador",
            "6": "Trabalhador familiar auxiliar",
        },
        "V4019": {"1": "Sim", "2": "Não"},
        "V4025": {"1": "Sim", "2": "Não"},
        "V4028": {"1": "Sim", "2": "Não"},
        "V4029": {"1": "Sim", "2": "Não"},
        "V4032": {"1": "Sim", "2": "Não"},
        "V4046": {"1": "Sim", "2": "Não"},
        "V4048": {"1": "Sim", "2": "Não"},
        "V3014": {"1": "Sim", "2": "Não"},
    }

    for col, labels in mapa.items():
        if col in df.columns:
            df[col] = df[col].str.strip().map(labels).fillna(df[col])

    # Conversão numérica
    for col in VARS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ── 6. SQLITE ─────────────────────────────────────

def gravar_sqlite(df: pd.DataFrame, db_path: Path, tabela: str) -> None:
    """
    Grava o DataFrame no banco SQLite.
    if_exists='append' adiciona sem apagar dados anteriores —
    permite construir o banco trimestre a trimestre.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    df.to_sql(tabela, con=con, if_exists="append", index=False)
    con.close()
    print(f"\n  [SQLITE] {len(df):,} registros gravados em {db_path.name}")


# ── 7. LIMPEZA ────────────────────────────────────

def limpar_arquivos_brutos(caminho_zip: Path, pasta_extraida: Path) -> None:
    """Remove ZIP (~200 MB) e pasta extraída (~1.6 GB) após gravar no banco."""
    if caminho_zip.exists():
        caminho_zip.unlink()
        print(f"  [LIMPEZA] ZIP removido: {caminho_zip.name}")
    if pasta_extraida.exists():
        shutil.rmtree(pasta_extraida, ignore_errors=True)
        print(f"  [LIMPEZA] Pasta removida: {pasta_extraida.name}")


# ── 8. CACHE ──────────────────────────────────────

def ja_processado(db_path: Path, tabela: str, ano: int, trimestre: int) -> bool:
    """Verifica se o período já está no banco — evita re-download."""
    if not db_path.exists():
        return False
    try:
        con = sqlite3.connect(db_path)
        n = pd.read_sql(
            f"SELECT COUNT(*) as n FROM {tabela} WHERE Ano=? AND Trimestre=?",
            con, params=(str(ano), str(trimestre))
        ).iloc[0, 0]
        con.close()
        return int(n) > 0
    except Exception:
        return False


# ── RESUMO FINAL ──────────────────────────────────

def resumo_banco(db_path: Path, tabela: str) -> None:
    print("\n" + "=" * 60)
    print("  RESUMO DO BANCO — pnad_ceara.db")
    print("=" * 60)

    con = sqlite3.connect(db_path)

    # Colunas reais do banco — evita erros se o banco tiver estrutura diferente
    colunas_banco = set(
        pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"].tolist()
    )

    def query_segura(titulo, sql):
        print(f"\n── {titulo}")
        try:
            print(pd.read_sql(sql, con).to_string(index=False))
        except Exception as e:
            print(f"  [AVISO] Coluna indisponível: {e}")

    total = pd.read_sql(f"SELECT COUNT(*) as n FROM {tabela}", con).iloc[0, 0]
    print(f"\n  Total de registros : {total:,}")
    print(f"  Total de colunas   : {len(colunas_banco)}")

    query_segura(
        "Registros por Ano/Trimestre ──────────────────────────",
        f"SELECT Ano, Trimestre, COUNT(*) as Registros FROM {tabela} GROUP BY Ano, Trimestre ORDER BY Ano, Trimestre",
    )
    query_segura(
        "Sexo (V2007) ─────────────────────────────────────────",
        f"SELECT V2007, COUNT(*) as Total FROM {tabela} GROUP BY V2007",
    )
    query_segura(
        "Cor ou Raça (V2010) ──────────────────────────────────",
        f"SELECT V2010, COUNT(*) as Total FROM {tabela} GROUP BY V2010 ORDER BY Total DESC",
    )
    query_segura(
        "Situação do Domicílio (V1022) ────────────────────────",
        f"SELECT V1022, COUNT(*) as Total FROM {tabela} GROUP BY V1022",
    )
    query_segura(
        "Posição no trabalho principal (V4012) ────────────────",
        f"SELECT V4012, COUNT(*) as Total FROM {tabela} WHERE V4012 IS NOT NULL GROUP BY V4012 ORDER BY Total DESC",
    )
    query_segura(
        "Rendimento habitual médio — trab. principal (V403312)",
        f"SELECT Ano, Trimestre, ROUND(AVG(CAST(V403312 AS FLOAT)), 2) as Renda_Habitual_Media FROM {tabela} WHERE V403312 IS NOT NULL GROUP BY Ano, Trimestre ORDER BY Ano, Trimestre",
    )
    query_segura(
        "Rendimento efetivo médio — trab. principal (V403412) ─",
        f"SELECT Ano, Trimestre, ROUND(AVG(CAST(V403412 AS FLOAT)), 2) as Renda_Efetiva_Media FROM {tabela} WHERE V403412 IS NOT NULL GROUP BY Ano, Trimestre ORDER BY Ano, Trimestre",
    )

    con.close()

def processar_periodo(
    ano: int,
    trimestre: int,
    pasta_saida: Path,
    db_path: Path,
    tabela: str,
) -> bool:

    print("\n" + "=" * 60)
    print(f"  PNAD Ceará  |  {ano}  |  Trimestre {trimestre}")
    print("=" * 60)

    # Cache: já está no banco?
    if ja_processado(db_path, tabela, ano, trimestre):
        print(f"\n  [CACHE] {ano} T{trimestre} já está no banco. Pulando.")
        return True

    # 1. Descobre URL no FTP
    print(f"\n  [BUSCA] Procurando arquivo no FTP do IBGE...")
    url = descobrir_url(ano, trimestre)
    if url is None:
        print(f"\n  [ERRO] Arquivo para {ano} T{trimestre} não encontrado.")
        print(f"         Verifique manualmente: {BASE_FTP}/{ano}/")
        return False
    print(f"  Encontrado: {url.split('/')[-1]}")

    # 2. Download
    nome_zip    = url.split("/")[-1]
    pasta_raw   = pasta_saida / "raw"
    caminho_zip = pasta_raw / nome_zip
    if baixar_arquivo(url, caminho_zip) is None:
        return False

    # 3. Extração
    pasta_extraida = pasta_raw / nome_zip.replace(".zip", "")
    extrair_zip(caminho_zip, pasta_extraida)

    # 4. Leitura FWF + filtro Ceará
    try:
        df = ler_microdados_ceara(pasta_extraida)
    except Exception as e:
        print(f"\n  [ERRO] Falha na leitura: {e}")
        limpar_arquivos_brutos(caminho_zip, pasta_extraida)
        return False

    # 5. Labels legíveis
    df = aplicar_labels(df)

    # 6. Grava no SQLite (as colunas Ano e Trimestre já vêm do FWF)
    gravar_sqlite(df, db_path, tabela)

    # 7. Limpa arquivos brutos — libera ~1.8 GB por trimestre
    limpar_arquivos_brutos(caminho_zip, pasta_extraida)

    return True


# ─────────────────────────────────────────────────
#  PONTO DE ENTRADA
# ─────────────────────────────────────────────────

if __name__ == "__main__":

    # ── Verificação de compatibilidade do banco ───────────────────────────
    # Se o banco existe mas foi gerado com colunas antigas (< 77 variáveis),
    # apaga automaticamente para forçar a recriação com as novas variáveis.
    COLUNAS_ESPERADAS = set(NOMES)  # 77 variáveis configuradas acima

    if DB_PATH.exists():
        try:
            _con = sqlite3.connect(DB_PATH)
            _info = pd.read_sql(f"PRAGMA table_info({TABELA_DB})", _con)
            _con.close()
            _colunas_banco = set(_info["name"].tolist())

            if not COLUNAS_ESPERADAS.issubset(_colunas_banco):
                faltando = COLUNAS_ESPERADAS - _colunas_banco
                print(f"\n{'='*60}")
                print(f"  [ATENÇÃO] Banco desatualizado detectado!")
                print(f"  Colunas faltando: {len(faltando)}")
                print(f"  Ex: {list(faltando)[:5]} ...")
                print(f"  O banco antigo será apagado e recriado do zero.")
                print(f"{'='*60}")
                DB_PATH.unlink()
                print(f"  ✅ Banco antigo removido: {DB_PATH.name}")
        except Exception:
            # Banco corrompido ou vazio → apaga também
            DB_PATH.unlink(missing_ok=True)
            print(f"  [AVISO] Banco inválido removido. Será recriado.")
    # ─────────────────────────────────────────────────────────────────────

    periodos = [
        (ano, tri)
        for ano in range(ANO_INICIO, ANO_FIM + 1)
        for tri in TRIMESTRES
    ]

    total = len(periodos)
    print(f"\n{'='*60}")
    print(f"  PNAD Contínua — Ceará (UF=23)")
    print(f"  Período: {ANO_INICIO} T1  →  {ANO_FIM} T{max(TRIMESTRES)}")
    print(f"  Total de trimestres: {total}")
    print(f"  Total de variáveis coletadas: {len(NOMES)}")
    print(f"  Banco de dados: {DB_PATH}")
    print(f"\n  Posições FWF configuradas:")
    for nome, (ini, fim) in SPECS_FWF:
        print(f"    {nome:15s} posição {ini+1:4d}–{fim:4d}  (largura {fim-ini})")
    print(f"{'='*60}")

    sucessos: List[str] = []
    falhas:   List[str] = []

    for idx, (ano, tri) in enumerate(periodos, 1):
        label = f"{ano} T{tri}"
        print(f"\n{'─'*60}")
        print(f"  [{idx}/{total}]  Processando {label}")
        print(f"{'─'*60}")

        ok = processar_periodo(
            ano=ano,
            trimestre=tri,
            pasta_saida=PASTA_SAIDA,
            db_path=DB_PATH,
            tabela=TABELA_DB,
        )

        if ok:
            sucessos.append(label)
        else:
            falhas.append(label)

    print(f"\n{'='*60}")
    print(f"  RELATÓRIO FINAL")
    print(f"{'='*60}")
    print(f"  ✅ Sucesso ({len(sucessos)}): {', '.join(sucessos) if sucessos else '—'}")
    print(f"  ❌ Falha   ({len(falhas)}): {', '.join(falhas)   if falhas   else '—'}")

    if DB_PATH.exists():
        resumo_banco(DB_PATH, TABELA_DB)
    else:
        print("\n  ❌ Nenhum dado foi gravado no banco.")
        sys.exit(1)
