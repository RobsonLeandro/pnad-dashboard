"""
pnad_app.py — Dashboard Streamlit · Microdados PNAD Contínua — Ceará
=====================================================================
Instale as dependências e rode:

    py -m pip install streamlit pandas plotly
    py -m streamlit run pnad_app.py

O banco pnad_ceara.db deve estar em dados_pnad_ce/pnad_ceara.db
(ou passe outro caminho na sidebar).
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────
#  CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────

st.set_page_config(
    page_title="PNAD Ceará",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────
#  CSS CUSTOMIZADO
# ─────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Fundo geral */
.stApp {
    background-color: #F7F6F2;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #1A1A2E;
    border-right: 2px solid #E8E4D9;
}
[data-testid="stSidebar"] * {
    color: #E8E4D9 !important;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stCheckbox label {
    color: #A0A0B8 !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-weight: 500;
}
[data-testid="stSidebar"] h1 {
    color: #FFFFFF !important;
    font-size: 1.1rem !important;
    font-weight: 600;
    letter-spacing: -0.01em;
}

/* Cards de métricas */
[data-testid="metric-container"] {
    background: #FFFFFF;
    border: 1px solid #E8E4D9;
    border-radius: 4px;
    padding: 1.2rem 1.4rem;
}
[data-testid="metric-container"] label {
    font-size: 0.68rem !important;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #888 !important;
    font-weight: 500;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-weight: 600;
    color: #1A1A2E !important;
    font-family: 'IBM Plex Mono', monospace;
}

/* Tabela */
[data-testid="stDataFrame"] {
    border: 1px solid #E8E4D9;
    border-radius: 4px;
    background: #FFFFFF;
}

/* Botão */
.stButton > button {
    background-color: #D93B2B;
    color: white;
    border: none;
    border-radius: 4px;
    font-weight: 600;
    font-size: 0.85rem;
    letter-spacing: 0.04em;
    width: 100%;
    padding: 0.65rem;
    transition: background 0.2s;
}
.stButton > button:hover {
    background-color: #B8301F;
    color: white;
}

/* Tabs */
[data-testid="stTabs"] [data-baseweb="tab"] {
    font-size: 0.8rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    font-weight: 500;
}

/* Título principal */
.main-title {
    font-size: 1.6rem;
    font-weight: 600;
    color: #1A1A2E;
    letter-spacing: -0.02em;
    margin-bottom: 0.1rem;
}
.main-subtitle {
    font-size: 0.78rem;
    color: #888;
    letter-spacing: 0.04em;
    margin-bottom: 1.5rem;
}

/* Badge de condição */
.badge-ocupada   { background:#D4EDDA; color:#155724; padding:2px 10px; border-radius:20px; font-size:0.75rem; font-weight:500; }
.badge-desocupada{ background:#FFF3CD; color:#856404; padding:2px 10px; border-radius:20px; font-size:0.75rem; font-weight:500; }
.badge-fora      { background:#E2E3E5; color:#383D41; padding:2px 10px; border-radius:20px; font-size:0.75rem; font-weight:500; }

/* Divider sidebar */
.sidebar-divider {
    border: none;
    border-top: 1px solid #2E2E4E;
    margin: 1rem 0;
}

/* Perfil sociodemográfico */
.perfil-section-title {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #1A1A2E;
    border-bottom: 2px solid #1A1A2E;
    padding-bottom: 0.4rem;
    margin-bottom: 0.8rem;
    margin-top: 1.2rem;
}

/* Estatísticas descritivas */
.estat-section-title {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #1A1A2E;
    border-bottom: 2px solid #D93B2B;
    padding-bottom: 0.4rem;
    margin-bottom: 0.8rem;
    margin-top: 1.2rem;
}

/* Análise gráfica / comparativa */
.graf-section-title {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #1A1A2E;
    border-bottom: 2px solid #2E6DA4;
    padding-bottom: 0.4rem;
    margin-bottom: 0.8rem;
    margin-top: 1.4rem;
}
.comp-section-title {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #1A1A2E;
    border-bottom: 2px solid #2A7A4B;
    padding-bottom: 0.4rem;
    margin-bottom: 0.8rem;
    margin-top: 1.4rem;
}
.insight-box {
    background: #F0F4FF;
    border-left: 3px solid #2E6DA4;
    padding: 0.7rem 1rem;
    border-radius: 0 4px 4px 0;
    font-size: 0.82rem;
    color: #1A1A2E;
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────
#  CONEXÃO COM O BANCO
# ─────────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent
DB_DEFAULT = BASE_DIR / "dados_pnad_ce" / "pnad_ceara.db"
TABELA     = "pnad_ce"

@st.cache_resource
def conectar(db_path: str):
    p = Path(db_path)
    if not p.exists():
        return None
    return sqlite3.connect(p, check_same_thread=False)

@st.cache_data(ttl=300)
def carregar_dados(_con, filtros: dict) -> pd.DataFrame:
    where = ["1=1"]
    params = []

    if filtros.get("anos"):
        ph = ",".join("?" * len(filtros["anos"]))
        where.append(f"Ano IN ({ph})")
        params += filtros["anos"]

    if filtros.get("trimestres"):
        ph = ",".join("?" * len(filtros["trimestres"]))
        where.append(f"Trimestre IN ({ph})")
        params += filtros["trimestres"]

    if filtros.get("sexos"):
        ph = ",".join("?" * len(filtros["sexos"]))
        where.append(f"V2007 IN ({ph})")
        params += filtros["sexos"]

    if filtros.get("racas"):
        ph = ",".join("?" * len(filtros["racas"]))
        where.append(f"V2010 IN ({ph})")
        params += filtros["racas"]

    if filtros.get("situacao") and filtros["situacao"] != "Todas":
        where.append("V1022 = ?")
        params.append(filtros["situacao"])

    sql = f"""
        SELECT
            Ano, Trimestre,
            UF, Capital, RM_RIDE, UPA, Estrato,
            V1008, V1014, V1016,
            V1022  AS Situacao,
            V1023  AS Area,
            V1027, V1028, V1029, V1033,
            posest, posest_sxi,
            V2001  AS Pessoas_Domicilio,
            V2007  AS Sexo,
            V2009  AS Idade,
            V2010  AS Cor_Raca,
            V3009A AS Curso_Anterior,
            V3014  AS Conclusao_Curso,
            V4002  AS Trab_Remunerado_Produtos,
            V4012  AS Posicao_Trabalho,
            V4013,
            V40132A AS Secao_CNAE,
            V4015, V40151, V401511, V401512,
            V4016, V40161, V40162, V40163,
            V4017, V40171, V401711,
            V4018, V40181, V40182, V40183,
            V4019  AS CNPJ,
            V4020, V4021,
            V4022  AS Local_Trabalho,
            V4024, V4025  AS Emp_Temporario,
            V4026, V4027,
            V4028  AS Servidor_Publico,
            V4029  AS Carteira_Assinada,
            V4032  AS Previdencia,
            V4033, V40331,
            V403312 AS Renda_Habitual,
            V403322 AS Renda_Hab_Produtos,
            V4034,
            V403412 AS Renda_MesRef,
            V403422 AS Renda_MesRef_Produtos,
            V4039  AS Horas_Semana,
            V4039C AS Horas_Efetivas,
            V4043  AS Posicao_Trab_Sec,
            V4044, V4045,
            V4046  AS CNPJ_Sec,
            V4048  AS Carteira_Sec,
            V4050, V40501,
            V405012 AS Renda_Hab_Sec,
            V4051, V40511,
            V405112 AS Renda_MesRef_Sec,
            V405122 AS Renda_MesRef_Produtos_Sec,
            V405912 AS Renda_MesRef_Outros,
            V405922 AS Renda_MesRef_Outros_Produtos
        FROM {TABELA}
        WHERE {' AND '.join(where)}
    """
    df = pd.read_sql(sql, _con, params=params)
    return df

@st.cache_data(ttl=300)
def periodos_disponiveis(_con):
    df = pd.read_sql(f"SELECT DISTINCT Ano, Trimestre FROM {TABELA} ORDER BY Ano, Trimestre", _con)
    return df

@st.cache_data(ttl=300)
def valores_unicos(_con, coluna):
    df = pd.read_sql(f"SELECT DISTINCT {coluna} FROM {TABELA} WHERE {coluna} IS NOT NULL ORDER BY {coluna}", _con)
    return df.iloc[:, 0].dropna().tolist()

# ─────────────────────────────────────────────────
#  QUERY AGREGADA PARA PERFIL — evita carregar tudo
# ─────────────────────────────────────────────────

@st.cache_data(ttl=300)
def carregar_perfil(_con, filtros: dict, coluna: str) -> pd.DataFrame:
    """Retorna distribuição de uma variável por período (Ano+Trimestre)."""
    where = ["1=1"]
    params = []

    if filtros.get("anos"):
        ph = ",".join("?" * len(filtros["anos"]))
        where.append(f"Ano IN ({ph})")
        params += filtros["anos"]

    if filtros.get("trimestres"):
        ph = ",".join("?" * len(filtros["trimestres"]))
        where.append(f"Trimestre IN ({ph})")
        params += filtros["trimestres"]

    if filtros.get("sexos"):
        ph = ",".join("?" * len(filtros["sexos"]))
        where.append(f"V2007 IN ({ph})")
        params += filtros["sexos"]

    if filtros.get("racas"):
        ph = ",".join("?" * len(filtros["racas"]))
        where.append(f"V2010 IN ({ph})")
        params += filtros["racas"]

    if filtros.get("situacao") and filtros["situacao"] != "Todas":
        where.append("V1022 = ?")
        params.append(filtros["situacao"])

    sql = f"""
        SELECT
            Ano || ' T' || Trimestre AS Periodo,
            TRIM({coluna}) AS Categoria,
            COUNT(*) AS N
        FROM {TABELA}
        WHERE {' AND '.join(where)}
          AND {coluna} IS NOT NULL
        GROUP BY Periodo, Categoria
        ORDER BY Periodo, N DESC
    """
    return pd.read_sql(sql, _con, params=params)


def montar_tabela_perfil(df_agg: pd.DataFrame) -> pd.DataFrame:
    """Pivot: linhas = categoria, colunas = período, valores = N (%)."""
    if df_agg.empty:
        return pd.DataFrame()

    periodos = df_agg["Periodo"].unique().tolist()
    categorias = df_agg["Categoria"].unique().tolist()

    totais = df_agg.groupby("Periodo")["N"].sum().to_dict()

    pivot = {}
    for _, row in df_agg.iterrows():
        cat = row["Categoria"]
        per = row["Periodo"]
        n   = row["N"]
        tot = totais.get(per, 1)
        pct = n / tot * 100
        pivot.setdefault(cat, {})[per] = f"{n:,}  ({pct:.1f}%)"

    result = []
    for cat in sorted(categorias, key=str):
        linha = {"Categoria": cat}
        for per in periodos:
            linha[per] = pivot.get(cat, {}).get(per, "—")
        result.append(linha)

    return pd.DataFrame(result)


# ─────────────────────────────────────────────────
#  FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────────

def condicao_ocupacao(row):
    pos = str(row.get("Posicao_Trabalho", "")).strip()
    if pos and pos not in ["", "nan", "None"]:
        return "Ocupada"
    renda = row.get("Renda_Habitual")
    if pd.notna(renda) and renda > 0:
        return "Ocupada"
    return "Fora da força"


def calcular_renda_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parte 1 — Constrói renda_total = r_princ + r_sec + r_outros

    r_princ  = Renda_MesRef      (V403412) + Renda_MesRef_Produtos   (V403422)
    r_sec    = Renda_MesRef_Sec  (V405112) + Renda_MesRef_Produtos_Sec (V405122)
    r_outros = Renda_MesRef_Outros (V405912) + Renda_MesRef_Outros_Produtos (V405922)

    Valores nulos são tratados como 0 apenas no somatório, preservando
    o nulo original nas colunas-fonte.
    """
    cols_renda = {
        "Renda_MesRef":               "V403412",
        "Renda_MesRef_Produtos":      "V403422",
        "Renda_MesRef_Sec":           "V405112",
        "Renda_MesRef_Produtos_Sec":  "V405122",
        "Renda_MesRef_Outros":        "V405912",
        "Renda_MesRef_Outros_Produtos": "V405922",
    }

    # garante que todas as colunas existam e sejam numéricas
    for col in cols_renda:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA

    r_princ  = df["Renda_MesRef"].fillna(0)  + df["Renda_MesRef_Produtos"].fillna(0)
    r_sec    = df["Renda_MesRef_Sec"].fillna(0) + df["Renda_MesRef_Produtos_Sec"].fillna(0)
    r_outros = df["Renda_MesRef_Outros"].fillna(0) + df["Renda_MesRef_Outros_Produtos"].fillna(0)

    renda_total = r_princ + r_sec + r_outros

    # Onde todas as fontes são nulas, mantém NaN (indivíduo sem rendimento algum)
    todas_nulas = (
        df["Renda_MesRef"].isna()
        & df["Renda_MesRef_Produtos"].isna()
        & df["Renda_MesRef_Sec"].isna()
        & df["Renda_MesRef_Produtos_Sec"].isna()
        & df["Renda_MesRef_Outros"].isna()
        & df["Renda_MesRef_Outros_Produtos"].isna()
    )
    renda_total[todas_nulas] = pd.NA

    df["Renda_Total"] = renda_total
    return df


def calcular_estatisticas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parte 2 — Estatísticas descritivas de Idade, Horas_Semana,
    Horas_Efetivas e Renda_Total, por período.

    Retorna um DataFrame pronto para exibição.
    """
    variaveis = {
        "Idade (V2009)":                    "Idade",
        "Horas Hab./Semana (V4039)":        "Horas_Semana",
        "Horas Efetivas/Semana (V4039C)":   "Horas_Efetivas",
        "Renda Total (R$)":                 "Renda_Total",
    }

    if "Periodo" not in df.columns:
        df["Periodo"] = df["Ano"].astype(str) + " T" + df["Trimestre"].astype(str)

    periodos = sorted(df["Periodo"].unique())
    linhas   = []

    for label, col in variaveis.items():
        if col not in df.columns:
            continue

        serie_global = pd.to_numeric(df[col], errors="coerce").dropna()
        # Para renda_total considera apenas valores > 0 nas estatísticas
        if col == "Renda_Total":
            serie_global = serie_global[serie_global > 0]

        linha = {
            "Variável":      label,
            "Período":       "GERAL",
            "N válidos":     len(serie_global),
            "Média":         serie_global.mean()   if len(serie_global) else None,
            "Mediana":       serie_global.median() if len(serie_global) else None,
            "Desvio-padrão": serie_global.std()    if len(serie_global) else None,
            "Q1 (25%)":      serie_global.quantile(0.25) if len(serie_global) else None,
            "Q3 (75%)":      serie_global.quantile(0.75) if len(serie_global) else None,
        }
        linhas.append(linha)

        for per in periodos:
            sub = pd.to_numeric(
                df.loc[df["Periodo"] == per, col], errors="coerce"
            ).dropna()
            if col == "Renda_Total":
                sub = sub[sub > 0]

            linhas.append({
                "Variável":      label,
                "Período":       per,
                "N válidos":     len(sub),
                "Média":         sub.mean()            if len(sub) else None,
                "Mediana":       sub.median()           if len(sub) else None,
                "Desvio-padrão": sub.std()              if len(sub) else None,
                "Q1 (25%)":      sub.quantile(0.25)    if len(sub) else None,
                "Q3 (75%)":      sub.quantile(0.75)    if len(sub) else None,
            })

    return pd.DataFrame(linhas)


def formatar_estat(df_estat: pd.DataFrame) -> pd.DataFrame:
    """Formata os números das estatísticas para exibição."""
    df = df_estat.copy()

    cols_reais = ["Média", "Mediana", "Desvio-padrão", "Q1 (25%)", "Q3 (75%)"]
    is_renda   = df["Variável"].str.contains("Renda")

    for col in cols_reais:
        df[col] = df.apply(
            lambda row: (
                f"R$ {row[col]:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                if pd.notna(row[col]) and "Renda" in row["Variável"]
                else (f"{row[col]:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                      if pd.notna(row[col])
                      else "—")
            ),
            axis=1,
        )

    df["N válidos"] = df["N válidos"].apply(
        lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "—"
    )

    return df


# ─────────────────────────────────────────────────
#  MAPA DE SEÇÕES CNAE (para CNAE Agrupado)
# ─────────────────────────────────────────────────

CNAE_SECOES = {
    "A": "Agricultura e Pecuária",
    "B": "Indústrias Extrativas",
    "C": "Indústrias de Transformação",
    "D": "Eletricidade e Gás",
    "E": "Água e Saneamento",
    "F": "Construção",
    "G": "Comércio e Reparação",
    "H": "Transporte e Armazenagem",
    "I": "Alojamento e Alimentação",
    "J": "Informação e Comunicação",
    "K": "Atividades Financeiras",
    "L": "Atividades Imobiliárias",
    "M": "Atividades Profissionais",
    "N": "Atividades Administrativas",
    "O": "Administração Pública",
    "P": "Educação",
    "Q": "Saúde e Serviços Sociais",
    "R": "Artes e Cultura",
    "S": "Outras Atividades de Serviços",
    "T": "Serviços Domésticos",
    "U": "Organismos Internacionais",
}

# Labels de nível de instrução (V3009A) — "Qual foi o curso mais elevado que
# ... frequentou anteriormente?" — tabela oficial do dicionário do IBGE
# (dicionario_PNADC_microdados_trimestral.xls). Os códigos vêm do banco como
# texto com zero à esquerda ("01", "02", ...), por isso as chaves aqui também
# precisam ser strings de 2 dígitos — se não bater exatamente, o código bruto
# aparece na tela em vez do nome (foi o que estava acontecendo antes).
INSTRUCAO_LABELS = {
    "01": "Creche",
    "02": "Pré-escola (maternal/jardim de infância)",
    "03": "Classe de alfabetização (CA)",
    "04": "Alfabetização de jovens e adultos",
    "05": "Antigo primário (elementar)",
    "06": "Antigo ginásio (médio 1º ciclo)",
    "07": "Fundamental regular (1º grau)",
    "08": "EJA / supletivo do Fundamental",
    "09": "Antigo científico/clássico (médio 2º ciclo)",
    "10": "Médio regular (2º grau)",
    "11": "EJA / supletivo do Médio",
    "12": "Superior — graduação",
    "13": "Especialização (pós-graduação lato sensu)",
    "14": "Mestrado",
    "15": "Doutorado",
}

# ─────────────────────────────────────────────────
#  FUNÇÕES — ANÁLISE GRÁFICA (PARTE 3)
# ─────────────────────────────────────────────────

CORES_PADRAO = px.colors.qualitative.Set2


def graf_histograma(df: pd.DataFrame, coluna: str, titulo: str, prefixo_x: str = "",
                     por_periodo: bool = False, bin_size: float = None,
                     corte_percentil: float = 99) -> go.Figure:
    dfx = df.copy()
    dfx[coluna] = pd.to_numeric(dfx[coluna], errors="coerce")
    if coluna == "Renda_Total":
        dfx = dfx[dfx[coluna] > 0]
    dfx = dfx.dropna(subset=[coluna])

    # ── Corta a cauda de valores extremos apenas para a VISUALIZAÇÃO ──
    # (os cálculos de média/mediana/etc. em outras partes do app usam os
    # dados completos — aqui só limitamos o eixo para o histograma não
    # virar "uma barra gigante + nada" por causa de poucos outliers.)
    limite_superior = None
    n_cortados = 0
    if len(dfx) > 0 and corte_percentil is not None:
        limite_superior = dfx[coluna].quantile(corte_percentil / 100)
        n_cortados = int((dfx[coluna] > limite_superior).sum())
        dfx = dfx[dfx[coluna] <= limite_superior]

    # ── Define o tamanho do bin (intervalo) ──
    if bin_size is None:
        # fallback: ~30 bins ao longo do intervalo visível
        amplitude = dfx[coluna].max() - dfx[coluna].min() if len(dfx) else 1
        bin_size = max(amplitude / 30, 1)

    xbins = dict(start=0, size=bin_size)

    aviso = ""
    if n_cortados > 0:
        aviso = (
            f' <span style="font-size:10px;color:#999;">'
            f'({n_cortados} valor(es) acima de {limite_superior:,.0f} '
            f'ocultado(s) do gráfico p/ melhor leitura)</span>'
        ).replace(",", ".")

    # ── Modo separado por período: um histograma por Ano+Trimestre ──
    if por_periodo and "Periodo" in dfx.columns and dfx["Periodo"].nunique() > 1:
        periodos = sorted(dfx["Periodo"].unique())
        n_periodos = len(periodos)
        n_cols = min(3, n_periodos)

        fig = px.histogram(
            dfx,
            x=coluna,
            facet_col="Periodo",
            facet_col_wrap=n_cols,
            category_orders={"Periodo": periodos},
            title=titulo + aviso,
            labels={coluna: prefixo_x or coluna, "count": "Frequência"},
            color_discrete_sequence=["#2E6DA4"],
            opacity=0.85,
        )
        fig.update_traces(xbins=xbins)
        # Limpa os títulos de cada subplot (ex.: "Periodo=2023 T1" -> "2023 T1")
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1], font_size=11))
        fig.update_yaxes(matches=None, showticklabels=True, gridcolor="#F0F0F0")
        fig.update_xaxes(showgrid=False, range=[0, limite_superior] if limite_superior else None)
        fig.update_layout(
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            font_family="IBM Plex Sans",
            title_font_size=13,
            showlegend=False,
            bargap=0.08,
            margin=dict(t=60, b=40, l=40, r=20),
            height=260 * ((n_periodos - 1) // n_cols + 1),
        )
        return fig

    # ── Modo agregado (todos os períodos juntos) ──
    fig = px.histogram(
        dfx,
        x=coluna,
        title=titulo + aviso,
        labels={coluna: prefixo_x or coluna, "count": "Frequência"},
        color_discrete_sequence=["#2E6DA4"],
        opacity=0.85,
    )
    fig.update_traces(xbins=xbins)
    fig.update_layout(
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        font_family="IBM Plex Sans",
        title_font_size=13,
        showlegend=False,
        bargap=0.05,
        xaxis=dict(showgrid=False, range=[0, limite_superior] if limite_superior else None),
        yaxis=dict(gridcolor="#F0F0F0"),
        margin=dict(t=50, b=40, l=40, r=20),
    )
    return fig


def graf_barras_categorico(df: pd.DataFrame, coluna: str, titulo: str,
                            mapa_labels: dict = None, top_n: int = 15,
                            por_periodo: bool = False) -> go.Figure:
    dfx = df[[coluna] + (["Periodo"] if "Periodo" in df.columns else [])].copy()
    dfx[coluna] = dfx[coluna].astype(str).str.strip()
    dfx = dfx.dropna(subset=[coluna])
    if mapa_labels:
        dfx[coluna] = dfx[coluna].map(mapa_labels).fillna(dfx[coluna])

    # ── Modo separado por período: barras agrupadas, cor = período ──
    if por_periodo and "Periodo" in dfx.columns and dfx["Periodo"].nunique() > 1:
        contagem = dfx.groupby(["Periodo", coluna]).size().reset_index(name="N")

        # mantém apenas as top_n categorias mais frequentes no total
        top_cats = (
            contagem.groupby(coluna)["N"].sum()
            .sort_values(ascending=False)
            .head(top_n)
            .index
        )
        contagem = contagem[contagem[coluna].isin(top_cats)]

        # ordena categorias pelo total geral (maior primeiro)
        ordem_cat = (
            contagem.groupby(coluna)["N"].sum()
            .sort_values(ascending=False)
            .index.tolist()
        )
        periodos = sorted(contagem["Periodo"].unique())

        fig = px.bar(
            contagem,
            x="N",
            y=coluna,
            color="Periodo",
            orientation="h",
            barmode="group",
            title=titulo,
            labels={"N": "Quantidade", coluna: ""},
            category_orders={coluna: ordem_cat, "Periodo": periodos},
            color_discrete_sequence=CORES_PADRAO,
        )
        fig.update_layout(
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            font_family="IBM Plex Sans",
            title_font_size=13,
            yaxis=dict(autorange="reversed"),
            xaxis=dict(gridcolor="#F0F0F0"),
            margin=dict(t=50, b=40, l=180, r=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, title=""),
            height=max(400, 40 * len(ordem_cat) + 120),
        )
        return fig

    # ── Modo agregado (todos os períodos juntos) ──
    contagem = dfx[coluna].value_counts().head(top_n).reset_index()
    contagem.columns = ["Categoria", "N"]

    fig = px.bar(
        contagem,
        x="N",
        y="Categoria",
        orientation="h",
        title=titulo,
        labels={"N": "Quantidade", "Categoria": ""},
        color="N",
        color_continuous_scale=["#C8DDEF", "#2E6DA4"],
    )
    fig.update_layout(
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        font_family="IBM Plex Sans",
        title_font_size=13,
        coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"),
        xaxis=dict(gridcolor="#F0F0F0"),
        margin=dict(t=50, b=40, l=180, r=20),
    )
    return fig


def insight_renda(df):
    s = pd.to_numeric(df["Renda_Total"], errors="coerce")
    s = s[s > 0].dropna()
    if s.empty:
        return "Sem dados de renda para o período selecionado."
    return (
        f"A renda total apresenta média de R$ {s.mean():,.2f} e mediana de R$ {s.median():,.2f}, "
        f"evidenciando assimetria à direita típica de distribuições de renda. "
        f"O desvio-padrão de R$ {s.std():,.2f} indica alta dispersão entre os trabalhadores."
    ).replace(",", "X").replace(".", ",").replace("X", ".")


def insight_idade(df):
    s = pd.to_numeric(df["Idade"], errors="coerce").dropna()
    if s.empty:
        return "Sem dados de idade disponíveis."
    return (
        f"A distribuição etária concentra-se entre {int(s.quantile(0.25))} e {int(s.quantile(0.75))} anos "
        f"(intervalo interquartil), com média de {s.mean():.1f} anos e mediana de {s.median():.1f} anos."
    )


def insight_instrucao(df):
    col = "Curso_Anterior"
    if col not in df.columns:
        return ""
    contagem = df[col].dropna().astype(str).str.strip()
    contagem = contagem.map(INSTRUCAO_LABELS).fillna(contagem).value_counts()
    if contagem.empty:
        return ""
    moda = contagem.index[0]
    pct  = contagem.iloc[0] / contagem.sum() * 100
    return f"O nível de instrução mais frequente é '{moda}', representando {pct:.1f}% dos registros com dado informado."


def insight_cnae(df):
    col = "Secao_CNAE"
    if col not in df.columns:
        return ""
    contagem = df[col].dropna().astype(str).str.strip()
    contagem = contagem.map(CNAE_SECOES).fillna(contagem).value_counts()
    if contagem.empty:
        return ""
    top = contagem.index[0]
    pct = contagem.iloc[0] / contagem.sum() * 100
    return f"O setor com maior concentração de trabalhadores é '{top}', com {pct:.1f}% dos ocupados."


# ─────────────────────────────────────────────────
#  FUNÇÕES — ANÁLISE COMPARATIVA (PARTE 4 — A e B)
# ─────────────────────────────────────────────────

def comp_sexo(df: pd.DataFrame):
    """A) Comparativo por Sexo: idade média, renda média e mediana da renda."""
    col_sexo  = "Sexo"
    col_renda = "Renda_Total"
    col_idade = "Idade"

    if col_sexo not in df.columns:
        return None, None, None

    df_work = df[[col_sexo, col_renda, col_idade]].copy()
    df_work[col_renda] = pd.to_numeric(df_work[col_renda], errors="coerce")
    df_work[col_idade] = pd.to_numeric(df_work[col_idade], errors="coerce")
    df_work = df_work[df_work[col_sexo].notna()]
    df_work[col_sexo] = df_work[col_sexo].astype(str).str.strip()

    # Tabela resumo
    grupos = df_work.groupby(col_sexo)
    renda_pos = df_work[df_work[col_renda] > 0].groupby(col_sexo)[col_renda]

    resumo = pd.DataFrame({
        "Sexo":           grupos[col_sexo].count().index,
        "N":              grupos[col_sexo].count().values,
        "Idade Média":    grupos[col_idade].mean().values,
        "Renda Média (R$)":   renda_pos.mean().reindex(grupos[col_sexo].count().index).values,
        "Mediana Renda (R$)": renda_pos.median().reindex(grupos[col_sexo].count().index).values,
    })

    # Gráfico 1 — idade média por sexo
    fig_idade = px.bar(
        resumo, x="Sexo", y="Idade Média",
        title="Idade Média por Sexo",
        color="Sexo",
        color_discrete_sequence=["#2E6DA4", "#D93B2B"],
        text_auto=".1f",
    )
    fig_idade.update_layout(
        plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
        font_family="IBM Plex Sans", showlegend=False,
        yaxis=dict(gridcolor="#F0F0F0"), xaxis=dict(showgrid=False),
        margin=dict(t=50, b=40, l=40, r=20),
    )

    # Gráfico 2 — renda média e mediana por sexo (grouped)
    df_renda_long = resumo.melt(
        id_vars="Sexo",
        value_vars=["Renda Média (R$)", "Mediana Renda (R$)"],
        var_name="Métrica", value_name="Valor",
    )
    fig_renda = px.bar(
        df_renda_long, x="Sexo", y="Valor", color="Métrica",
        barmode="group",
        title="Renda Média e Mediana por Sexo (R$)",
        color_discrete_sequence=["#2E6DA4", "#F4A53A"],
        text_auto=",.0f",
    )
    fig_renda.update_layout(
        plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
        font_family="IBM Plex Sans",
        yaxis=dict(gridcolor="#F0F0F0"), xaxis=dict(showgrid=False),
        margin=dict(t=50, b=40, l=40, r=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )

    return resumo, fig_idade, fig_renda


def comp_escolaridade(df: pd.DataFrame):
    """B) Comparativo por Escolaridade (V3009A): renda média e mediana."""
    col_instr = "Curso_Anterior"
    col_renda = "Renda_Total"

    if col_instr not in df.columns:
        return None, None

    df_work = df[[col_instr, col_renda]].copy()
    df_work[col_renda] = pd.to_numeric(df_work[col_renda], errors="coerce")
    df_work = df_work[df_work[col_instr].notna() & (df_work[col_renda] > 0)]
    df_work[col_instr] = (
        df_work[col_instr].astype(str).str.strip()
        .map(INSTRUCAO_LABELS)
        .fillna(df_work[col_instr].astype(str).str.strip())
    )

    # Ordem natural de escolaridade
    ordem = list(INSTRUCAO_LABELS.values())
    presentes = [o for o in ordem if o in df_work[col_instr].unique()]
    outros    = [v for v in df_work[col_instr].unique() if v not in presentes]
    ordem_final = presentes + sorted(outros)

    grupos = df_work.groupby(col_instr)[col_renda]
    resumo = pd.DataFrame({
        "Escolaridade":        grupos.mean().index,
        "N":                   df_work.groupby(col_instr)[col_renda].count().values,
        "Renda Média (R$)":    grupos.mean().values,
        "Mediana Renda (R$)":  grupos.median().values,
    })
    resumo["Escolaridade"] = pd.Categorical(resumo["Escolaridade"], categories=ordem_final, ordered=True)
    resumo = resumo.sort_values("Escolaridade")

    # Gráfico — renda média e mediana por escolaridade
    df_long = resumo.melt(
        id_vars="Escolaridade",
        value_vars=["Renda Média (R$)", "Mediana Renda (R$)"],
        var_name="Métrica", value_name="Valor",
    )
    fig = px.bar(
        df_long, x="Valor", y="Escolaridade",
        color="Métrica", barmode="group",
        orientation="h",
        title="Renda Média e Mediana por Nível de Instrução (R$)",
        color_discrete_sequence=["#2E6DA4", "#F4A53A"],
    )
    fig.update_layout(
        plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
        font_family="IBM Plex Sans",
        yaxis=dict(autorange="reversed", categoryorder="array", categoryarray=ordem_final),
        xaxis=dict(gridcolor="#F0F0F0"),
        margin=dict(t=50, b=40, l=200, r=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=420,
    )

    return resumo, fig


# ─────────────────────────────────────────────────
#  SIDEBAR — FILTROS
# ─────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📊 PNAD Ceará")
    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    db_path = st.text_input(
        "CAMINHO DO BANCO",
        value=str(DB_DEFAULT),
        help="Caminho para o arquivo pnad_ceara.db"
    )

    con = conectar(db_path)

    if con is None:
        caminho_absoluto = Path(db_path).resolve()
        st.error(
            f"Banco não encontrado.\n\n"
            f"Caminho informado: `{db_path}`\n\n"
            f"Caminho absoluto verificado: `{caminho_absoluto}`\n\n"
            f"Rode primeiro o `pnad_ceara.py` (ele cria a pasta "
            f"`dados_pnad_ce/` ao lado dele mesmo), ou cole aqui o caminho "
            f"completo do arquivo `pnad_ceara.db`."
        )
        st.stop()

    periodos = periodos_disponiveis(con)
    anos_disp = sorted(periodos["Ano"].unique().tolist())
    tri_disp  = sorted(periodos["Trimestre"].unique().tolist())

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    anos_sel = st.multiselect(
        "ANO",
        options=anos_disp,
        default=anos_disp[:1] if anos_disp else [],
    )

    tri_map  = {1:"1º Trimestre", 2:"2º Trimestre", 3:"3º Trimestre", 4:"4º Trimestre"}
    tri_opts = [tri_map.get(t, str(t)) for t in tri_disp]
    tri_sel_labels = st.multiselect(
        "TRIMESTRE",
        options=tri_opts,
        default=tri_opts[:1] if tri_opts else [],
    )
    tri_inv  = {v: k for k, v in tri_map.items()}
    tri_sel  = [tri_inv.get(l, l) for l in tri_sel_labels]

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    sexos_disp = valores_unicos(con, "V2007")
    sexos_sel  = st.multiselect("SEXO", options=sexos_disp, default=sexos_disp)

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    racas_disp = valores_unicos(con, "V2010")
    racas_sel  = st.multiselect(
        "COR OU RAÇA",
        options=racas_disp,
        default=racas_disp
    )

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    situacao_disp = ["Todas"] + valores_unicos(con, "V1022")
    situacao_sel  = st.selectbox("SITUAÇÃO DO DOMICÍLIO", options=situacao_disp)

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
    aplicar = st.button("Aplicar filtros")

# ─────────────────────────────────────────────────
#  MONTAGEM DOS FILTROS (compartilhados entre abas)
# ─────────────────────────────────────────────────

filtros = dict(
    anos      = [str(a) for a in anos_sel],
    trimestres= [str(t) for t in tri_sel],
    sexos     = sexos_sel,
    racas     = racas_sel,
    situacao  = situacao_sel,
)

# ─────────────────────────────────────────────────
#  CARREGAMENTO DOS DADOS (aba Registros)
# ─────────────────────────────────────────────────

if "df" not in st.session_state or aplicar:
    with st.spinner("Carregando dados..."):
        df = carregar_dados(con, filtros)
    st.session_state["df"] = df
    st.session_state["filtros_ativos"] = filtros
else:
    df = st.session_state["df"]

# ─────────────────────────────────────────────────
#  PROCESSAMENTO
# ─────────────────────────────────────────────────

if not df.empty:
    df["Renda_Habitual"] = pd.to_numeric(df["Renda_Habitual"], errors="coerce")
    df["Renda_MesRef"]   = pd.to_numeric(df["Renda_MesRef"],   errors="coerce")
    df["Idade"]          = pd.to_numeric(df["Idade"],           errors="coerce")
    df["Horas_Semana"]   = pd.to_numeric(df["Horas_Semana"],    errors="coerce")
    df["Horas_Efetivas"] = pd.to_numeric(df["Horas_Efetivas"],  errors="coerce")
    df["Condicao"]       = df.apply(condicao_ocupacao, axis=1)
    df["Periodo"]        = df["Ano"].astype(str) + " T" + df["Trimestre"].astype(str)

    # ── PARTE 1: calcula renda_total ──────────────
    df = calcular_renda_total(df)

# ─────────────────────────────────────────────────
#  CABEÇALHO PRINCIPAL
# ─────────────────────────────────────────────────

st.markdown('<p class="main-title">Microdados PNAD Contínua — Ceará</p>', unsafe_allow_html=True)
st.markdown('<p class="main-subtitle">Banco longitudinal 2023–2025 · Fonte: IBGE</p>', unsafe_allow_html=True)

if df.empty:
    st.warning("Nenhum registro encontrado com os filtros selecionados.")
    st.stop()

# ─────────────────────────────────────────────────
#  MÉTRICAS
# ─────────────────────────────────────────────────

renda_media  = df["Renda_Habitual"].dropna()
renda_media  = renda_media[renda_media > 0].mean()
idade_media  = df["Idade"].mean()
horas_media  = df["Horas_Semana"].dropna().mean()

c1, c2, c3 = st.columns(3)
c1.metric("REGISTROS FILTRADOS", f"{len(df):,}".replace(",", "."))
c2.metric("RENDA MÉDIA", f"R$ {renda_media:,.0f}".replace(",", ".") if pd.notna(renda_media) else "—")
c3.metric("IDADE MÉDIA", f"{idade_media:.1f} anos" if pd.notna(idade_media) else "—")


st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────
#  ABAS
# ─────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 Registros",
    "👥 Perfil Sociodemográfico",
    "📈 Estatísticas Descritivas",
    "📊 Análise Gráfica",
    "🔍 Análise Comparativa",
])

# ── ABA 1: TABELA DE REGISTROS ───────────────────

with tab1:
    total = len(df)
    st.caption(f"Total: **{total:,}** registros — role a tabela (horizontal e vertical) para navegar")

    exibir = df.copy()

    renomear = {
        "Ano":"Ano","Trimestre":"Trim.","UF":"UF","Capital":"Capital","RM_RIDE":"RM/RIDE",
        "UPA":"UPA","Estrato":"Estrato","V1008":"Nº Seleção Domicílio","V1014":"Painel",
        "V1016":"Nº Entrevista","Situacao":"Situação Domicílio","Area":"Tipo de Área",
        "V1027":"Peso s/ Calibração","V1028":"Peso c/ Calibração","V1029":"Projeção Geográfica",
        "V1033":"Projeção Sexo/Idade","posest":"Domínio Projeção Geo.","posest_sxi":"Domínio Projeção S/I",
        "Pessoas_Domicilio":"Pessoas no Domicílio","Sexo":"Sexo","Idade":"Idade","Cor_Raca":"Cor/Raça",
        "Curso_Anterior":"Curso Mais Elevado","Conclusao_Curso":"Concluiu Curso",
        "Trab_Remunerado_Produtos":"Trab. Rem. Produtos","Posicao_Trabalho":"Posição Trab. Principal",
        "V4013":"CNAE (código)","Secao_CNAE":"Seção CNAE","V4015":"Trab. Não Remunerado",
        "V40151":"Não Remun. 1–5","V401511":"1 a 5 Não Remun.","V401512":"6 a 10 Não Remun.",
        "V4016":"Nº Empregados","V40161":"1–5 Empregados","V40162":"6–10 Empregados",
        "V40163":"11–50 Empregados","V4017":"Tinha Sócio","V40171":"Qtd. Sócios","V401711":"1–5 Sócios",
        "V4018":"Total Pessoas Negócio","V40181":"1–5 Pessoas Negócio","V40182":"6–10 Pessoas Negócio",
        "V40183":"11–50 Pessoas Negócio","CNPJ":"CNPJ Registrado","V4020":"Tipo Local Negócio",
        "V4021":"Trab. no Estabelecimento","Local_Trabalho":"Local de Trabalho",
        "V4024":"Serv. Dom. +1 Domicílio","Emp_Temporario":"Emp. Temporário",
        "V4026":"Contratado pelo Resp.","V4027":"Contratado por Interm.",
        "Servidor_Publico":"Servidor Público","Carteira_Assinada":"Carteira Assinada","Previdencia":"Previdência",
        "V4033":"Rend. Hab. (aux.)","V40331":"Recebia em Dinheiro Hab.",
        "Renda_Habitual":"Renda Hab. (R$)","Renda_Hab_Produtos":"Renda Hab. Produtos (R$)",
        "V4034":"Rend. Mês Ref. (aux.)","Renda_MesRef":"Renda Mês Ref. (R$)",
        "Renda_MesRef_Produtos":"Renda Mês Ref. Produtos (R$)","Horas_Semana":"Horas Normais/Sem",
        "Horas_Efetivas":"Horas Efetivas/Sem","Posicao_Trab_Sec":"Posição Trab. Secundário",
        "V4044":"CNAE Trab. Secundário","V4045":"Área Trab. Secundário","CNPJ_Sec":"CNPJ Trab. Secundário",
        "Carteira_Sec":"Carteira Trab. Secundário","V4050":"Rend. Hab. Sec. (aux.)",
        "V40501":"Recebia Dinheiro Hab. Sec.","Renda_Hab_Sec":"Renda Hab. Secundário (R$)",
        "V4051":"Rend. Mês Ref. Sec. (aux.)","V40511":"Recebeu Dinheiro Mês Sec.",
        "Renda_MesRef_Sec":"Renda Mês Ref. Sec. (R$)","Renda_MesRef_Produtos_Sec":"Renda Mês Ref. Prod. Sec. (R$)",
        "Renda_MesRef_Outros":"Renda Mês Ref. Outros (R$)","Renda_MesRef_Outros_Produtos":"Renda Mês Ref. Outros Prod. (R$)",
        "Condicao":"Condição Ocupação","Periodo":"Período",
        # ── PARTE 1: nova coluna ──────────────────
        "Renda_Total": "Renda Total (R$)",
    }
    exibir = exibir.rename(columns={k: v for k, v in renomear.items() if k in exibir.columns})

    for col_renda in [
        "Renda Hab. (R$)", "Renda Hab. Produtos (R$)",
        "Renda Mês Ref. (R$)", "Renda Mês Ref. Produtos (R$)",
        "Renda Hab. Secundário (R$)", "Renda Mês Ref. Sec. (R$)",
        "Renda Mês Ref. Prod. Sec. (R$)",
        "Renda Mês Ref. Outros (R$)", "Renda Mês Ref. Outros Prod. (R$)",
        "Renda Total (R$)",   # ← nova coluna incluída na formatação
    ]:
        if col_renda in exibir.columns:
            exibir[col_renda] = exibir[col_renda].apply(
                lambda x: f"R$ {x:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                if pd.notna(x) and x > 0 else "—"
            )

    for col in exibir.columns:
        exibir[col] = exibir[col].apply(
            lambda x: "—" if (x is None or (isinstance(x, float) and pd.isna(x))
                              or str(x).strip() in ["None","nan","NaN",""]) else x
        )

    # Reordena para garantir que Renda Total fique como última coluna
    cols_ordem = [c for c in exibir.columns if c != "Renda Total (R$)"] + ["Renda Total (R$)"]
    exibir = exibir[cols_ordem]

    st.dataframe(
        exibir,
        use_container_width=True,
        hide_index=True,
        height=560,
    )

# ── ABA 2: PERFIL SOCIODEMOGRÁFICO ───────────────

with tab2:
    st.caption("Distribuição longitudinal por período — contagem e percentual dentro de cada período")

    # Variáveis do perfil: (coluna_banco, rótulo amigável)
    PERFIL_VARS = [
        ("V2007",  "V2007 — Sexo"),
        ("V2010",  "V2010 — Cor ou Raça"),
        ("V1022",  "V1022 — Situação do Domicílio"),
        ("V4012",  "V4012 — Posição no Trabalho Principal"),
        ("V4029",  "V4029 — Carteira de Trabalho Assinada"),
        ("V4032",  "V4032 — Contribuinte de Previdência"),
    ]

    for coluna, titulo in PERFIL_VARS:
        st.markdown(f'<div class="perfil-section-title">{titulo}</div>', unsafe_allow_html=True)

        # Query agregada no banco — muito mais leve que processar o df completo
        df_agg = carregar_perfil(con, filtros, coluna)

        if df_agg.empty:
            st.caption("Sem dados para os filtros selecionados.")
            continue

        df_pivot = montar_tabela_perfil(df_agg)

        if df_pivot.empty:
            st.caption("Sem dados.")
            continue

        st.dataframe(
            df_pivot,
            use_container_width=True,
            hide_index=True,
            height=min(40 + len(df_pivot) * 38, 400),
        )

        st.markdown("<br>", unsafe_allow_html=True)

# ── ABA 3: ESTATÍSTICAS DESCRITIVAS ─────────────

with tab3:
    st.caption(
        "Medidas de tendência central, dispersão e posição para Idade, "
        "Horas trabalhadas e Renda Total — calculadas sobre os registros filtrados."
    )

    st.markdown('<div class="estat-section-title">Visão Geral + Por Período</div>', unsafe_allow_html=True)

    df_estat = calcular_estatisticas(df)

    if df_estat.empty:
        st.warning("Sem dados suficientes para calcular estatísticas.")
    else:
        df_fmt = formatar_estat(df_estat)

        # Exibe cada variável em uma seção separada
        for variavel in df_fmt["Variável"].unique():
            st.markdown(
                f'<div class="estat-section-title">{variavel}</div>',
                unsafe_allow_html=True,
            )

            sub = df_fmt[df_fmt["Variável"] == variavel].drop(columns=["Variável"])

            # Destaca a linha GERAL
            def highlight_geral(row):
                if row["Período"] == "GERAL":
                    return ["background-color: #F0F4FF; font-weight: 600"] * len(row)
                return [""] * len(row)

            st.dataframe(
                sub.style.apply(highlight_geral, axis=1),
                use_container_width=True,
                hide_index=True,
                height=min(40 + len(sub) * 38, 400),
            )

            st.markdown("<br>", unsafe_allow_html=True)

        # ── Tabela consolidada (todas as variáveis × período) ──
        st.markdown('<div class="estat-section-title">Tabela Consolidada — Médias por Período</div>', unsafe_allow_html=True)
        st.caption("Apenas as médias, para comparação rápida entre variáveis e períodos.")

        pivot_resumo = (
            df_estat[df_estat["Período"] != "GERAL"]
            .pivot_table(
                index="Variável",
                columns="Período",
                values="Média",
                aggfunc="first",
            )
            .reset_index()
        )

        # Formata valores da pivot
        for col in pivot_resumo.columns:
            if col == "Variável":
                continue
            pivot_resumo[col] = pivot_resumo.apply(
                lambda row: (
                    f"R$ {row[col]:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                    if pd.notna(row[col]) and "Renda" in str(row["Variável"])
                    else (f"{row[col]:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                          if pd.notna(row[col])
                          else "—")
                ),
                axis=1,
            )

        st.dataframe(
            pivot_resumo,
            use_container_width=True,
            hide_index=True,
        )

# ── ABA 4: ANÁLISE GRÁFICA ───────────────────────

with tab4:
    st.caption(
        "Distribuições e frequências das principais variáveis — "
        "calculadas sobre os registros filtrados na sidebar."
    )

    n_periodos_ativos = df["Periodo"].nunique() if "Periodo" in df.columns else 1

    col_toggle, _ = st.columns([1, 3])
    with col_toggle:
        por_periodo = st.checkbox(
            "📅 Separar gráficos por período (Ano/Trimestre)",
            value=True,
            disabled=(n_periodos_ativos <= 1),
            help="Mostra um painel por período em vez de agregar tudo junto. "
                 "Selecione mais de um Ano/Trimestre na sidebar para habilitar.",
        )
    if n_periodos_ativos <= 1:
        st.caption("Apenas um período selecionado na sidebar — selecione mais anos/trimestres para comparar.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 1. Histograma da Renda Total ─────────────
    st.markdown('<div class="graf-section-title">Histograma — Renda Total (renda_total)</div>', unsafe_allow_html=True)

    renda_valida = pd.to_numeric(df["Renda_Total"], errors="coerce")
    renda_valida = renda_valida[renda_valida > 0].dropna()

    if renda_valida.empty:
        st.caption("Sem dados de renda total para os filtros selecionados.")
    else:
        fig_h_renda = graf_histograma(
            df, "Renda_Total", "Distribuição da Renda Total (R$)", "Renda Total (R$)",
            por_periodo=por_periodo, bin_size=2000, corte_percentil=99,
        )
        st.plotly_chart(fig_h_renda, use_container_width=True)
        st.markdown(
            f'<div class="insight-box">💡 {insight_renda(df)}</div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 2. Histograma da Idade ────────────────────
    st.markdown('<div class="graf-section-title">Histograma — Idade (V2009)</div>', unsafe_allow_html=True)

    idade_valida = pd.to_numeric(df["Idade"], errors="coerce").dropna()

    if idade_valida.empty:
        st.caption("Sem dados de idade para os filtros selecionados.")
    else:
        fig_h_idade = graf_histograma(
            df, "Idade", "Distribuição da Idade (anos)", "Idade (anos)",
            por_periodo=por_periodo, bin_size=5, corte_percentil=100,
        )
        st.plotly_chart(fig_h_idade, use_container_width=True)
        st.markdown(
            f'<div class="insight-box">💡 {insight_idade(df)}</div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 3. Gráfico de barras — Nível de Instrução ─
    st.markdown('<div class="graf-section-title">Nível de Instrução — V3009A</div>', unsafe_allow_html=True)

    col_instr = "Curso_Anterior"
    if col_instr not in df.columns or df[col_instr].dropna().empty:
        st.caption("Variável V3009A não disponível nos dados filtrados.")
    else:
        fig_instr = graf_barras_categorico(
            df, col_instr,
            "Distribuição por Nível de Instrução (V3009A)",
            mapa_labels=INSTRUCAO_LABELS,
            por_periodo=por_periodo,
        )
        st.plotly_chart(fig_instr, use_container_width=True)
        txt = insight_instrucao(df)
        if txt:
            st.markdown(f'<div class="insight-box">💡 {txt}</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 4. Gráfico de barras — CNAE Agrupado ──────
    st.markdown('<div class="graf-section-title">Setor Econômico — CNAE Agrupado (V40132A)</div>', unsafe_allow_html=True)

    col_cnae = "Secao_CNAE"
    if col_cnae not in df.columns or df[col_cnae].dropna().empty:
        st.caption("Variável V40132A não disponível nos dados filtrados.")
    else:
        fig_cnae = graf_barras_categorico(
            df, col_cnae,
            "Distribuição por Seção CNAE (setor econômico)",
            mapa_labels=CNAE_SECOES,
            por_periodo=por_periodo,
        )
        st.plotly_chart(fig_cnae, use_container_width=True)
        txt = insight_cnae(df)
        if txt:
            st.markdown(f'<div class="insight-box">💡 {txt}</div>', unsafe_allow_html=True)

# ── ABA 5: ANÁLISE COMPARATIVA ────────────────────

with tab5:
    st.caption(
        "Comparações entre grupos sociodemográficos — "
        "A) por Sexo (V2007) e B) por Escolaridade (V3009A)."
    )

    # ── A) Comparativo por Sexo ───────────────────
    st.markdown('<div class="comp-section-title">A) Comparativo por Sexo (V2007)</div>', unsafe_allow_html=True)
    st.caption("Idade média, renda média e mediana da renda total — por sexo.")

    resumo_sexo, fig_idade_sexo, fig_renda_sexo = comp_sexo(df)

    if resumo_sexo is None:
        st.warning("Variável de sexo (V2007) não encontrada nos dados.")
    else:
        # Tabela resumo
        resumo_sexo_fmt = resumo_sexo.copy()
        resumo_sexo_fmt["N"] = resumo_sexo_fmt["N"].apply(lambda x: f"{int(x):,}".replace(",", "."))
        resumo_sexo_fmt["Idade Média"] = resumo_sexo_fmt["Idade Média"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "—"
        )
        for col_r in ["Renda Média (R$)", "Mediana Renda (R$)"]:
            resumo_sexo_fmt[col_r] = resumo_sexo_fmt[col_r].apply(
                lambda x: (
                    f"R$ {x:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                    if pd.notna(x) else "—"
                )
            )
        st.dataframe(resumo_sexo_fmt, use_container_width=True, hide_index=True)

        st.markdown("<br>", unsafe_allow_html=True)

        col_a1, col_a2 = st.columns(2)
        with col_a1:
            st.plotly_chart(fig_idade_sexo, use_container_width=True)
        with col_a2:
            st.plotly_chart(fig_renda_sexo, use_container_width=True)

        # Insight automático
        if len(resumo_sexo) == 2:
            grupos_sorted = resumo_sexo.sort_values("Renda Média (R$)", ascending=False)
            maior = grupos_sorted.iloc[0]
            menor = grupos_sorted.iloc[1]
            dif_pct = (maior["Renda Média (R$)"] - menor["Renda Média (R$)"]) / menor["Renda Média (R$)"] * 100
            txt_insight = (
                f"{maior['Sexo']} apresenta renda média superior em {dif_pct:.1f}% "
                f"em relação a {menor['Sexo']} "
                f"(R$ {maior['Renda Média (R$)']:,.2f} vs R$ {menor['Renda Média (R$)']:,.2f})."
            ).replace(",","X").replace(".",",").replace("X",".")
            st.markdown(f'<div class="insight-box">💡 {txt_insight}</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── B) Comparativo por Escolaridade ───────────
    st.markdown('<div class="comp-section-title">B) Comparativo por Escolaridade (V3009A)</div>', unsafe_allow_html=True)
    st.caption("Renda média e mediana da renda total — por nível de instrução, em ordem crescente de escolaridade.")

    resumo_esc, fig_esc = comp_escolaridade(df)

    if resumo_esc is None:
        st.warning("Variável de escolaridade (V3009A / Curso_Anterior) não encontrada nos dados.")
    else:
        # Tabela resumo
        resumo_esc_fmt = resumo_esc.copy()
        resumo_esc_fmt["N"] = resumo_esc_fmt["N"].apply(lambda x: f"{int(x):,}".replace(",", "."))
        for col_r in ["Renda Média (R$)", "Mediana Renda (R$)"]:
            resumo_esc_fmt[col_r] = resumo_esc_fmt[col_r].apply(
                lambda x: (
                    f"R$ {x:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                    if pd.notna(x) else "—"
                )
            )
        st.dataframe(resumo_esc_fmt, use_container_width=True, hide_index=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.plotly_chart(fig_esc, use_container_width=True)

        # Insight automático
        resumo_num = resumo_esc.dropna(subset=["Renda Média (R$)"])
        if not resumo_num.empty:
            menor_r = resumo_num.sort_values("Renda Média (R$)").iloc[0]
            maior_r = resumo_num.sort_values("Renda Média (R$)").iloc[-1]
            razao   = maior_r["Renda Média (R$)"] / menor_r["Renda Média (R$)"]
            txt_esc = (
                f"A renda média de quem possui '{maior_r['Escolaridade']}' é "
                f"{razao:.1f}× maior do que a de quem possui '{menor_r['Escolaridade']}', "
                f"evidenciando forte retorno financeiro à escolarização."
            )
            st.markdown(f'<div class="insight-box">💡 {txt_esc}</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────────
#  RODAPÉ
# ─────────────────────────────────────────────────

st.markdown("---")
st.caption("Fonte: IBGE — PNAD Contínua Trimestral · Microdados Ceará (UF 23) · Banco SQLite gerado por pnad_ceara.py")
