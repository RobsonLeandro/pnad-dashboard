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
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────
#  CONEXÃO COM O BANCO
# ─────────────────────────────────────────────────

DB_DEFAULT = Path("dados_pnad_ce/pnad_ceara.db")
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

def badge_condicao(val):
    if val == "Ocupada":
        return "🟢 Ocupada"
    elif val == "Desocupada":
        return "🟡 Desocupada"
    return "⚪ Fora da força"

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
        st.error(f"Banco não encontrado:\n{db_path}")
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
#  CARREGAMENTO DOS DADOS
# ─────────────────────────────────────────────────

if "df" not in st.session_state or aplicar:
    filtros = dict(
        anos      = [str(a) for a in anos_sel],
        trimestres= [str(t) for t in tri_sel],
        sexos     = sexos_sel,
        racas     = racas_sel,
        situacao  = situacao_sel,
    )
    with st.spinner("Carregando dados..."):
        df = carregar_dados(con, filtros)
    st.session_state["df"] = df
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
    df["Condicao"]       = df.apply(condicao_ocupacao, axis=1)
    df["Periodo"]        = df["Ano"].astype(str) + " T" + df["Trimestre"].astype(str)

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

renda_media = df["Renda_Habitual"].dropna()
renda_media = renda_media[renda_media > 0].mean()
idade_media = df["Idade"].mean()
horas_media = df["Horas_Semana"].dropna().mean()
pct_formal  = (df["Carteira_Assinada"] == "Sim").sum() / max(len(df), 1) * 100

c1, c2, c3, c4 = st.columns(4)
c1.metric("REGISTROS FILTRADOS", f"{len(df):,}".replace(",", "."))
c2.metric("RENDA MÉDIA", f"R$ {renda_media:,.0f}".replace(",", ".") if pd.notna(renda_media) else "—")
c3.metric("IDADE MÉDIA", f"{idade_media:.1f} anos" if pd.notna(idade_media) else "—")
c4.metric("FORMALIDADE", f"{pct_formal:.1f}%" if pct_formal else "—")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────
#  ABAS
# ─────────────────────────────────────────────────

tab1 = st.tabs(["📋 Registros"])[0]

# ── ABA 1: TABELA DE REGISTROS ───────────────────

with tab1:
    total = len(df)
    st.caption(f"Total: **{total:,}** registros — role a tabela (horizontal e vertical) para navegar")

    exibir = df.copy()

    # Renomeia todas as colunas para nomes legíveis
    renomear = {
        # Identificação
        "Ano":                       "Ano",
        "Trimestre":                 "Trim.",
        "UF":                        "UF",
        "Capital":                   "Capital",
        "RM_RIDE":                   "RM/RIDE",
        "UPA":                       "UPA",
        "Estrato":                   "Estrato",
        "V1008":                     "Nº Seleção Domicílio",
        "V1014":                     "Painel",
        "V1016":                     "Nº Entrevista",
        "Situacao":                  "Situação Domicílio",
        "Area":                      "Tipo de Área",
        "V1027":                     "Peso s/ Calibração",
        "V1028":                     "Peso c/ Calibração",
        "V1029":                     "Projeção Geográfica",
        "V1033":                     "Projeção Sexo/Idade",
        "posest":                    "Domínio Projeção Geo.",
        "posest_sxi":                "Domínio Projeção S/I",
        # Moradores
        "Pessoas_Domicilio":         "Pessoas no Domicílio",
        "Sexo":                      "Sexo",
        "Idade":                     "Idade",
        "Cor_Raca":                  "Cor/Raça",
        # Educação
        "Curso_Anterior":            "Curso Mais Elevado",
        "Conclusao_Curso":           "Concluiu Curso",
        # Trab. principal — estrutura
        "Trab_Remunerado_Produtos":  "Trab. Rem. Produtos",
        "Posicao_Trabalho":          "Posição Trab. Principal",
        "V4013":                     "CNAE (código)",
        "Secao_CNAE":                "Seção CNAE",
        "V4015":                     "Trab. Não Remunerado",
        "V40151":                    "Não Remun. 1–5",
        "V401511":                   "1 a 5 Não Remun.",
        "V401512":                   "6 a 10 Não Remun.",
        "V4016":                     "Nº Empregados",
        "V40161":                    "1–5 Empregados",
        "V40162":                    "6–10 Empregados",
        "V40163":                    "11–50 Empregados",
        "V4017":                     "Tinha Sócio",
        "V40171":                    "Qtd. Sócios",
        "V401711":                   "1–5 Sócios",
        "V4018":                     "Total Pessoas Negócio",
        "V40181":                    "1–5 Pessoas Negócio",
        "V40182":                    "6–10 Pessoas Negócio",
        "V40183":                    "11–50 Pessoas Negócio",
        "CNPJ":                      "CNPJ Registrado",
        "V4020":                     "Tipo Local Negócio",
        "V4021":                     "Trab. no Estabelecimento",
        "Local_Trabalho":            "Local de Trabalho",
        "V4024":                     "Serv. Dom. +1 Domicílio",
        "Emp_Temporario":            "Emp. Temporário",
        "V4026":                     "Contratado pelo Resp.",
        "V4027":                     "Contratado por Interm.",
        "Servidor_Publico":          "Servidor Público",
        "Carteira_Assinada":         "Carteira Assinada",
        "Previdencia":               "Previdência",
        # Trab. principal — rendimento
        "V4033":                     "Rend. Hab. (aux.)",
        "V40331":                    "Recebia em Dinheiro Hab.",
        "Renda_Habitual":            "Renda Hab. (R$)",
        "Renda_Hab_Produtos":        "Renda Hab. Produtos (R$)",
        "V4034":                     "Rend. Mês Ref. (aux.)",
        "Renda_MesRef":              "Renda Mês Ref. (R$)",
        "Renda_MesRef_Produtos":     "Renda Mês Ref. Produtos (R$)",
        # Trab. principal — horas
        "Horas_Semana":              "Horas Normais/Sem",
        "Horas_Efetivas":            "Horas Efetivas/Sem",
        # Trab. secundário
        "Posicao_Trab_Sec":          "Posição Trab. Secundário",
        "V4044":                     "CNAE Trab. Secundário",
        "V4045":                     "Área Trab. Secundário",
        "CNPJ_Sec":                  "CNPJ Trab. Secundário",
        "Carteira_Sec":              "Carteira Trab. Secundário",
        "V4050":                     "Rend. Hab. Sec. (aux.)",
        "V40501":                    "Recebia Dinheiro Hab. Sec.",
        "Renda_Hab_Sec":             "Renda Hab. Secundário (R$)",
        "V4051":                     "Rend. Mês Ref. Sec. (aux.)",
        "V40511":                    "Recebeu Dinheiro Mês Sec.",
        "Renda_MesRef_Sec":          "Renda Mês Ref. Sec. (R$)",
        "Renda_MesRef_Produtos_Sec": "Renda Mês Ref. Prod. Sec. (R$)",
        # Outros trabalhos
        "Renda_MesRef_Outros":       "Renda Mês Ref. Outros (R$)",
        "Renda_MesRef_Outros_Produtos": "Renda Mês Ref. Outros Prod. (R$)",
        # Derivadas
        "Condicao":                  "Condição Ocupação",
        "Periodo":                   "Período",
    }
    exibir = exibir.rename(columns={k: v for k, v in renomear.items() if k in exibir.columns})

    # Formata colunas de renda
    for col_renda in [
        "Renda Hab. (R$)", "Renda Hab. Produtos (R$)",
        "Renda Mês Ref. (R$)", "Renda Mês Ref. Produtos (R$)",
        "Renda Hab. Secundário (R$)", "Renda Mês Ref. Sec. (R$)",
        "Renda Mês Ref. Prod. Sec. (R$)",
        "Renda Mês Ref. Outros (R$)", "Renda Mês Ref. Outros Prod. (R$)",
    ]:
        if col_renda in exibir.columns:
            exibir[col_renda] = exibir[col_renda].apply(
                lambda x: f"R$ {x:,.2f}".replace(",","X").replace(".",",").replace("X",".") 
                if pd.notna(x) and x > 0 else "—"
            )

    # Substitui None/NaN por "—" em todas as colunas
    for col in exibir.columns:
        exibir[col] = exibir[col].apply(
            lambda x: "—" if (x is None or (isinstance(x, float) and pd.isna(x))
                              or str(x).strip() in ["None","nan","NaN",""]) else x
        )

    st.dataframe(
        exibir,
        use_container_width=True,
        hide_index=True,
        height=560,
    )
    
    # Renda média por posição no trabalho
    rpos = (
        df[(df["Renda_Habitual"] > 0) & df["Posicao_Trabalho"].notna()]
        .groupby("Posicao_Trabalho")["Renda_Habitual"]
        .agg(["mean","count"])
        .reset_index()
    )
    rpos.columns = ["Posição","Renda Média","N"]
    rpos = rpos[rpos["N"] >= 30].sort_values("Renda Média", ascending=True)

    fig3 = px.bar(
        rpos, x="Renda Média", y="Posição", orientation="h",
        title="Renda habitual média por posição no trabalho (R$)",
        color="Renda Média",
        color_continuous_scale=["#E8E4D9","#1A1A2E"],
    )
    fig3.update_layout(
        plot_bgcolor="#FFFFFF", paper_bgcolor="#F7F6F2",
        font_family="IBM Plex Sans", title_font_size=13,
        margin=dict(t=40, b=20, l=10, r=10),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig3, use_container_width=True)

# ─────────────────────────────────────────────────
#  RODAPÉ
# ─────────────────────────────────────────────────

st.markdown("---")
st.caption("Fonte: IBGE — PNAD Contínua Trimestral · Microdados Ceará (UF 23) · Banco SQLite gerado por pnad_ceara.py")