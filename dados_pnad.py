"""
pnad_visualizar.py — Visualização longitudinal das variáveis PNAD no terminal
==============================================================================
Exibe todas as variáveis do banco pnad_ceara.db lado a lado por trimestre,
com navegação, filtros e estatísticas, diretamente no terminal do VS Code.

Dependências:
    pip install rich pandas sqlite3

Uso:
    python pnad_visualizar.py
    python pnad_visualizar.py --db caminho/para/pnad_ceara.db
"""

import sqlite3
import sys
import argparse
from pathlib import Path

try:
    import pandas as pd
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.columns import Columns
    from rich.text import Text
    from rich.prompt import Prompt, IntPrompt
    from rich import box
    from rich.style import Style
    from rich.layout import Layout
    from rich.align import Align
    from rich.rule import Rule
except ImportError:
    print("\n[ERRO] Dependências não encontradas. Execute:\n")
    print("    pip install rich pandas\n")
    sys.exit(1)

console = Console()

# ─────────────────────────────────────────────────
#  MAPEAMENTO COMPLETO DE VARIÁVEIS (nome legível)
# ─────────────────────────────────────────────────

DESCRICOES = {
    "Ano":        "Ano de referência",
    "Trimestre":  "Trimestre",
    "UF":         "UF (23 = Ceará)",
    "Capital":    "Município da Capital",
    "RM_RIDE":    "Região Metropolitana / RIDE",
    "UPA":        "Unidade Primária de Amostragem",
    "Estrato":    "Estrato",
    "V1008":      "Nº de seleção do domicílio",
    "V1014":      "Painel",
    "V1016":      "Nº da entrevista no domicílio",
    "V1022":      "Situação do domicílio",
    "V1023":      "Tipo de área",
    "V1027":      "Peso domicílio (sem calibração)",
    "V1028":      "Peso domicílio (com calibração)",
    "V1029":      "Projeção populacional (geográfica)",
    "V1033":      "Projeção populacional (sexo/idade)",
    "posest":     "Domínios de projeção geográfica",
    "posest_sxi": "Domínios de projeção sexo/idade",
    "V2001":      "Nº de pessoas no domicílio",
    "V2007":      "Sexo",
    "V2009":      "Idade (data de referência)",
    "V2010":      "Cor ou raça",
    "V3009A":     "Curso mais elevado (anterior)",
    "V3014":      "Conclusão do curso anterior",
    "V4002":      "Trab. remunerado em produtos/moradia",
    "V4012":      "Posição no trabalho principal",
    "V4013":      "Código CNAE (atividade principal)",
    "V40132A":    "Seção CNAE (letra)",
    "V4015":      "Teve trab. não remunerado (princ.)",
    "V40151":     "Qtd. não remunerados 1–5 (princ.)",
    "V401511":    "1 a 5 trab. não remunerados",
    "V401512":    "6 a 10 trab. não remunerados",
    "V4016":      "Nº de empregados no negócio",
    "V40161":     "1 a 5 empregados",
    "V40162":     "6 a 10 empregados",
    "V40163":     "11 a 50 empregados",
    "V4017":      "Tinha sócio no negócio",
    "V40171":     "Qtd. de sócios",
    "V401711":    "1 a 5 sócios",
    "V4018":      "Total de pessoas no negócio",
    "V40181":     "1 a 5 pessoas",
    "V40182":     "6 a 10 pessoas",
    "V40183":     "11 a 50 pessoas",
    "V4019":      "Negócio registrado no CNPJ",
    "V4020":      "Tipo de local do negócio",
    "V4021":      "Exercia trab. no estabelecimento",
    "V4022":      "Local normal de trabalho",
    "V4024":      "Serv. doméstico em +1 domicílio",
    "V4025":      "Contratado como empregado temporário",
    "V4026":      "Contratado só pelo responsável",
    "V4027":      "Contratado só por intermediário",
    "V4028":      "Servidor público estatutário",
    "V4029":      "Carteira de trabalho assinada",
    "V4032":      "Contribuinte de previdência (princ.)",
    "V4033":      "Rendimento habitual — aux. (princ.)",
    "V40331":     "Recebia em dinheiro habitualmente",
    "V403312":    "Renda habitual em dinheiro (princ.) R$",
    "V403322":    "Renda habitual em produtos (princ.) R$",
    "V4034":      "Rendimento mês de ref. — aux. (princ.)",
    "V403412":    "Renda mês ref. em dinheiro (princ.) R$",
    "V403422":    "Renda mês ref. em produtos (princ.) R$",
    "V4039":      "Horas normais/semana (princ.)",
    "V4039C":     "Horas efetivas/semana ref. (princ.)",
    "V4043":      "Posição no trabalho secundário",
    "V4044":      "Código CNAE (trab. secundário)",
    "V4045":      "Área do trab. secundário",
    "V4046":      "CNPJ — trab. secundário",
    "V4048":      "Carteira assinada — trab. secundário",
    "V4050":      "Rendimento habitual — aux. (sec.)",
    "V40501":     "Recebia em dinheiro hab. (sec.)",
    "V405012":    "Renda habitual em dinheiro (sec.) R$",
    "V4051":      "Rendimento mês ref. — aux. (sec.)",
    "V40511":     "Recebeu em dinheiro mês ref. (sec.)",
    "V405112":    "Renda mês ref. em dinheiro (sec.) R$",
    "V405122":    "Renda mês ref. em produtos (sec.) R$",
    "V405912":    "Renda mês ref. em dinheiro (outros) R$",
    "V405922":    "Renda mês ref. em produtos (outros) R$",
}

GRUPOS = {
    "Identificação": ["Ano","Trimestre","UF","Capital","RM_RIDE","UPA","Estrato","V1008","V1014","V1016","V1022","V1023","posest","posest_sxi"],
    "Pesos/Projeção": ["V1027","V1028","V1029","V1033"],
    "Moradores":     ["V2001","V2007","V2009","V2010"],
    "Educação":      ["V3009A","V3014"],
    "Trab. Principal — Estrutura": ["V4002","V4012","V4013","V40132A","V4015","V40151","V401511","V401512","V4016","V40161","V40162","V40163","V4017","V40171","V401711","V4018","V40181","V40182","V40183","V4019","V4020","V4021","V4022","V4024","V4025","V4026","V4027","V4028","V4029","V4032"],
    "Trab. Principal — Rendimento": ["V4033","V40331","V403312","V403322","V4034","V403412","V403422"],
    "Trab. Principal — Horas":      ["V4039","V4039C"],
    "Trab. Secundário":              ["V4043","V4044","V4045","V4046","V4048","V4050","V40501","V405012","V4051","V40511","V405112","V405122"],
    "Outros Trabalhos":              ["V405912","V405922"],
}

VARS_RENDA = ["V403312","V403322","V403412","V403422","V405012","V405112","V405122","V405912","V405922"]

# ─────────────────────────────────────────────────


def conectar(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        console.print(f"\n[bold red]ERRO:[/] Banco não encontrado: [yellow]{db_path}[/]")
        console.print("Execute primeiro o [bold]pnad_ceara.py[/] para gerar os dados.\n")
        sys.exit(1)
    return sqlite3.connect(db_path)


def periodos_disponiveis(con: sqlite3.Connection, tabela: str) -> list:
    df = pd.read_sql(
        f"SELECT DISTINCT Ano, Trimestre FROM {tabela} ORDER BY Ano, Trimestre", con
    )
    return [(str(r.Ano), str(r.Trimestre)) for _, r in df.iterrows()]


def cabecalho_periodos(periodos: list) -> list:
    return [f"{a} T{t}" for a, t in periodos]


# ── VISÃO 1: ESTATÍSTICAS LONGITUDINAIS ──────────

def view_longitudinal(con, tabela, periodos):
    """Tabela: variável × período com estatísticas (mediana / moda / média)"""
    headers = cabecalho_periodos(periodos)

    # Colunas disponíveis no banco
    cols_banco = set(pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"].tolist())

    for grupo, variaveis in GRUPOS.items():
        variaveis = [v for v in variaveis if v in cols_banco]
        if not variaveis:
            continue

        t = Table(
            title=f" {grupo} ",
            box=box.SIMPLE_HEAD,
            header_style="bold cyan",
            title_style="bold white on blue",
            show_lines=True,
            expand=True,
        )

        t.add_column("Variável", style="bold", min_width=14, no_wrap=True)
        t.add_column("Descrição", style="dim", min_width=30)
        for h in headers:
            t.add_column(h, justify="right", min_width=13)

        for var in variaveis:
            desc = DESCRICOES.get(var, "")
            valores_por_periodo = []

            for ano, tri in periodos:
                try:
                    if var in VARS_RENDA:
                        r = pd.read_sql(
                            f"SELECT ROUND(AVG(CAST({var} AS FLOAT)),2) as v "
                            f"FROM {tabela} WHERE Ano=? AND Trimestre=? AND {var} IS NOT NULL",
                            con, params=(ano, tri)
                        )
                        val = r.iloc[0, 0]
                        cell = f"R$ {val:,.2f}" if val else "—"
                    elif var in ["V2009","V4039","V4039C"]:
                        r = pd.read_sql(
                            f"SELECT ROUND(AVG(CAST({var} AS FLOAT)),1) as v "
                            f"FROM {tabela} WHERE Ano=? AND Trimestre=? AND {var} IS NOT NULL",
                            con, params=(ano, tri)
                        )
                        val = r.iloc[0, 0]
                        cell = f"{val:.1f}" if val else "—"
                    elif var in ["V1027","V1028","V1029","V1033"]:
                        r = pd.read_sql(
                            f"SELECT COUNT(*) as n FROM {tabela} WHERE Ano=? AND Trimestre=?",
                            con, params=(ano, tri)
                        )
                        cell = f"n={int(r.iloc[0,0]):,}"
                    else:
                        r = pd.read_sql(
                            f"SELECT {var}, COUNT(*) as n FROM {tabela} "
                            f"WHERE Ano=? AND Trimestre=? AND {var} IS NOT NULL "
                            f"GROUP BY {var} ORDER BY n DESC LIMIT 1",
                            con, params=(ano, tri)
                        )
                        if r.empty:
                            cell = "—"
                        else:
                            moda = str(r.iloc[0, 0]).strip()
                            cnt  = int(r.iloc[0, 1])
                            cell = f"{moda}\n({cnt:,})"
                except Exception:
                    cell = "erro"

                valores_por_periodo.append(cell)

            t.add_row(var, desc, *valores_por_periodo)

        console.print(t)
        console.print()


# ── VISÃO 2: DISTRIBUIÇÃO DE UMA VARIÁVEL ────────

def view_distribuicao(con, tabela, variavel, periodos):
    cols_banco = set(pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"].tolist())
    if variavel not in cols_banco:
        console.print(f"[red]Variável '{variavel}' não encontrada no banco.[/]")
        return

    headers = cabecalho_periodos(periodos)
    desc    = DESCRICOES.get(variavel, "")

    console.print(Panel(
        f"[bold]{variavel}[/]  —  {desc}",
        title="Distribuição longitudinal",
        border_style="cyan",
    ))

    # Coleta todos os valores únicos em todos os períodos
    valores_unicos = set()
    dfs = {}
    for ano, tri in periodos:
        df = pd.read_sql(
            f"SELECT TRIM({variavel}) as val, COUNT(*) as n FROM {tabela} "
            f"WHERE Ano=? AND Trimestre=? AND {variavel} IS NOT NULL "
            f"GROUP BY val ORDER BY n DESC",
            con, params=(ano, tri)
        )
        dfs[(ano, tri)] = dict(zip(df["val"].astype(str), df["n"]))
        valores_unicos |= set(df["val"].astype(str))

    t = Table(box=box.SIMPLE_HEAD, header_style="bold cyan", show_lines=True, expand=True)
    t.add_column("Categoria", style="bold", min_width=20)
    for h in headers:
        t.add_column(h, justify="right", min_width=13)

    for val in sorted(valores_unicos, key=lambda x: x):
        row = [val]
        for ano, tri in periodos:
            n   = dfs[(ano, tri)].get(val, 0)
            tot = sum(dfs[(ano, tri)].values()) or 1
            pct = n / tot * 100
            row.append(f"{n:,}\n({pct:.1f}%)")
        t.add_row(*row)

    console.print(t)


# ── VISÃO 3: RENDIMENTOS LONGITUDINAIS ───────────

def view_rendimentos(con, tabela, periodos):
    headers = cabecalho_periodos(periodos)

    t = Table(
        title=" Rendimentos médios — evolução longitudinal ",
        box=box.SIMPLE_HEAD,
        header_style="bold cyan",
        title_style="bold white on dark_green",
        show_lines=True,
        expand=True,
    )
    t.add_column("Variável", style="bold", min_width=12)
    t.add_column("Descrição", style="dim", min_width=36)
    for h in headers:
        t.add_column(h, justify="right", min_width=15)

    cols_banco = set(pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"].tolist())

    for var in VARS_RENDA:
        if var not in cols_banco:
            continue
        desc = DESCRICOES.get(var, "")
        row  = [var, desc]
        for ano, tri in periodos:
            r = pd.read_sql(
                f"SELECT ROUND(AVG(CAST({var} AS FLOAT)),2) as m, COUNT(*) as n "
                f"FROM {tabela} WHERE Ano=? AND Trimestre=? AND {var} IS NOT NULL AND {var} > 0",
                con, params=(ano, tri)
            )
            m = r.iloc[0, 0]
            n = r.iloc[0, 1]
            row.append(f"R$ {m:,.2f}\n(n={int(n):,})" if m else "—")
        t.add_row(*row)

    console.print(t)


# ── VISÃO 4: PERFIL SOCIODEMOGRÁFICO ─────────────

def view_perfil(con, tabela, periodos):
    headers = cabecalho_periodos(periodos)

    cols = ["V2007","V2010","V1022","V4012","V4029","V4032"]
    cols_banco = set(pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"].tolist())
    cols = [c for c in cols if c in cols_banco]

    for col in cols:
        desc = DESCRICOES.get(col,"")
        console.print(Rule(f"[bold cyan]{col}[/] — {desc}", style="cyan"))

        t = Table(box=box.SIMPLE, header_style="bold", show_lines=False, expand=True)
        t.add_column("Categoria", min_width=22)
        for h in headers:
            t.add_column(h, justify="right", min_width=16)

        valores = set()
        dfs = {}
        for ano, tri in periodos:
            df = pd.read_sql(
                f"SELECT TRIM({col}) as val, COUNT(*) as n FROM {tabela} "
                f"WHERE Ano=? AND Trimestre=? AND {col} IS NOT NULL "
                f"GROUP BY val",
                con, params=(ano, tri)
            )
            dfs[(ano,tri)] = dict(zip(df["val"].astype(str), df["n"]))
            valores |= set(df["val"].astype(str))

        for val in sorted(valores):
            row = [val]
            for ano, tri in periodos:
                n   = dfs[(ano,tri)].get(val, 0)
                tot = sum(dfs[(ano,tri)].values()) or 1
                row.append(f"{n:,}  ({n/tot*100:.1f}%)")
            t.add_row(*row)

        console.print(t)
        console.print()


# ── VISÃO 5: VARIÁVEIS BRUTAS (amostra) ──────────

def view_amostra(con, tabela, n=20):
    cols_banco = list(pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"])
    df = pd.read_sql(f"SELECT * FROM {tabela} LIMIT {n}", con)

    console.print(Panel(
        f"[bold]Primeiras {n} linhas — todas as colunas[/]",
        border_style="yellow",
    ))

    # Exibe em blocos de 10 colunas para caber no terminal
    bloco = 10
    for i in range(0, len(cols_banco), bloco):
        sub = cols_banco[i:i+bloco]
        t = Table(box=box.MINIMAL_DOUBLE_HEAD, header_style="bold yellow", expand=True)
        for c in sub:
            t.add_column(c, min_width=10, max_width=18, no_wrap=True)
        for _, row in df[sub].iterrows():
            t.add_row(*[str(v) if pd.notna(v) else "—" for v in row])
        console.print(t)
        console.print()


# ── MENU PRINCIPAL ────────────────────────────────

def menu(db_path: Path, tabela: str = "pnad_ce"):
    con = conectar(db_path)
    periodos = periodos_disponiveis(con, tabela)

    if not periodos:
        console.print("[red]Nenhum período encontrado no banco.[/]")
        sys.exit(1)

    opcoes_menu = {
        "1": "Visão longitudinal — todas as variáveis (moda/média por período)",
        "2": "Distribuição detalhada de uma variável",
        "3": "Rendimentos médios — evolução longitudinal",
        "4": "Perfil sociodemográfico (sexo, raça, área, posição, formalidade)",
        "5": "Amostra de linhas brutas",
        "0": "Sair",
    }

    while True:
        console.clear()
        console.print(Panel(
            Align.center(
                f"[bold white]PNAD Contínua — Ceará[/]\n"
                f"[dim]Banco:[/] [yellow]{db_path}[/]\n"
                f"[dim]Períodos:[/] {', '.join(cabecalho_periodos(periodos))}\n"
                f"[dim]Variáveis:[/] {len(DESCRICOES)}"
            ),
            border_style="blue",
        ))

        console.print()
        for k, v in opcoes_menu.items():
            console.print(f"  [bold cyan]{k}[/]  {v}")

        console.print()
        escolha = Prompt.ask("[bold]Opção", choices=list(opcoes_menu.keys()), default="1")

        if escolha == "0":
            break

        console.clear()

        if escolha == "1":
            view_longitudinal(con, tabela, periodos)

        elif escolha == "2":
            console.print("[dim]Variáveis disponíveis:[/]")
            for grupo, vars_ in GRUPOS.items():
                cols_banco = set(pd.read_sql(f"PRAGMA table_info({tabela})", con)["name"].tolist())
                vars_ok = [v for v in vars_ if v in cols_banco]
                if vars_ok:
                    console.print(f"  [cyan]{grupo}:[/] {', '.join(vars_ok)}")
            console.print()
            var = Prompt.ask("Nome da variável")
            view_distribuicao(con, tabela, var.strip(), periodos)

        elif escolha == "3":
            view_rendimentos(con, tabela, periodos)

        elif escolha == "4":
            view_perfil(con, tabela, periodos)

        elif escolha == "5":
            n = IntPrompt.ask("Quantas linhas?", default=20)
            view_amostra(con, tabela, n)

        console.print()
        Prompt.ask("\n[dim]Pressione Enter para voltar ao menu[/]", default="")

    con.close()
    console.print("\n[dim]Até logo.[/]\n")


# ─────────────────────────────────────────────────
#  ENTRADA
# ─────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Visualiza microdados PNAD Ceará (SQLite) no terminal."
    )
    parser.add_argument(
        "--db",
        default="dados_pnad_ce/pnad_ceara.db",
        help="Caminho para o banco SQLite (padrão: dados_pnad_ce/pnad_ceara.db)",
    )
    parser.add_argument(
        "--tabela",
        default="pnad_ce",
        help="Nome da tabela no banco (padrão: pnad_ce)",
    )
    args = parser.parse_args()

    menu(Path(args.db), args.tabela)