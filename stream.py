import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import matplotlib.pyplot as plt  # Para gráficos de pizza/barras

# =========================================================================
# 1. Configurações de Conexão ao Banco
# =========================================================================
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "U7urkInVDg[(D^{&")
DB_HOST = os.getenv("DB_HOST", "34.130.95.218")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=False)

# =========================================================================
# 2. Funções Auxiliares
# =========================================================================

def carregar_dados():
    """
    Lê dados de sku_marketplace, comissoes_pedido e evento_centauro,
    realiza os LEFT JOINs necessários e retorna um DataFrame consolidado.
    Também normaliza os tipos de evento para garantir consistência.
    """
    query = text("""
        SELECT
            sm.id AS sku_marketplace_id,
            sm.numero_pedido,
            sm.valor_liquido,
            sm.valor_final,
            cp.data AS data_comissao,
            cp.porcentagem,
            (cp.porcentagem * sm.valor_liquido) AS comissao_calc,
            ec.tipo_evento,
            ec.repasse_liquido_evento,
            ec.data AS data_evento,          -- data do pedido em evento_centauro
            ec.data_repasse AS data_ciclo    -- data do ciclo no evento_centauro
        FROM sku_marketplace sm
        LEFT JOIN comissoes_pedido cp
            ON sm.id = cp.sku_marketplace_id
        LEFT JOIN evento_centauro ec
            ON ec.numero_pedido = sm.numero_pedido
    """)
    df = pd.read_sql(query, engine)

    # Filtra colunas que você quer exibir
    colunas_desejadas = [
        "sku_marketplace_id",
        "numero_pedido",
        "valor_liquido",
        "valor_final",
        "repasse_liquido_evento",
        "data_comissao",
        "porcentagem",
        "comissao_calc",
        "tipo_evento",
        "data_evento",
        "data_ciclo"
    ]
    df = df[colunas_desejadas]

    # Normalização dos tipos de evento
    df["tipo_evento_normalizado"] = df["tipo_evento"].apply(normalizar_tipo_evento)

    return df


def normalizar_tipo_evento(evento):
    """
    Mapeia variantes de tipos de evento para nomes padronizados.
    """
    if pd.isnull(evento):
        return "Desconhecido"

    evento = evento.strip().lower()

    mapping = {
        "repasse normal": "Repasse Normal",
        "repasse - normal": "Repasse Normal",
        "repassse normal": "Repasse Normal",   # Possíveis erros de digitação
        "repassse - normal": "Repasse Normal",

        "descontar hove": "Descontar Hove/Houve",
        "descontar houve": "Descontar Hove/Houve",
        "descontar - houve": "Descontar Hove/Houve",
        "descontar - hove": "Descontar Hove/Houve",

        "descontar reversa centauro envios": "Descontar Reversa Centauro Envios",
        "descontar - reversa centauro envios": "Descontar Reversa Centauro Envios",

        "ajuste de ciclo": "Ajuste de Ciclo",

        "descontar retroativo": "Descontar Retroativo",
        "descontar - retroativo": "Descontar Retroativo",
        "descontar retroativo sac": "Descontar Retroativo",
        "descontar - retroativo sac": "Descontar Retroativo",
    }
    return mapping.get(evento, "Outros")


def checar_erro_comissao(row):
    """
    Verifica se, para eventos de 'Repasse Normal', o valor_final está correto
    com base na porcentagem de comissão.
    """
    if row["tipo_evento_normalizado"] != "Repasse Normal":
        return ""

    if pd.isnull(row["porcentagem"]):
        return ""

    vl_liquido = round(row["valor_liquido"], 2)
    vl_final = round(row["valor_final"], 2)  # Mantendo a lógica original
    porcent = round(row["porcentagem"], 4)

    valor_calc = round(vl_liquido - (vl_liquido * porcent), 2)
    if valor_calc != vl_final:
        return "ERRO"
    else:
        return ""


def checar_erros_adicionais(row):
    """
    Marca erros adicionais (valor final negativo, falta de comissão, etc.) para repasse normal.
    Também marca Erro Devolução se erro_descontar == "ERRO_DEVOLUCAO".
    """
    erros = []
    if row["tipo_evento_normalizado"] == "Repasse Normal":
        if row["valor_final"] < 0:
            erros.append("Valor Final Negativo")

        if pd.isnull(row["porcentagem"]):
            erros.append("Falta de Comissão")

        if pd.isnull(row["data_comissao"]):
            erros.append("Falta de Data de Comissão")

        if row["erro_comissao"] == "ERRO":
            erros.append("Erro Cálculo Comissão")

    if "erro_descontar" in row and row["erro_descontar"] == "ERRO_DEVOLUCAO":
        erros.append("Erro Devolução")

    return erros


def filtrar_por_erros(df, erros_selecionados):
    """
    Retorna apenas linhas que contenham algum dos erros selecionados, se houver.
    """
    if not erros_selecionados:
        return df
    mask = df["lista_erros"].apply(lambda lista: any(e in lista for e in erros_selecionados))
    return df[mask]


def verificar_descontar_hove(df):
    """
    Verifica se, para "Repasse Normal" + "Descontar Hove/Houve", os valores absolutos batem.
    """
    subset = df[["numero_pedido", "tipo_evento_normalizado",
                 "valor_liquido", "repasse_liquido_evento"]].copy()

    grupos = []
    for pedido, grupo in subset.groupby("numero_pedido"):
        valor_liquido_repasse_normal = None
        repasse_liquido_evento_descontar = None

        for _, row in grupo.iterrows():
            if row["tipo_evento_normalizado"] == "Repasse Normal":
                valor_liquido_repasse_normal = row["valor_liquido"]
            elif row["tipo_evento_normalizado"] == "Descontar Hove/Houve":
                repasse_liquido_evento_descontar = row["repasse_liquido_evento"]

        if (valor_liquido_repasse_normal is not None) and (repasse_liquido_evento_descontar is not None):
            if round(abs(valor_liquido_repasse_normal), 2) != round(abs(repasse_liquido_evento_descontar), 2):
                erro = "ERRO_DEVOLUCAO"
            else:
                erro = ""
            grupos.append({
                "numero_pedido": pedido,
                "valor_liquido_repasse_normal": valor_liquido_repasse_normal,
                "repasse_liquido_evento_descontar_houve": repasse_liquido_evento_descontar,
                "erro_descontar": erro
            })

    df_result = pd.DataFrame(grupos, columns=[
        "numero_pedido",
        "valor_liquido_repasse_normal",
        "repasse_liquido_evento_descontar_houve",
        "erro_descontar"
    ])
    df_result = df_result.drop_duplicates(subset=["numero_pedido"])
    return df_result


def verificar_descontar_retroativo(df):
    """
    Agrupa 'Descontar Retroativo' e soma repasse_liquido_evento;
    marca ERRO_DESCONTAR_RETROATIVO se a soma for igual ao valor_liquido (em valor absoluto).
    """
    subset = df[df["tipo_evento_normalizado"] == "Descontar Retroativo"].copy()
    if subset.empty:
        return pd.DataFrame(columns=[
            "numero_pedido",
            "valor_liquido",
            "soma_descontar_retroativo",
            "Diferenca",
            "erro_descontar_retroativo"
        ])

    grouped = subset.groupby("numero_pedido").agg({
        "valor_liquido": "first",
        "repasse_liquido_evento": "sum"
    }).reset_index()

    grouped.rename(columns={"repasse_liquido_evento": "soma_descontar_retroativo"}, inplace=True)
    grouped["Diferenca"] = grouped["valor_liquido"] + grouped["soma_descontar_retroativo"]

    def verificar_erro(row):
        if (round(abs(row["soma_descontar_retroativo"]), 2) == round(abs(row["valor_liquido"]), 2)
           and round(row["valor_liquido"], 2) != 0):
            return "ERRO_DESCONTAR_RETROATIVO"
        return ""

    grouped["erro_descontar_retroativo"] = grouped.apply(verificar_erro, axis=1)
    return grouped


# =========================================================================
# 4. Streamlit
# =========================================================================

def main():
    st.title("Painel de Análises e Filtros (Com Data/Ciclo)")

    # ------------------- SIDEBAR -------------------
    st.sidebar.header("Filtros de Pesquisa")
    pedido_filtro = st.sidebar.text_input("Número do Pedido:", "")
    st.sidebar.header("Filtros de Tipo de Evento")
    tipos_evento_padronizados = [
        "Repasse Normal",
        "Descontar Hove/Houve",
        "Descontar Reversa Centauro Envios",
        "Descontar Retroativo",
        "Ajuste de Ciclo",
        "Outros"
    ]
    evento_filtro = st.sidebar.multiselect(
        "Selecione o(s) Tipo(s) de Evento:",
        tipos_evento_padronizados,
        default=tipos_evento_padronizados
    )

    col1, col2 = st.sidebar.columns(2)
    data_ini = col1.date_input("Data inicial (comissão)", None)
    data_fim = col2.date_input("Data final (comissão)", None)

    st.sidebar.header("Filtros por Erro")
    opcoes_de_erros = [
        "Valor Final Negativo",
        "Falta de Comissão",
        "Falta de Data de Comissão",
        "Erro Cálculo Comissão",
        "Erro Devolução",
    ]
    erros_selecionados = st.sidebar.multiselect(
        "Selecione o(s) tipo(s) de erro:",
        opcoes_de_erros
    )

    # ------------------- CARREGAR DADOS -------------------
    df = carregar_dados()

    # 1) Verificação de comissão
    df["erro_comissao"] = df.apply(checar_erro_comissao, axis=1)

    # 2) Verificação "Descontar Hove/Houve"
    df_descontar_hove = verificar_descontar_hove(df)
    df = df.merge(df_descontar_hove, on="numero_pedido", how="left")
    if 'erro_descontar' not in df.columns:
        df['erro_descontar'] = ''

    # 3) Erros adicionais (lista de erros)
    df["lista_erros"] = df.apply(checar_erros_adicionais, axis=1)

    # 4) Filtros
    df_filtrado = df.copy()

    # Filtro por Número do Pedido
    if pedido_filtro:
        df_filtrado = df_filtrado[df_filtrado["numero_pedido"].astype(str).str.contains(pedido_filtro, na=False)]

    # Filtro por Tipo de Evento
    if evento_filtro:
        df_filtrado = df_filtrado[df_filtrado["tipo_evento_normalizado"].isin(evento_filtro)]

    # Filtro por Data de Comissão
    if data_ini and data_fim:
        df_filtrado["data_comissao"] = pd.to_datetime(df_filtrado["data_comissao"], errors="coerce")
        df_filtrado = df_filtrado[
            (df_filtrado["data_comissao"].notnull()) &
            (df_filtrado["data_comissao"] >= pd.to_datetime(data_ini)) &
            (df_filtrado["data_comissao"] <= pd.to_datetime(data_fim))
        ]

    # 5) Filtrar por erros selecionados
    df_filtrado = filtrar_por_erros(df_filtrado, erros_selecionados)

    # 6) Abas
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Visão Geral",
        "Visão Geral Anymarket",
        "Erros de Comissão",
        "Erros de Descontar Hove/Houve",
        "Descontar Retroativo",
        "Duplicatas",
        "Gráficos"
    ])

    # ------------------- ABA 1: VISÃO GERAL -------------------
    with tab1:
        st.markdown("## Visão Geral dos Dados Filtrados")

        colunas_visao_geral = [
            "numero_pedido",
            "valor_liquido",
            "repasse_liquido_evento",
            "tipo_evento",
            "data_evento",
            "porcentagem",
            "comissao_calc",
            "data_ciclo",
            "lista_erros"
        ]
        df_visao_geral = df_filtrado[colunas_visao_geral].copy()

        df_visao_geral = df_visao_geral.rename(columns={
            "comissao_calc": "Comissão",
            "lista_erros": "Erros",
            "numero_pedido": "Número do Pedido",
            "valor_liquido": "Valor Pedido",
            "repasse_liquido_evento": "Valor Final",
            "tipo_evento": "Tipo de Evento",
            "data_evento": "Data do Pedido",
            "porcentagem": "Porcentagem",
            "data_ciclo": "Data do Ciclo"
        })

        df_visao_geral_styled = df_visao_geral.style.format({
            "Valor Pedido": "{:.2f}",
            "Valor Final": "{:.2f}",
            "Porcentagem": "{:.2f}",
            "Comissão": "{:.2f}"
        })
        st.dataframe(data=df_visao_geral_styled, width=20000000)

        st.markdown("### Resumo de Registros")
        qtd_total = len(df_filtrado)
        qtd_erro_comissao = sum(df_filtrado["erro_comissao"] == "ERRO")

        # Erros de Devolução (sem duplicar pedidos)
        df_err_devolucao = df_filtrado[df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO"]
        df_err_devolucao = df_err_devolucao.drop_duplicates(subset=["numero_pedido"])
        qtd_erro_devolucao = len(df_err_devolucao)

        # Soma usando repasse_liquido_evento
        soma_val_final = df_filtrado["repasse_liquido_evento"].sum()

        colA, colB, colC, colD = st.columns(4)
        colA.metric("Qtd. Registros (filtro)", qtd_total)
        colB.metric("Erros de Comissão", qtd_erro_comissao)
        colC.metric("Erros de Devolução", qtd_erro_devolucao)
        colD.metric("Soma Valor Final", f"{soma_val_final:,.2f}")

        df_filtrado["tem_algum_erro"] = df_filtrado["lista_erros"].apply(lambda x: len(x) > 0)
        qtd_qualquer_erro = df_filtrado["tem_algum_erro"].sum()
        st.info(f"Registros com *qualquer erro*: {qtd_qualquer_erro}")

    # ---------------------------------------------------------------------
    # ABA 2: VISÃO GERAL ANYMARKET
    # ---------------------------------------------------------------------
    with tab2:
        st.markdown("## Visão Geral Anymarket")

        # 1) Carrega dados da tabela 'vendas' para obter valor_vendas
        query_vendas = text("""
            SELECT
                id AS venda_id,
                sku_marketplace_id,
                valor_liquido AS valor_vendas
            FROM vendas
        """)
        df_vendas = pd.read_sql(query_vendas, engine)

        # 2) Merge com df_filtrado para comparar valores
        df_any = df_filtrado.merge(
            df_vendas,
            how="left",
            left_on="sku_marketplace_id",
            right_on="sku_marketplace_id",
            suffixes=("", "_vendas")
        )

        # Substituir NaN em valor_vendas por 0 => venda não encontrada
        df_any["valor_vendas"] = df_any["valor_vendas"].fillna(0)

        # 3) Criar e aplicar função para verificar erros
        def checar_erros_anymarket(row):
            erros = []
            # Erro se não encontrar a venda (valor_vendas == 0)
            if row["valor_vendas"] == 0:
                erros.append("ERRO_VENDA_NAO_ENCONTRADA")

            # Erro se for Repasse Normal e os valores divergem
            if (
                row["tipo_evento_normalizado"] == "Repasse Normal"
                and round(row["valor_liquido"], 2) != round(row["valor_vendas"], 2)
                and row["valor_vendas"] != 0
            ):
                erros.append("ERRO_VALORES_DIVERGENTES")

            return erros

        df_any["erros_anymarket"] = df_any.apply(checar_erros_anymarket, axis=1)

        # === NOVO: Filtro para exibir "Todos", "SEM_ERRO", "ERRO_VENDA_NAO_ENCONTRADA", "ERRO_VALORES_DIVERGENTES" ===
        st.subheader("Filtrar por Erros Anymarket")
        error_options = [
            "Todos",
            "SEM_ERRO",
            "ERRO_VENDA_NAO_ENCONTRADA",
            "ERRO_VALORES_DIVERGENTES"
        ]
        selected_any_error = st.selectbox("Selecione o tipo de erro a exibir", error_options, index=0)

        # 4) Montar DF de exibição
        colunas_any = [
            "numero_pedido",
            "tipo_evento_normalizado",
            "valor_liquido",       # do sku_marketplace
            "valor_vendas",        # do vendas
            "erros_anymarket"
        ]
        df_any_exibe = df_any[colunas_any].copy()

        # Renomear colunas para melhor visualização
        df_any_exibe = df_any_exibe.rename(columns={
            "numero_pedido": "Número do Pedido",
            "valor_liquido": "Valor (sku_marketplace)",
            "valor_vendas": "Valor (vendas)",
            "tipo_evento_normalizado": "Tipo de Evento",
            "erros_anymarket": "Erros Anymarket"
        })

        # Remoção de duplicatas: mesmo pedido + mesmo tipo de evento
        df_any_exibe = df_any_exibe.drop_duplicates(
            subset=["Número do Pedido", "Tipo de Evento"]
        )

        # Filtrar de acordo com o selectbox
        # Transformar "Erros Anymarket" em uma string para facilitar a checagem
        df_any_exibe["ErrosStr"] = df_any_exibe["Erros Anymarket"].apply(lambda lista: ",".join(lista) if lista else "")

        if selected_any_error == "SEM_ERRO":
            # mostrar só registros onde ErrosStr está vazio
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"] == ""]
        elif selected_any_error == "ERRO_VENDA_NAO_ENCONTRADA":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"].str.contains("ERRO_VENDA_NAO_ENCONTRADA")]
        elif selected_any_error == "ERRO_VALORES_DIVERGENTES":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"].str.contains("ERRO_VALORES_DIVERGENTES")]
        else:
            # "Todos" => não filtra
            pass

        # Format
        df_any_exibe_style = df_any_exibe.style.format({
            "Valor (sku_marketplace)": "{:.2f}",
            "Valor (vendas)": "{:.2f}"
        })
        st.dataframe(df_any_exibe_style)

        # 5) Resumo de erros
        todas_ocorrencias = []
        for lista_e in df_any_exibe["Erros Anymarket"]:
            todas_ocorrencias.extend(lista_e)

        qtd_erro_venda_nao_encontrada = sum(e == "ERRO_VENDA_NAO_ENCONTRADA" for e in todas_ocorrencias)
        qtd_erro_valores_diverg = sum(e == "ERRO_VALORES_DIVERGENTES" for e in todas_ocorrencias)

        colA1, colA2 = st.columns(2)
        colA1.metric("ERRO_VENDA_NAO_ENCONTRADA", qtd_erro_venda_nao_encontrada)
        colA2.metric("ERRO_VALORES_DIVERGENTES", qtd_erro_valores_diverg)

        st.info(f"{len(df_any_exibe)} registro(s) exibidos na 'Visão Geral Anymarket'")

    # ---------------------------------------------------------------------
    # ABA 3: ERROS DE COMISSÃO
    # ---------------------------------------------------------------------
    with tab3:
        st.markdown("## Erros de Comissão (Repasse Normal)")
        df_err = df_filtrado[df_filtrado["erro_comissao"] == "ERRO"]
        if df_err.empty:
            st.info("Nenhum erro de comissão com base nos filtros.")
        else:
            st.warning(f"{len(df_err)} registros com erro de comissão.")
            colunas_erros_comissao = [
                "numero_pedido",
                "valor_liquido",
                "valor_final",
                "tipo_evento",
                "data_evento",
                "porcentagem",
                "comissao_calc",
                "data_ciclo",
                "lista_erros"
            ]
            df_err_vis = df_err[colunas_erros_comissao].copy()
            df_err_vis = df_err_vis.rename(columns={
                "comissao_calc": "Comissão",
                "lista_erros": "Erros",
                "numero_pedido": "Número do Pedido",
                "valor_liquido": "Valor Pedido",
                "valor_final": "Valor Final",
                "tipo_evento": "Tipo de Evento",
                "data_evento": "Data do Pedido",
                "porcentagem": "Porcentagem",
                "data_ciclo": "Data do Ciclo"
            })

            df_err_vis_styled = df_err_vis.style.format({
                "Valor Pedido": "{:.2f}",
                "Valor Final": "{:.2f}",
                "Porcentagem": "{:.2f}",
                "Comissão": "{:.2f}"
            })

            st.dataframe(df_err_vis_styled)

    # ---------------------------------------------------------------------
    # ABA 4: ERROS DE DESCONTAR HOVE/HOUVE
    # ---------------------------------------------------------------------
    with tab4:
        st.markdown("## Erros de Descontar Hove/Houve")
        df_descontar_hove_erro = df_filtrado[
            (df_filtrado["tipo_evento_normalizado"] == "Descontar Hove/Houve") &
            (df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO")
        ]

        colunas_descontar_hove = [
            "numero_pedido",
            "valor_liquido",
            "repasse_liquido_evento",
            "tipo_evento",
            "data_evento",
            "data_ciclo"
        ]
        df_descontar_hove_erro = df_descontar_hove_erro[colunas_descontar_hove].drop_duplicates(subset=["numero_pedido"])
        df_descontar_hove_erro = df_descontar_hove_erro.rename(columns={
            "numero_pedido": "Número do Pedido",
            "valor_liquido": "Valor Pedido",
            "repasse_liquido_evento": "Valor Final",
            "tipo_evento": "Tipo de Evento",
            "data_evento": "Data do Pedido",
            "data_ciclo": "Data do Ciclo"
        })

        df_descontar_hove_erro["Diferença"] = df_descontar_hove_erro["Valor Final"] + df_descontar_hove_erro["Valor Pedido"]

        def color_diff(val):
            color = 'green' if val > 0 else 'red' if val < 0 else 'black'
            return f'color: {color}'

        styled_df_hove = df_descontar_hove_erro.style.format({
            "Valor Pedido": "{:.2f}",
            "Valor Final": "{:.2f}",
            "Diferença": "{:.2f}"
        }).map(color_diff, subset=["Diferença"])

        if df_descontar_hove_erro.empty:
            st.info("Nenhum erro de Descontar Hove/Houve com base nos filtros.")
        else:
            st.error(f"{len(df_descontar_hove_erro)} registro(s) com erro de Descontar Hove/Houve.")
            st.dataframe(styled_df_hove)

    # ---------------------------------------------------------------------
    # ABA 5: DESCONTAR RETROATIVO
    # ---------------------------------------------------------------------
    with tab5:
        st.markdown("## Verificação: Descontar Retroativo")
        df_retroativo = verificar_descontar_retroativo(df_filtrado)

        if df_retroativo.empty:
            st.info("Nenhum registro de 'Descontar Retroativo' encontrado com base nos filtros.")
        else:
            st.markdown("### Registros de Descontar Retroativo Agrupados")

            def color_diff(val):
                color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                return f'color: {color}'

            df_retroativo_exibe = df_retroativo.rename(columns={
                "numero_pedido": "Número do Pedido",
                "valor_liquido": "Valor Pedido",
                "soma_descontar_retroativo": "Soma Retroativo",
                "Diferenca": "Diferença",
                "erro_descontar_retroativo": "Erro Retroativo"
            })

            df_retroativo_styled = df_retroativo_exibe.style.format({
                "Valor Pedido": "{:.2f}",
                "Soma Retroativo": "{:.2f}",
                "Diferença": "{:.2f}"
            }).map(color_diff, subset=["Diferença"])

            qtd_erro_retro = (df_retroativo_exibe["Erro Retroativo"] == "ERRO_DESCONTAR_RETROATIVO").sum()
            if qtd_erro_retro > 0:
                st.error(f"{qtd_erro_retro} registro(s) com erro de Descontar Retroativo.")
            else:
                st.info("Nenhum erro de Descontar Retroativo detectado na soma absoluta.")

            st.dataframe(df_retroativo_styled)

    # ---------------------------------------------------------------------
    # ABA 6: DUPLICATAS
    # ---------------------------------------------------------------------
    with tab6:
        st.markdown("## Possíveis Duplicatas (Mesmo Pedido + Mesmo Tipo de Evento)")
        df_dups = df_filtrado.groupby(["numero_pedido", "tipo_evento_normalizado"]).size().reset_index(name="count")
        df_dups = df_dups[df_dups["count"] > 1]
        if df_dups.empty:
            st.info("Nenhuma duplicata pelo critério (pedido + tipo de evento).")
        else:
            st.warning("Duplicatas encontradas:")
            df_dups = df_dups.rename(columns={
                "numero_pedido": "Número do Pedido",
                "tipo_evento_normalizado": "Tipo de Evento",
                "count": "Quantidade"
            })

            df_dups["Quantidade"] = df_dups["Quantidade"].astype(float)
            df_dups_styled = df_dups.style.format({"Quantidade": "{:.2f}"})
            st.dataframe(df_dups_styled)

    # ---------------------------------------------------------------------
    # ABA 7: GRÁFICOS
    # ---------------------------------------------------------------------
    with tab7:
        st.markdown("## Gráficos e Visualizações")

        st.subheader("Distribuição de Tipo de Evento")
        if not df_filtrado.empty:
            cont_eventos = df_filtrado["tipo_evento_normalizado"].value_counts()
            st.bar_chart(cont_eventos)
        else:
            st.info("Sem dados para exibir.")

        st.subheader("Distribuição de Erros Encontrados")
        todas_ocorrencias = []
        for lista_e in df_filtrado["lista_erros"]:
            todas_ocorrencias.extend(lista_e)

        if not todas_ocorrencias:
            st.info("Nenhum erro no dataset filtrado.")
        else:
            contagem = pd.Series(todas_ocorrencias).value_counts()
            st.write("**Gráfico de Barras**:")
            st.bar_chart(contagem)

            st.write("**Gráfico de Pizza**:")
            fig, ax = plt.subplots()
            ax.pie(contagem.values, labels=contagem.index, autopct="%1.1f%%")
            ax.axis("equal")
            st.pyplot(fig)


if __name__ == "__main__":
    main()
