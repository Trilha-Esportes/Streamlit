import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import matplotlib.pyplot as plt  # Para gráficos de pizza/barras
from dotenv import load_dotenv

# =========================================================================
# 1. Configurações de Conexão ao Banco
# =========================================================================
load_dotenv()

DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")
DB_NAME = os.getenv("DB_NAME", "")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=False)

# =========================================================================
# 2. Funções Auxiliares
# =========================================================================

@st.cache_data
def carregar_dados_geral():
    """
    Lê dados de sku_marketplace, marketplaces, comissoes_pedido e evento_centauro,
    realiza os LEFT JOINs necessários e retorna um DataFrame consolidado.
    Também normaliza os tipos de evento para garantir consistência.
    """
    query = text("""
        SELECT
            mk.nome AS marketplace,
            sm.id AS sku_marketplace_id,
            sm.numero_pedido,
            COALESCE(sm.valor_liquido, 0) AS valor_liquido_repasse,
            COALESCE(sm.valor_final, 0) AS valor_final_repasse_comissao,
            COALESCE(v.valor_liquido, 0) AS valor_liquido_vendas,
            COALESCE(v."data", NULL) AS data_vendas,
            COALESCE(v."status", NULL) AS status_vendas,
            cp.data AS data_comissao,
            cp.porcentagem,
            (cp.porcentagem * sm.valor_liquido) AS comissao_calc,
            ec.tipo_evento,
            ec.repasse_liquido_evento,
            v.data AS data_evento,
            ec.data_repasse AS data_ciclo
        FROM sku_marketplace sm
        LEFT JOIN marketplaces mk
            ON sm.marketplace_id = mk.id
        LEFT JOIN vendas v
            ON sm.id = v.sku_marketplace_id
        LEFT JOIN comissoes_pedido cp
            ON sm.id = cp.sku_marketplace_id
        LEFT JOIN evento_centauro ec
            ON ec.numero_pedido = sm.numero_pedido;
    """)
    df = pd.read_sql(query, engine)

    # Renomeia colunas para padronizar internamente
    df.rename(columns={
        "numero_pedido": "numero_pedido",
        "valor_liquido_repasse": "valor_liquido",
        "valor_final_repasse_comissao": "valor_final"
    }, inplace=True)

    # Preenche valores nulos em colunas-chave para evitar problemas de filtragem
    df["valor_liquido"] = df["valor_liquido"].fillna(0)
    df["valor_final"] = df["valor_final"].fillna(0)
    df["repasse_liquido_evento"] = df["repasse_liquido_evento"].fillna(0)
    df["valor_liquido_vendas"] = df["valor_liquido_vendas"].fillna(0)
    df["tipo_evento"] = df["tipo_evento"].fillna("")
    df["porcentagem"] = df["porcentagem"].fillna(0)

    # Cria coluna normalizada de tipo_evento
    df["tipo_evento_normalizado"] = df["tipo_evento"].apply(normalizar_tipo_evento)

    return df


def normalizar_tipo_evento(evento):
    if not evento.strip():
        return "Desconhecido"

    evento = evento.strip().lower()
    mapping = {
        "repasse normal": "Repasse Normal",
        "repasse - normal": "Repasse Normal",
        "repassse normal": "Repasse Normal",
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
        "descontar retroativos": "Descontar Retroativo",
        "descontar - retroativos": "Descontar Retroativo",
        "descontar retroativos sac": "Descontar Retroativo",
        "descontar - retroativos sac": "Descontar Retroativo",
    }
    return mapping.get(evento, "Outros")


def checar_erro_comissao(row):
    """
    Verifica se, para eventos de 'Repasse Normal', o valor_final está correto
    com base na porcentagem de comissão, mas com tolerância de R$0,05.
    """
    if row["tipo_evento_normalizado"] != "Repasse Normal":
        return ""

    # Sem porcentagem, sem como checar
    if pd.isnull(row["porcentagem"]) or row["porcentagem"] == 0:
        return ""

    vl_liquido = round(row["valor_liquido"], 2)
    vl_final = round(row["valor_final"], 2)
    porcent = round(row["porcentagem"], 4)

    valor_calc = round(vl_liquido - (vl_liquido * porcent), 2)

    # Se diferença maior que 5 centavos, marcar erro
    if abs(valor_calc - vl_final) > 0.05:
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

        if row["porcentagem"] == 0:
            erros.append("Falta de Comissão")

        if pd.isnull(row["data_comissao"]):
            erros.append("Falta de Data de Comissão")

        if row["erro_comissao"] == "ERRO":
            erros.append("Erro Cálculo Comissão")

    # Se a verificação de "Descontar Hove/Houve" detectou erro:
    if "erro_descontar" in row and row["erro_descontar"] == "ERRO_DEVOLUCAO":
        erros.append("Erro Devolução")

    return erros


def filtrar_por_erros(df, erros_selecionados):
    """Retorna apenas linhas que contenham algum dos erros selecionados, se houver."""
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


@st.cache_data
def carregar_vendas():
    """Carrega df_vendas de forma cacheada."""
    query_vendas = text("""
        SELECT
            id AS venda_id,
            sku_marketplace_id,
            valor_liquido AS valor_vendas
        FROM vendas
    """)
    return pd.read_sql(query_vendas, engine)


def montar_resumo_financeiro(df_geral, df_vendas):
    """
    Retorna um DF consolidado com:
    - Marketplace
    - CÓDIGO PEDIDO (aqui usaremos "numero_pedido")
    - DATA PEDIDO
    - VALOR TOTAL DOS PRODUTOS
    - Comissão Esperada
    - Valor a Receber
    - Valor Recebido
    - Valor Descontado (SOMA de Descontar Hove/Houve + Descontar Retroativo)
    - Desconto frete
    - Situação do pagamento
    - Situação final

    Obs.: Registros onde VALOR TOTAL DOS PRODUTOS = 0 são descartados (se a coluna existir).
    """

    # Faz merge para obter valor_vendas (valor total dos produtos)
    df_merge = df_geral.merge(
        df_vendas,
        how="left",
        left_on="sku_marketplace_id",
        right_on="sku_marketplace_id",
        suffixes=("", "_vendas")
    )

    # Se não encontrar valor_vendas, considere 0
    df_merge["valor_vendas"] = df_merge["valor_vendas"].fillna(0)

    grupos = []
    for (marketplace, pedido), grupo in df_merge.groupby(["marketplace", "numero_pedido"]):
        data_pedido = grupo["data_evento"].min() if not grupo["data_evento"].isna().all() else None

        # Valor total = valor_vendas (pode estar repetido, então pegamos o max())
        valor_total = grupo["valor_vendas"].max()

        comissao_esperada = grupo["comissao_calc"].max()
        if pd.isna(comissao_esperada):
            comissao_esperada = 0

        valor_a_receber = valor_total - comissao_esperada

        # Valor Recebido: só soma 'Repasse Normal'
        mask_rep_normal = (grupo["tipo_evento_normalizado"] == "Repasse Normal")
        valor_recebido = grupo.loc[mask_rep_normal, "repasse_liquido_evento"].max()
        if pd.isna(valor_recebido):
            valor_recebido = 0

        # Situação do pagamento (pago, pago a maior, pago a menor, nao pago)
        diferenca = valor_recebido - valor_a_receber
        if abs(diferenca) < 0.05:
            situacao_pag = "pago"
        elif diferenca > 0:
            situacao_pag = "pago a maior"
        elif valor_recebido > 0:
            situacao_pag = "pago a menor"
        else:
            situacao_pag = "nao pago"

        # Valor Descontado: soma de "Descontar Hove/Houve" + "Descontar Retroativo"
        mask_hove = (grupo["tipo_evento_normalizado"] == "Descontar Hove/Houve")
        valor_hove = grupo.loc[mask_hove, "repasse_liquido_evento"].max()
        if pd.isna(valor_hove):
            valor_hove = 0

        mask_retro = (grupo["tipo_evento_normalizado"] == "Descontar Retroativo")
        valor_retro = grupo.loc[mask_retro, "repasse_liquido_evento"].max()
        if pd.isna(valor_retro):
            valor_retro = 0

        valor_descontado = valor_hove + valor_retro

        # Desconto frete
        mask_frete = (grupo["tipo_evento_normalizado"] == "Descontar Reversa Centauro Envios")
        desconto_frete = grupo.loc[mask_frete, "repasse_liquido_evento"].max()
        if pd.isna(desconto_frete):
            desconto_frete = 0

        # Lógica para "Situação" final:
        # -> Se "Descontar Hove/Houve" != valor_total, define erro_devolucao
        erro_devolucao = False
        soma_hove = grupo.loc[mask_hove, "repasse_liquido_evento"].max()
        if pd.isna(soma_hove):
            soma_hove = 0
        if abs(soma_hove) > 0 and abs(soma_hove) != abs(valor_total):
            erro_devolucao = True

        if abs(diferenca) < 0.01 and not erro_devolucao:
            situacao_final = "Correta"
        elif erro_devolucao:
            situacao_final = "Erro Devolução"
        else:
            situacao_final = situacao_pag

        grupos.append({
            "Marketplace": marketplace,
            "CÓDIGO PEDIDO": pedido,
            "DATA PEDIDO": data_pedido,
            "VALOR TOTAL DOS PRODUTOS": valor_total,
            "Comissão Esperada": comissao_esperada,
            "Valor a Receber": valor_a_receber,
            "Valor Recebido": valor_recebido,
            "Situação do pagamento": situacao_pag,
            "Valor Descontado": valor_descontado,
            "Desconto frete": desconto_frete,
            "Situação": situacao_final
        })

    df_resumo = pd.DataFrame(grupos)

    # Verifica se df_resumo não está vazio e se tem a coluna "VALOR TOTAL DOS PRODUTOS"
    if not df_resumo.empty and "VALOR TOTAL DOS PRODUTOS" in df_resumo.columns:
        df_resumo = df_resumo[df_resumo["VALOR TOTAL DOS PRODUTOS"] != 0].copy()

    return df_resumo


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
        "Outros",
        "Desconhecido"  # para eventos nulos
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
    df = carregar_dados_geral()
    # 1) Verificação de comissão (com tolerância 0.05)
    df["erro_comissao"] = df.apply(checar_erro_comissao, axis=1)

    # 2) Verificar "Descontar Hove/Houve"
    df_descontar_hove = verificar_descontar_hove(df)
    df = df.merge(df_descontar_hove, on="numero_pedido", how="left")
    if 'erro_descontar' not in df.columns:
        df['erro_descontar'] = ''

    # 3) Erros adicionais (lista de erros)
    df["lista_erros"] = df.apply(checar_erros_adicionais, axis=1)

    # 4) Filtros iniciais
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

    # 6) Carrega df_vendas (cacheado) para uso geral
    df_vendas = carregar_vendas()

    # 7) Abas
    tab1, tab2, tab3, tab4 = st.tabs([
        "Visão Geral",
        "Resumo Financeiro",
        "Erros de Descontar Hove/Houve",
        "Gráficos",
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
        df_err_devolucao = df_filtrado[df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO"].drop_duplicates(subset=["numero_pedido"])
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

        # ---------------------------------------------------------
        # Visão Geral "Anymarket"
        # ---------------------------------------------------------
        st.markdown("## Visão Geral Anymarket")

        df_any = df_filtrado.merge(
            df_vendas,
            how="left",
            left_on="sku_marketplace_id",
            right_on="sku_marketplace_id",
            suffixes=("", "_vendas")
        )

        # Substituir NaN em valor_vendas por 0 => venda não encontrada
        df_any["valor_vendas"] = df_any["valor_vendas"].fillna(0)

        def checar_erros_anymarket(row):
            erros = []
            # Erro se não encontrar a venda (valor_vendas == 0)
            if row["valor_vendas"] == 0:
                erros.append("ERRO_VENDA_NAO_ENCONTRADA")

            # Se for Repasse Normal e os valores divergem
            if (
                row["tipo_evento_normalizado"] == "Repasse Normal"
                and round(row["valor_liquido"], 2) != round(row["valor_vendas"], 2)
                and row["valor_vendas"] != 0
            ):
                erros.append("ERRO_VALORES_DIVERGENTES")

            return erros

        df_any["erros_anymarket"] = df_any.apply(checar_erros_anymarket, axis=1)

        st.subheader("Filtrar por Erros Anymarket")
        error_options = [
            "Todos",
            "SEM_ERRO",
            "ERRO_VENDA_NAO_ENCONTRADA",
            "ERRO_VALORES_DIVERGENTES"
        ]
        selected_any_error = st.selectbox("Selecione o tipo de erro a exibir", error_options, index=0)

        colunas_any = [
            "numero_pedido",
            "tipo_evento_normalizado",
            "valor_liquido",
            "valor_vendas",
            "erros_anymarket"
        ]
        df_any_exibe = df_any[colunas_any].copy()

        df_any_exibe = df_any_exibe.rename(columns={
            "numero_pedido": "Número do Pedido",
            "valor_liquido": "Valor (sku_marketplace)",
            "valor_vendas": "Valor (vendas)",
            "tipo_evento_normalizado": "Tipo de Evento",
            "erros_anymarket": "Erros Anymarket"
        })

        # Remove duplicatas
        df_any_exibe = df_any_exibe.drop_duplicates(subset=["Número do Pedido", "Tipo de Evento"])

        df_any_exibe["ErrosStr"] = df_any_exibe["Erros Anymarket"].apply(lambda lista: ",".join(lista) if lista else "")

        if selected_any_error == "SEM_ERRO":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"] == ""]
        elif selected_any_error == "ERRO_VENDA_NAO_ENCONTRADA":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"].str.contains("ERRO_VENDA_NAO_ENCONTRADA")]
        elif selected_any_error == "ERRO_VALORES_DIVERGENTES":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"].str.contains("ERRO_VALORES_DIVERGENTES")]
        # "Todos" => não filtra

        df_any_exibe_style = df_any_exibe.style.format({
            "Valor (sku_marketplace)": "{:.2f}",
            "Valor (vendas)": "{:.2f}"
        })
        st.dataframe(df_any_exibe_style)

        todas_ocorrencias_any = []
        for lista_e in df_any_exibe["Erros Anymarket"]:
            todas_ocorrencias_any.extend(lista_e)

        qtd_erro_venda_nao_encontrada = sum(e == "ERRO_VENDA_NAO_ENCONTRADA" for e in todas_ocorrencias_any)
        qtd_erro_valores_diverg = sum(e == "ERRO_VALORES_DIVERGENTES" for e in todas_ocorrencias_any)

        colA1, colA2 = st.columns(2)
        colA1.metric("ERRO_VENDA_NAO_ENCONTRADA", qtd_erro_venda_nao_encontrada)
        colA2.metric("ERRO_VALORES_DIVERGENTES", qtd_erro_valores_diverg)

        st.info(f"{len(df_any_exibe)} registro(s) exibidos na 'Visão Geral Anymarket'")

        # ---------------------------------------------------------
        # Erros de Comissão
        # ---------------------------------------------------------
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
    # ABA 2: RESUMO FINANCEIRO
    # ---------------------------------------------------------------------
    with tab2:
        st.markdown("## Resumo Financeiro")

        df_financeiro = montar_resumo_financeiro(df_filtrado, df_vendas)

        # Filtro de situação na sidebar
        st.sidebar.header("Filtro Situação Resumo Financeiro")
        situacoes_disponiveis = ["Correta", "pago", "pago a maior", "pago a menor", "nao pago", "Erro Devolução"]
        filtro_situacao = st.sidebar.multiselect("Situação:", situacoes_disponiveis)

        if filtro_situacao:
            df_financeiro = df_financeiro[df_financeiro["Situação"].isin(filtro_situacao)]

        if df_financeiro.empty:
            st.info("Nenhum dado no Resumo Financeiro (verifique filtros ou valor_total=0).")
        else:
            df_financeiro["DATA PEDIDO"] = pd.to_datetime(df_financeiro["DATA PEDIDO"], errors="coerce")

            st.dataframe(
                df_financeiro.style.format(
                    {
                        "VALOR TOTAL DOS PRODUTOS": "{:.2f}",
                        "Comissão Esperada": "{:.2f}",
                        "Valor a Receber": "{:.2f}",
                        "Valor Recebido": "{:.2f}",
                        "Valor Descontado": "{:.2f}",
                        "Desconto frete": "{:.2f}",
                    }
                )
            )

            # --- Exibe somatórios:
            total_valor_a_receber = df_financeiro["Valor a Receber"].sum()
            total_valor_recebido = df_financeiro["Valor Recebido"].sum()
            diferenca = total_valor_a_receber - total_valor_recebido

            st.markdown("### Totais")
            colS1, colS2, colS3 = st.columns(3)
            colS1.metric("Total Valor a Receber", f"{total_valor_a_receber:,.2f}")
            colS2.metric("Total Valor Recebido", f"{total_valor_recebido:,.2f}")
            colS3.metric("Diferença", f"{diferenca:,.2f}")

    # ---------------------------------------------------------------------
    # ABA 3: ERROS DE DESCONTAR HOVE/HOUVE
    # ---------------------------------------------------------------------
    with tab3:
        st.markdown("## Erros de Descontar Hove/Houve")
        df_descontar_hove_erro = df_filtrado[
            (df_filtrado["tipo_evento_normalizado"] == "Descontar Hove/Houve") &
            (df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO")
        ].drop_duplicates(subset=["numero_pedido"])

        colunas_descontar_hove = [
            "numero_pedido",
            "valor_liquido",
            "repasse_liquido_evento",
            "tipo_evento",
            "data_evento",
            "data_ciclo"
        ]
        if not df_descontar_hove_erro.empty:
            df_descontar_hove_erro = df_descontar_hove_erro[colunas_descontar_hove]
            df_descontar_hove_erro = df_descontar_hove_erro.rename(columns={
                "numero_pedido": "Número do Pedido",
                "valor_liquido": "Valor Pedido",
                "repasse_liquido_evento": "Valor Final",
                "tipo_evento": "Tipo de Evento",
                "data_evento": "Data do Pedido",
                "data_ciclo": "Data do Ciclo"
            })

            df_descontar_hove_erro["Diferença"] = (
                df_descontar_hove_erro["Valor Final"] + df_descontar_hove_erro["Valor Pedido"]
            )

            def color_diff(val):
                color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                return f'color: {color}'

            styled_df_hove = df_descontar_hove_erro.style.format({
                "Valor Pedido": "{:.2f}",
                "Valor Final": "{:.2f}",
                "Diferença": "{:.2f}"
            }).applymap(color_diff, subset=["Diferença"])

            st.error(f"{len(df_descontar_hove_erro)} registro(s) com erro de Descontar Hove/Houve.")
            st.dataframe(styled_df_hove)
        else:
            st.info("Nenhum erro de Descontar Hove/Houve com base nos filtros.")

    # ---------------------------------------------------------------------
    # ABA 4: GRÁFICOS
    # ---------------------------------------------------------------------
    with tab4:
        st.markdown("## Gráficos e Visualizações")

        st.subheader("Distribuição de Tipo de Evento")
        if not df_filtrado.empty:
            cont_eventos = df_filtrado["tipo_evento_normalizado"].value_counts()
            st.bar_chart(cont_eventos)
        else:
            st.info("Sem dados para exibir na distribuição de Tipo de Evento.")

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
