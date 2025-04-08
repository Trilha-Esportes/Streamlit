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
    Lê dados de diversas tabelas do banco:
    - sku_marketplace (sm)
    - marketplaces (mk)
    - comissoes_pedido (cp)
    - vendas (v)
    - evento_centauro (ec)
    
    Faz um LEFT JOIN para cada tabela, consolidando:
      - marketplace (nome)
      - sku_marketplace_id (ligado à tabela `sku_marketplace`)
      - número do pedido
      - valor_liquido (do pedido) vem de 'vendas'
      - data e porcentagem da comissão (de comissoes_pedido)
      - cálculo da comissão = porcentagem * valor_liquido
      - tipo de evento e valor_final (repasse_liquido_evento) vindos de 'evento_centauro'
      - data do pedido (de vendas)
      - data do repasse (data_ciclo) do evento_centauro

    Depois, preenche valores nulos com zero ou strings vazias,
    e normaliza o tipo_evento para valores padronizados (Repasse Normal, etc.).
    Retorna um DataFrame pronto para ser exibido/filtrado.
    """
    query = text("""
        SELECT
            mk.nome AS marketplace,
            sm.id AS sku_marketplace_id,
            sm.numero_pedido,

            -- Valor do pedido (universal) buscado da tabela vendas:
            COALESCE(v.valor_liquido, 0) AS valor_liquido,

            -- Data e porcentagem da comissão:
            cp.data AS data_comissao,
            cp.porcentagem,

            -- Cálculo da comissão baseado no valor de 'vendas':
            (cp.porcentagem * COALESCE(v.valor_liquido, 0)) AS comissao_calc,

            -- Informações de evento (centauro):
            ec.tipo_evento,
            COALESCE(ec.repasse_liquido_evento, 0) AS valor_final,
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

    # Preenche valores nulos em colunas-chave
    df["valor_liquido"] = df["valor_liquido"].fillna(0)
    df["valor_final"] = df["valor_final"].fillna(0)
    df["porcentagem"] = df["porcentagem"].fillna(0)
    df["tipo_evento"] = df["tipo_evento"].fillna("")

    # Cria uma coluna de tipo_evento_normalizado para unificar valores semelhantes.
    df["tipo_evento_normalizado"] = df["tipo_evento"].apply(normalizar_tipo_evento)

    return df


def normalizar_tipo_evento(evento: str) -> str:
    """
    Converte diferentes variações de strings de evento em formatos padronizados.
    Exemplo: "repasse - normal" ou "Repassse Normal" => "Repasse Normal".
    Se não estiver no mapeamento, retorna "Outros" ou "Desconhecido" se vazio.
    """
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


def checar_erro_comissao(row: pd.Series) -> str:
    """
    Checa se, para eventos de 'Repasse Normal', a diferença
    entre (valor_liquido - comissão) e valor_final é maior que 0.05.
    Se for, retorna "ERRO", senão retorna string vazia.
    """
    if row["tipo_evento_normalizado"] != "Repasse Normal":
        return ""

    # Se não houver porcentagem, não conseguimos verificar
    if pd.isnull(row["porcentagem"]) or row["porcentagem"] == 0:
        return ""

    # Valor base do pedido:
    vl_liquido = round(row["valor_liquido"], 2)
    # Valor efetivamente repassado (evento_centauro):
    vl_final = round(row["valor_final"], 2)
    porcent = round(row["porcentagem"], 4)

    # Valor calculado após a comissão:
    valor_calc = round(vl_liquido - (vl_liquido * porcent), 2)

    # Se a diferença for maior que 5 centavos, consideramos um erro.
    if abs(valor_calc - vl_final) > 0.05:
        return "ERRO"
    else:
        return ""


def checar_erros_adicionais(row: pd.Series) -> list:
    """
    Identifica erros adicionais:
    - Valor final negativo
    - Falta de comissão
    - Falta de data de comissão
    - Se erro_comissao == "ERRO"
    - Se 'erro_descontar' == "ERRO_DEVOLUCAO"

    Retorna uma lista de erros encontrados.
    """
    erros = []
    if row["tipo_evento_normalizado"] == "Repasse Normal":
        # Valor final negativo não deveria ocorrer em um repasse normal
        if row["valor_final"] < 0:
            erros.append("Valor Final Negativo")

        # Se porcentagem = 0, significa que não há comissão configurada
        if row["porcentagem"] == 0:
            erros.append("Falta de Comissão")

        # Se data de comissão não existe, podemos marcar como erro
        if pd.isnull(row["data_comissao"]):
            erros.append("Falta de Data de Comissão")

        # Se a checagem de comissão detectou erro
        if row["erro_comissao"] == "ERRO":
            erros.append("Erro Cálculo Comissão")

    # Se a verificação de "Descontar Hove/Houve" detectou divergência
    if "erro_descontar" in row and row["erro_descontar"] == "ERRO_DEVOLUCAO":
        erros.append("Erro Devolução")

    return erros


def filtrar_por_erros(df: pd.DataFrame, erros_selecionados: list) -> pd.DataFrame:
    """
    Filtra o DataFrame para manter somente as linhas que contenham
    ao menos um dos erros selecionados na coluna 'lista_erros'.
    Se erros_selecionados for vazio, retorna o df original.
    """
    if not erros_selecionados:
        return df
    mask = df["lista_erros"].apply(lambda lista: any(e in lista for e in erros_selecionados))
    return df[mask]


def verificar_descontar_hove(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica se, para "Repasse Normal" + "Descontar Hove/Houve",
    o valor repassado no Repasse Normal (valor_liquido) é igual
    (em valor absoluto) ao valor do evento "Descontar Hove/Houve" (valor_final).
    
    Se não bater, marca 'erro_descontar' = "ERRO_DEVOLUCAO".
    Retorna um DataFrame auxiliar com as colunas:
      - numero_pedido
      - valor_liquido_repasse_normal
      - repasse_liquido_evento_descontar_houve
      - erro_descontar
    """
    subset = df[["numero_pedido", "tipo_evento_normalizado",
                 "valor_liquido", "valor_final"]].copy()

    grupos = []
    # Agrupa por pedido para verificar se existe um "Repasse Normal" e um "Descontar Hove/Houve"
    for pedido, grupo in subset.groupby("numero_pedido"):
        valor_liquido_repasse_normal = None
        repasse_hove = None

        for _, row in grupo.iterrows():
            if row["tipo_evento_normalizado"] == "Repasse Normal":
                valor_liquido_repasse_normal = row["valor_liquido"]
            elif row["tipo_evento_normalizado"] == "Descontar Hove/Houve":
                repasse_hove = row["valor_final"]

        # Se ambos existem, checamos se bate
        if (valor_liquido_repasse_normal is not None) and (repasse_hove is not None):
            if round(abs(valor_liquido_repasse_normal), 2) != round(abs(repasse_hove), 2):
                erro = "ERRO_DEVOLUCAO"
            else:
                erro = ""
            grupos.append({
                "numero_pedido": pedido,
                "valor_liquido_repasse_normal": valor_liquido_repasse_normal,
                "repasse_liquido_evento_descontar_houve": repasse_hove,
                "erro_descontar": erro
            })

    df_result = pd.DataFrame(grupos, columns=[
        "numero_pedido",
        "valor_liquido_repasse_normal",
        "repasse_liquido_evento_descontar_houve",
        "erro_descontar"
    ])
    # Garante que cada pedido apareça só uma vez
    df_result = df_result.drop_duplicates(subset=["numero_pedido"])
    return df_result


def verificar_descontar_retroativo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica se, para "Descontar Retroativo", a soma de valor_final
    (repasse_liquido_evento) é igual (em valor absoluto) ao valor_liquido do pedido.
    Se for igual, marca "ERRO_DESCONTAR_RETROATIVO".
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
        "valor_liquido": "first",  # valor base do pedido
        "valor_final": "sum"       # soma dos valores "Descontar Retroativo"
    }).reset_index()

    grouped.rename(columns={"valor_final": "soma_descontar_retroativo"}, inplace=True)
    grouped["Diferenca"] = grouped["valor_liquido"] + grouped["soma_descontar_retroativo"]

    def verificar_erro(row):
        # Se a soma de desconto for igual ao valor do pedido (sem ser 0), marcamos erro
        if (round(abs(row["soma_descontar_retroativo"]), 2) == round(abs(row["valor_liquido"]), 2)
           and round(row["valor_liquido"], 2) != 0):
            return "ERRO_DESCONTAR_RETROATIVO"
        return ""

    grouped["erro_descontar_retroativo"] = grouped.apply(verificar_erro, axis=1)
    return grouped


@st.cache_data
def carregar_vendas() -> pd.DataFrame:
    """
    Retorna um DataFrame com as vendas (id, sku_marketplace_id, valor_liquido).
    Aqui chamamos de valor_vendas para evitar confusão.
    """
    query_vendas = text("""
        SELECT
            id AS venda_id,
            sku_marketplace_id,
            valor_liquido AS valor_vendas
        FROM vendas
    """)
    return pd.read_sql(query_vendas, engine)


def montar_resumo_financeiro(df_geral: pd.DataFrame, df_vendas: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna um DF consolidado para exibir em "Resumo Financeiro", com as colunas:
      - Marketplace
      - CÓDIGO PEDIDO
      - DATA PEDIDO
      - VALOR TOTAL DOS PRODUTOS
      - Comissão Esperada
      - Valor a Receber
      - Valor Recebido
      - Valor Descontado (Hove/Houve + Retroativo)
      - Desconto frete
      - Situação do pagamento
      - Situação final

    Lógica:
      - 'valor_vendas' é o valor do pedido obtido da tabela 'vendas'.
      - 'comissao_esperada' é o maior comissao_calc do grupo para aquele pedido.
      - 'valor_a_receber' = valor_total - comissao_esperada
      - 'valor_recebido' = o max() de valor_final onde tipo_evento_normalizado = "Repasse Normal"
      - 'valor_descontado' = soma de Hove/Houve + Retroativo
      - 'desconto_frete' = soma de "Descontar Reversa Centauro Envios"
      - 'situacao_pagamento' = "pago", "pago a maior", "pago a menor" ou "nao pago"
      - 'situacao_final' = "Correta", "Erro Devolução" ou a situacao do pagamento
    """
    # Mesclamos df_geral com df_vendas para obter a coluna "valor_vendas"
    df_merge = df_geral.merge(
        df_vendas,
        how="left",
        left_on="sku_marketplace_id",
        right_on="sku_marketplace_id",
        suffixes=("", "_vendas")
    )
    # Se não encontrar valor_vendas, consideramos 0
    df_merge["valor_vendas"] = df_merge["valor_vendas"].fillna(0)

    grupos = []
    # Agrupamos por (marketplace, numero_pedido)
    for (marketplace, pedido), grupo in df_merge.groupby(["marketplace", "numero_pedido"]):
        # Data do pedido é a menor data_evento do grupo (ou None, se não existir)
        data_pedido = grupo["data_evento"].min() if not grupo["data_evento"].isna().all() else None

        # O valor total do pedido (max no grupo, geralmente é igual em todas as linhas)
        valor_total = grupo["valor_vendas"].max()

        # Comissão esperada (maior valor de comissao_calc)
        comissao_esperada = grupo["comissao_calc"].max()
        if pd.isna(comissao_esperada):
            comissao_esperada = 0

        valor_a_receber = valor_total - comissao_esperada

        # Valor Recebido (Repasse Normal)
        mask_rep_normal = (grupo["tipo_evento_normalizado"] == "Repasse Normal")
        valor_recebido = grupo.loc[mask_rep_normal, "valor_final"].max()
        if pd.isna(valor_recebido):
            valor_recebido = 0

        # Determina a situação do pagamento com base na diferença
        diferenca = valor_recebido - valor_a_receber
        if abs(diferenca) < 0.05:
            situacao_pag = "pago"
        elif diferenca > 0:
            situacao_pag = "pago a maior"
        elif valor_recebido > 0:
            situacao_pag = "pago a menor"
        else:
            situacao_pag = "nao pago"

        # Valor Descontado = soma de "Descontar Hove/Houve" e "Descontar Retroativo"
        mask_hove = (grupo["tipo_evento_normalizado"] == "Descontar Hove/Houve")
        # Neste exemplo, usamos max() para "Descontar Hove/Houve" (pode haver variações).
        valor_hove = grupo.loc[mask_hove, "valor_final"].max()
        if pd.isna(valor_hove):
            valor_hove = 0

        mask_retro = (grupo["tipo_evento_normalizado"] == "Descontar Retroativo")
        # Aqui, somamos todas as linhas de retroativo
        valor_retro = grupo.loc[mask_retro, "valor_final"].sum()
        valor_descontado = valor_hove + valor_retro

        # Desconto de frete => soma de "Descontar Reversa Centauro Envios"
        mask_frete = (grupo["tipo_evento_normalizado"] == "Descontar Reversa Centauro Envios")
        desconto_frete = grupo.loc[mask_frete, "valor_final"].sum()

        # Checamos se há erro de devolução, assumindo que se "Descontar Hove/Houve" for != valor_total, há erro
        erro_devolucao = False
        if abs(valor_hove) > 0 and abs(valor_hove) != abs(valor_total):
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

    # Remove do resumo linhas que tenham "VALOR TOTAL DOS PRODUTOS" = 0, caso existam
    if not df_resumo.empty and "VALOR TOTAL DOS PRODUTOS" in df_resumo.columns:
        df_resumo = df_resumo[df_resumo["VALOR TOTAL DOS PRODUTOS"] != 0].copy()

    return df_resumo


# =========================================================================
# 4. Streamlit
# =========================================================================
# Aqui construímos as abas, filtros e a UI do Streamlit.

def main():
    """
    Função principal do Streamlit:
    - Exibe o título do painel
    - Cria sidebar com filtros
    - Carrega e filtra os dados
    - Mostra diversas abas para visualização:
        1) Visão Geral
        2) Resumo Financeiro
        3) Erros de Descontar Hove/Houve
        4) Gráficos
    """
    st.title("Painel de Análises e Filtros (Com Data/Ciclo)")

    # ------------------- SIDEBAR: Filtros -------------------
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
        "Desconhecido"  # para eventos nulos ou vazios
    ]
    evento_filtro = st.sidebar.multiselect(
        "Selecione o(s) Tipo(s) de Evento:",
        tipos_evento_padronizados,
        default=tipos_evento_padronizados
    )

    # Filtros de datas (Data inicial e Data final para data_comissao)
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

    # ------------------- 1) CARREGAR DADOS -------------------
    df = carregar_dados_geral()

    # 1) Verificação de comissão => cria coluna "erro_comissao"
    df["erro_comissao"] = df.apply(checar_erro_comissao, axis=1)

    # 2) Verificar "Descontar Hove/Houve" => data frame auxiliar
    df_descontar_hove = verificar_descontar_hove(df)
    df = df.merge(df_descontar_hove, on="numero_pedido", how="left")
    if 'erro_descontar' not in df.columns:
        # Se não houver, cria a coluna
        df['erro_descontar'] = ''

    # 3) Erros adicionais => cria coluna "lista_erros"
    df["lista_erros"] = df.apply(checar_erros_adicionais, axis=1)

    # 4) Aplica os filtros iniciais => copia para df_filtrado
    df_filtrado = df.copy()

    # --- Filtro por Número do Pedido
    if pedido_filtro:
        df_filtrado = df_filtrado[df_filtrado["numero_pedido"].astype(str).str.contains(pedido_filtro, na=False)]

    # --- Filtro por Tipo de Evento
    if evento_filtro:
        df_filtrado = df_filtrado[df_filtrado["tipo_evento_normalizado"].isin(evento_filtro)]

    # --- Filtro por Data de Comissão
    if data_ini and data_fim:
        df_filtrado["data_comissao"] = pd.to_datetime(df_filtrado["data_comissao"], errors="coerce")
        df_filtrado = df_filtrado[
            (df_filtrado["data_comissao"].notnull()) &
            (df_filtrado["data_comissao"] >= pd.to_datetime(data_ini)) &
            (df_filtrado["data_comissao"] <= pd.to_datetime(data_fim))
        ]

    # --- Filtro por erros selecionados
    df_filtrado = filtrar_por_erros(df_filtrado, erros_selecionados)

    # 6) Carrega df_vendas (cacheado), se precisarmos comparar
    df_vendas = carregar_vendas()

    # 7) Cria as abas do Streamlit
    tab1, tab2, tab3, tab4 = st.tabs([
        "Visão Geral",
        "Resumo Financeiro",
        "Erros de Descontar Hove/Houve",
        "Gráficos",
    ])

    # ------------------- ABA 1: VISÃO GERAL -------------------
    with tab1:
        st.markdown("## Visão Geral dos Dados Filtrados")

        # Selecionamos só algumas colunas para exibir
        colunas_visao_geral = [
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
        df_visao_geral = df_filtrado[colunas_visao_geral].copy()

        # Renomeia para exibição
        df_visao_geral = df_visao_geral.rename(columns={
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

        # Aplica formatação decimal em algumas colunas
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

        # Soma usando valor_final
        soma_val_final = df_filtrado["valor_final"].sum()

        colA, colB, colC, colD = st.columns(4)
        colA.metric("Qtd. Registros (filtro)", qtd_total)
        colB.metric("Erros de Comissão", qtd_erro_comissao)
        colC.metric("Erros de Devolução", qtd_erro_devolucao)
        colD.metric("Soma Valor Final", f"{soma_val_final:,.2f}")

        # Quantos registros têm qualquer erro na lista_erros
        df_filtrado["tem_algum_erro"] = df_filtrado["lista_erros"].apply(lambda x: len(x) > 0)
        qtd_qualquer_erro = df_filtrado["tem_algum_erro"].sum()
        st.info(f"Registros com *qualquer erro*: {qtd_qualquer_erro}")

        # ---------------------------------------------------------
        # Visão Geral "Anymarket"
        # ---------------------------------------------------------
        st.markdown("## Visão Geral Anymarket")

        # Mescla com df_vendas para verificar possíveis divergências
        df_any = df_filtrado.merge(
            df_vendas,
            how="left",
            left_on="sku_marketplace_id",
            right_on="sku_marketplace_id",
            suffixes=("", "_vendas")
        )

        # Substitui NaN em valor_vendas (significa que não achou a venda)
        df_any["valor_vendas"] = df_any["valor_vendas"].fillna(0)

        def checar_erros_anymarket(row):
            """
            - Se não encontrar a venda (valor_vendas == 0), retorna ERRO_VENDA_NAO_ENCONTRADA
            - Se for Repasse Normal e valor_liquido != valor_vendas, retorna ERRO_VALORES_DIVERGENTES
            """
            erros = []
            if row["valor_vendas"] == 0:
                erros.append("ERRO_VENDA_NAO_ENCONTRADA")

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
            "valor_liquido": "Valor (sku_marketplace/vendasDF)",
            "valor_vendas": "Valor (vendas)",
            "tipo_evento_normalizado": "Tipo de Evento",
            "erros_anymarket": "Erros Anymarket"
        })

        # Remove duplicatas que podem ocorrer
        df_any_exibe = df_any_exibe.drop_duplicates(subset=["Número do Pedido", "Tipo de Evento"])

        # Cria uma coluna para exibição fácil dos erros
        df_any_exibe["ErrosStr"] = df_any_exibe["Erros Anymarket"].apply(lambda lista: ",".join(lista) if lista else "")

        # Filtra de acordo com a escolha do usuário
        if selected_any_error == "SEM_ERRO":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"] == ""]
        elif selected_any_error == "ERRO_VENDA_NAO_ENCONTRADA":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"].str.contains("ERRO_VENDA_NAO_ENCONTRADA")]
        elif selected_any_error == "ERRO_VALORES_DIVERGENTES":
            df_any_exibe = df_any_exibe[df_any_exibe["ErrosStr"].str.contains("ERRO_VALORES_DIVERGENTES")]
        # Se "Todos", não filtramos

        df_any_exibe_style = df_any_exibe.style.format({
            "Valor (sku_marketplace/vendasDF)": "{:.2f}",
            "Valor (vendas)": "{:.2f}"
        })
        st.dataframe(df_any_exibe_style)

        # Exibe métricas de quantos erros foram encontrados
        todas_ocorrencias_any = []
        for lista_e in df_any_exibe["Erros Anymarket"]:
            todas_ocorrencias_any.extend(lista_e)

        qtd_erro_venda_nao_encontrada = sum(e == "ERRO_VENDA_NAO_ENCONTRADA" for e in todas_ocorrencias_any)
        qtd_erro_valores_diverg = sum(e == "ERRO_VALORES_DIVERGENTES" for e in todas_ocorrencias_any)

        colA1, colA2 = st.columns(2)
        colA1.metric("ERRO_VENDA_NAO_ENCONTRADA", qtd_erro_venda_nao_encontrada)
        colA2.metric("ERRO_VALORES_DIVERGENTES", qtd_erro_valores_diverg)

        st.info(f"{len(df_any_exibe)} registro(s) exibidos na 'Visão Geral Anymarket'")

       
    # ---------------------------------------------------------------------
    # ABA 2: RESUMO FINANCEIRO
    # ---------------------------------------------------------------------
    with tab2:
        st.markdown("## Resumo Financeiro")

        # Montamos o DF usando a função acima
        df_financeiro = montar_resumo_financeiro(df_filtrado, df_vendas)

        # Filtro adicional de Situação (opcional)
        st.sidebar.header("Filtro Situação Resumo Financeiro")
        situacoes_disponiveis = ["Correta", "pago", "pago a maior", "pago a menor", "nao pago", "Erro Devolução"]
        filtro_situacao = st.sidebar.multiselect("Situação:", situacoes_disponiveis)

        if filtro_situacao:
            df_financeiro = df_financeiro[df_financeiro["Situação"].isin(filtro_situacao)]

        if df_financeiro.empty:
            st.info("Nenhum dado no Resumo Financeiro (verifique filtros ou valor_total=0).")
        else:
            # Converter DATA PEDIDO para datetime, para exibição ou filtragem
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

            # Exibe somatórios
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

        # Filtra pedidos que realmente deram erro de devolução
        df_descontar_hove_erro = df_filtrado[
            (df_filtrado["tipo_evento_normalizado"] == "Descontar Hove/Houve") &
            (df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO")
        ].drop_duplicates(subset=["numero_pedido"])

        colunas_descontar_hove = [
            "numero_pedido",
            "valor_liquido",
            "valor_final",
            "tipo_evento",
            "data_evento",
            "data_ciclo"
        ]
        if not df_descontar_hove_erro.empty:
            df_descontar_hove_erro = df_descontar_hove_erro[colunas_descontar_hove]
            # Renomeamos para exibição
            df_descontar_hove_erro = df_descontar_hove_erro.rename(columns={
                "numero_pedido": "Número do Pedido",
                "valor_liquido": "Valor Pedido",
                "valor_final": "Valor Final",
                "tipo_evento": "Tipo de Evento",
                "data_evento": "Data do Pedido",
                "data_ciclo": "Data do Ciclo"
            })

            # Exemplo de cálculo de diferença
            df_descontar_hove_erro["Diferença"] = (
                df_descontar_hove_erro["Valor Final"] + df_descontar_hove_erro["Valor Pedido"]
            )

            # Função para colorir a diferença
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

        # 1) Gráfico de Barras: distribuição de tipo de evento
        st.subheader("Distribuição de Tipo de Evento")
        if not df_filtrado.empty:
            cont_eventos = df_filtrado["tipo_evento_normalizado"].value_counts()
            st.bar_chart(cont_eventos)
        else:
            st.info("Sem dados para exibir na distribuição de Tipo de Evento.")

        # 2) Gráfico de Erros (Barrinhas e Pizza)
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
            ax.axis("equal")  # Mantém o círculo perfeito
            st.pyplot(fig)


if __name__ == "__main__":
    main()
