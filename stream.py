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

    # Filtra colunas que você **quer** exibir
    colunas_desejadas = [
        "numero_pedido",
        "valor_liquido",
        "valor_final",
        "data_comissao",
        "porcentagem",
        "comissao_calc",
        "tipo_evento",
        "repasse_liquido_evento",
        "data_evento",
        "data_ciclo"
    ]
    df = df[colunas_desejadas]

    # Normalização dos tipos de evento
    df["tipo_evento_normalizado"] = df["tipo_evento"].apply(
        normalizar_tipo_evento)

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
        "repassse normal": "Repasse Normal",  # Corrigindo possível erro de digitação
        "repassse - normal": "Repasse Normal",  # Corrigindo possível erro de digitação
        "descontar hove": "Descontar Hove/Houve",
        "descontar houve": "Descontar Hove/Houve",
        "descontar - houve": "Descontar Hove/Houve",
        "descontar - hove": "Descontar Hove/Houve",
        "descontar reversa centauro envios": "Descontar Reversa Centauro Envios",
        "descontar - reversa centauro envios": "Descontar Reversa Centauro Envios",
        "ajuste de ciclo": "Ajuste de Ciclo"
    }

    return mapping.get(evento, "Outros")


def checar_erro_comissao(row):
    """
    Verifica se, para eventos de 'Repasse Normal',
    o valor_final está correto com base na porcentagem de comissão.
    Retorna "ERRO" se houver discrepância, senão "".
    """
    if row["tipo_evento_normalizado"] != "Repasse Normal":
        return ""  # Ignorar se não for repasse normal

    if pd.isnull(row["porcentagem"]):
        return ""  # Sem porcentagem => não valida

    vl_liquido = round(row["valor_liquido"], 2)
    vl_final = round(row["valor_final"], 2)
    porcent = round(row["porcentagem"], 4)

    valor_calc = round(vl_liquido - (vl_liquido * porcent), 2)

    if valor_calc != vl_final:
        return "ERRO"
    else:
        return ""


def verificar_descontar_hove(df):
    """
    Verifica, para cada numero_pedido que tenha eventos 'Repasse Normal' e
    'Descontar Hove/Houve',
    se o |valor_liquido| da 'Repasse Normal' é diferente do |repasse_liquido_evento| da 'Descontar Hove/Houve'.
    Se diferente, marca como "ERRO_DEVOLUCAO".
    Retorna um DataFrame com os resultados.
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
            # Comparação dos valores absolutos arredondados para 2 casas
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

    # Garantir que o DataFrame retorne as colunas mesmo que 'grupos' esteja vazio
    df_result = pd.DataFrame(grupos, columns=[
        "numero_pedido",
        "valor_liquido_repasse_normal",
        "repasse_liquido_evento_descontar_houve",
        "erro_descontar"
    ])

    # Remover duplicatas se existirem
    df_result = df_result.drop_duplicates(subset=["numero_pedido"])

    return df_result


def checar_erros_adicionais(row):
    """
    Lista de possíveis erros adicionais:
      - Valor Final Negativo (para repasse normal)
      - Falta de Comissão
      - Falta de Data de Comissão
      - Se checar_erro_comissao = "ERRO", marcamos "Erro Cálculo Comissão"
      - Se erro_descontar = "ERRO_DEVOLUCAO", marcamos "Erro Devolução"

    Retorna uma lista com os erros encontrados.
    """
    erros = []
    if row["tipo_evento_normalizado"] == "Repasse Normal":
        # 1) Valor Final Negativo
        if row["valor_final"] < 0:
            erros.append("Valor Final Negativo")

        # 2) Falta de Comissão
        if pd.isnull(row["porcentagem"]):
            erros.append("Falta de Comissão")

        # 3) Falta de Data de Comissão
        if pd.isnull(row["data_comissao"]):
            erros.append("Falta de Data de Comissão")

        # 4) Erro no Cálculo de Comissão
        if row["erro_comissao"] == "ERRO":
            erros.append("Erro Cálculo Comissão")

    # Verificação adicional para "Descontar Hove/Houve"
    if "erro_descontar" in row and row["erro_descontar"] == "ERRO_DEVOLUCAO":
        erros.append("Erro Devolução")

    return erros


def filtrar_por_erros(df, erros_selecionados):
    """
    Retorna apenas linhas que contenham algum dos erros selecionados,
    se 'erros_selecionados' não estiver vazio.
    """
    if not erros_selecionados:
        return df  # sem filtro de erro

    mask = df["lista_erros"].apply(
        lambda lista: any(e in lista for e in erros_selecionados)
    )
    return df[mask]

# =========================================================================
# Streamlit
# =========================================================================


def main():
    st.title("Painel de Análises e Filtros (Com Data/Ciclo)")

    st.sidebar.header("Filtros de Pesquisa")
    pedido_filtro = st.sidebar.text_input("Número do Pedido:", "")

    # Filtro por Tipo de Evento
    st.sidebar.header("Filtros de Tipo de Evento")
    # Definir os tipos de evento padronizados
    tipos_evento_padronizados = [
        "Repasse Normal",
        "Descontar Hove/Houve",
        "Descontar Reversa Centauro Envios",
        "Ajuste de Ciclo",
        "Outros"  # Para eventos que não se enquadram nas categorias acima
    ]
    evento_filtro = st.sidebar.multiselect(
        "Selecione o(s) Tipo(s) de Evento:",
        tipos_evento_padronizados,
        default=tipos_evento_padronizados  # Seleciona todos por padrão
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
        "Erro Devolução"  # Novo erro adicionado
    ]
    erros_selecionados = st.sidebar.multiselect(
        "Selecione o(s) tipo(s) de erro:",
        opcoes_de_erros
    )

    # Carregar dados
    df = carregar_dados()

    # 1) Verificação de comissão
    df["erro_comissao"] = df.apply(checar_erro_comissao, axis=1)

    # 2) Verificação "Descontar Hove/Houve"
    df_descontar_hove = verificar_descontar_hove(df)

    # Merge da verificação "Descontar Hove/Houve" com o DataFrame principal
    df = df.merge(df_descontar_hove, on="numero_pedido", how="left")

    # Verificar se a coluna 'erro_descontar' existe; se não, adicioná-la com valores vazios
    if 'erro_descontar' not in df.columns:
        df['erro_descontar'] = ''

    # 3) Erros adicionais
    df["lista_erros"] = df.apply(checar_erros_adicionais, axis=1)

    # 4) Filtros
    df_filtrado = df.copy()

    # Filtrar por Número do Pedido
    if pedido_filtro:
        df_filtrado = df_filtrado[
            df_filtrado["numero_pedido"].astype(
                str).str.contains(pedido_filtro, na=False)
        ]

    # Filtrar por Tipo de Evento
    if evento_filtro:
        df_filtrado = df_filtrado[
            df_filtrado["tipo_evento_normalizado"].isin(evento_filtro)
        ]

    # Filtrar por Data de Comissão
    if data_ini and data_fim:
        # filtra pela data_comissao
        df_filtrado["data_comissao"] = pd.to_datetime(
            df_filtrado["data_comissao"], errors="coerce")
        df_filtrado = df_filtrado[
            (df_filtrado["data_comissao"].notnull()) &
            (df_filtrado["data_comissao"] >= pd.to_datetime(data_ini)) &
            (df_filtrado["data_comissao"] <= pd.to_datetime(data_fim))
        ]

    # 5) Filtra por erros selecionados
    df_filtrado = filtrar_por_erros(df_filtrado, erros_selecionados)

    # 6) Cria abas
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Visão Geral",
        "Erros de Comissão",
        "Erros de Descontar Hove/Houve",
        "Duplicatas",
        "Gráficos"
    ])

    # -------- ABA 1: VISÃO GERAL --------
    with tab1:
        st.markdown("## Visão Geral dos Dados Filtrados")

        # Selecionar as colunas especificadas
        colunas_visao_geral = [
            "numero_pedido",
            "valor_liquido",
            "repasse_liquido_evento",
            "tipo_evento",             # Adicionado aqui
            "data_evento",
            "porcentagem",
            "comissao_calc",
            "data_ciclo",
            "lista_erros"
        ]
        df_visao_geral = df_filtrado[colunas_visao_geral].copy()
        # Renomear colunas para melhor visualização
        df_visao_geral = df_visao_geral.rename(columns={
            "comissao_calc": "Comissão",
            "lista_erros": "Erros",
            "numero_pedido": "Número do Pedido",
            "valor_liquido": "Valor Pedido",
            "repasse_liquido_evento": "Valor Final",
            "tipo_evento": "Tipo de Evento",       # Renomeado aqui
            "data_evento": "Data do Pedido",
            "porcentagem": "Porcentagem",
            "data_ciclo": "Data do Ciclo"
        })

        # Identificar colunas numéricas para formatação
        colunas_numericas_visao = ["Valor Pedido", "Valor Final", "Porcentagem", "Comissão"]

        # Formatar os valores numéricos para duas casas decimais
        df_visao_geral_styled = df_visao_geral.style.format({
            "Valor Pedido": "{:.2f}",
            "Valor Final": "{:.2f}",
            "Porcentagem": "{:.2f}",
            "Comissão": "{:.2f}"
        })

        st.dataframe(df_visao_geral_styled)

        st.markdown("### Resumo de Registros")
        qtd_total = len(df_filtrado)
        qtd_erro_comissao = sum(df_filtrado["erro_comissao"] == "ERRO")
        qtd_erro_devolucao = sum(
            df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO")
        soma_val_final = df_filtrado["valor_final"].sum()

        colA, colB, colC, colD = st.columns(4)
        colA.metric("Qtd. Registros (filtro)", qtd_total)
        colB.metric("Erros de Comissão", qtd_erro_comissao)
        colC.metric("Erros de Devolução", qtd_erro_devolucao)
        colD.metric("Soma Valor Final", f"{soma_val_final:,.2f}")

        # Quantos têm algum erro adicional
        df_filtrado["tem_algum_erro"] = df_filtrado["lista_erros"].apply(
            lambda x: len(x) > 0)
        qtd_qualquer_erro = df_filtrado["tem_algum_erro"].sum()
        st.info(
            f"Registros com *qualquer erro* (negativo/falta comiss/etc.): {qtd_qualquer_erro}")

    # -------- ABA 2: ERROS DE COMISSÃO --------
    with tab2:
        st.markdown("## Erros de Comissão (Repasse Normal)")
        df_err = df_filtrado[df_filtrado["erro_comissao"] == "ERRO"]
        if df_err.empty:
            st.info("Nenhum erro de comissão com base nos filtros.")
        else:
            st.warning(f"{len(df_err)} registros com erro de comissão.")
            # Selecionar e renomear as colunas para melhor visualização
            colunas_erros_comissao = [
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
            df_err_vis = df_err[colunas_erros_comissao].copy()
            df_err_vis = df_err_vis.rename(columns={
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

            # Identificar colunas numéricas para formatação
            colunas_numericas_comissao = ["Valor Pedido", "Valor Final", "Porcentagem", "Comissão"]

            # Formatar os valores numéricos para duas casas decimais
            df_err_vis_styled = df_err_vis.style.format({
                "Valor Pedido": "{:.2f}",
                "Valor Final": "{:.2f}",
                "Porcentagem": "{:.2f}",
                "Comissão": "{:.2f}"
            })

            st.dataframe(df_err_vis_styled)

    # -------- ABA 3: ERROS DE DESCONTAR HOVE/HOUVE --------
    with tab3:
        st.markdown("## Erros de Descontar Hove/Houve")

        # Filtrar apenas os registros com tipo_evento_normalizado 'Descontar Hove/Houve' e erro_descontar 'ERRO_DEVOLUCAO'
        df_descontar_hove = df_filtrado[
            (df_filtrado["tipo_evento_normalizado"] == "Descontar Hove/Houve") &
            (df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO")
        ]

        # Selecionar apenas as colunas especificadas
        colunas_descontar_hove = [
            "numero_pedido",
            "valor_liquido",
            "repasse_liquido_evento",
            "tipo_evento",  # Adicionado aqui para renomear
            "data_evento",
            "data_ciclo"
        ]
        df_descontar_hove = df_descontar_hove[colunas_descontar_hove].drop_duplicates(subset=["numero_pedido"])
        # Renomear as colunas para melhor visualização
        df_descontar_hove = df_descontar_hove.rename(columns={
            "numero_pedido": "Número do Pedido",
            "valor_liquido": "Valor Pedido",
            "repasse_liquido_evento": "Valor Final",
            "tipo_evento": "Tipo de Evento",
            "data_evento": "Data do Pedido",
            "data_ciclo": "Data do Ciclo"
        })

        # Calcular a Diferença
        df_descontar_hove["Diferença"] = df_descontar_hove["Valor Final"] + df_descontar_hove["Valor Pedido"]

        # Identificar colunas numéricas para formatação (incluindo Diferença)
        colunas_numericas_descontar = ["Valor Pedido", "Valor Final", "Diferença"]

        # Função para aplicar cores na coluna Diferença
        def color_diff(val):
            color = 'green' if val > 0 else 'red' if val < 0 else 'black'
            return f'color: {color}'

        # Aplicar estilo à coluna Diferença e formatar os valores numéricos
        styled_df = df_descontar_hove.style.format({
            "Valor Pedido": "{:.2f}",
            "Valor Final": "{:.2f}",
            "Diferença": "{:.2f}"
        }).applymap(color_diff, subset=["Diferença"])

        if df_descontar_hove.empty:
            st.info("Nenhum erro de Descontar Hove/Houve com base nos filtros.")
        else:
            st.error(f"{len(df_descontar_hove)} registros com erro de Descontar Hove/Houve.")
            st.dataframe(styled_df)

    # -------- ABA 4: DUPLICATAS --------
    with tab4:
        st.markdown(
            "## Possíveis Duplicatas (Mesmo Pedido + Mesmo Tipo de Evento)")
        df_dups = df_filtrado.groupby(
            ["numero_pedido", "tipo_evento_normalizado"]).size().reset_index(name="count")
        df_dups = df_dups[df_dups["count"] > 1]
        if df_dups.empty:
            st.info("Nenhuma duplicata pelo critério (pedido + tipo de evento).")
        else:
            st.warning("Duplicatas encontradas:")
            # Renomear as colunas para melhor visualização
            df_dups = df_dups.rename(columns={
                "numero_pedido": "Número do Pedido",
                "tipo_evento_normalizado": "Tipo de Evento",
                "count": "Quantidade"
            })

            # Converter "Quantidade" para float para permitir duas casas decimais
            df_dups["Quantidade"] = df_dups["Quantidade"].astype(float)

            # Identificar colunas numéricas para formatação
            colunas_numericas_dups = ["Quantidade"]

            # Formatar os valores numéricos para duas casas decimais
            df_dups_styled = df_dups.style.format({
                "Quantidade": "{:.2f}"
            })

            st.dataframe(df_dups_styled)

    # -------- ABA 5: GRÁFICOS --------
    with tab5:
        st.markdown("## Gráficos e Visualizações")

        # 1) Tipo de Evento (barras)
        st.subheader("Distribuição de Tipo de Evento")
        if not df_filtrado.empty:
            cont_eventos = df_filtrado["tipo_evento_normalizado"].value_counts()
            st.bar_chart(cont_eventos)
        else:
            st.info("Sem dados para exibir.")

        # 2) Erros em Barras e Pizza
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
    # Rode com: streamlit run arquivo.py
    main()
