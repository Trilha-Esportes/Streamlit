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
        "valor_final",            # de sku_marketplace
        "repasse_liquido_evento", # de evento_centauro
        "data_comissao",
        "porcentagem",
        "comissao_calc",
        "tipo_evento",
        "data_evento",
        "data_ciclo"
    ]
    df = df[colunas_desejadas]

    # Normalização dos tipos de evento
    df["tipo_evento_normalizado"] = df["tipo_evento"].apply(
        normalizar_tipo_evento
    )

    return df


def normalizar_tipo_evento(evento):
    """
    Mapeia variantes de tipos de evento para nomes padronizados.
    Inclui as variações de 'Descontar Retroativo' também.
    """
    if pd.isnull(evento):
        return "Desconhecido"

    evento = evento.strip().lower()

    mapping = {
        # Repasse Normal
        "repasse normal": "Repasse Normal",
        "repasse - normal": "Repasse Normal",
        "repassse normal": "Repasse Normal",  # Possíveis erros de digitação
        "repassse - normal": "Repasse Normal",

        # Descontar Hove/Houve
        "descontar hove": "Descontar Hove/Houve",
        "descontar houve": "Descontar Hove/Houve",
        "descontar - houve": "Descontar Hove/Houve",
        "descontar - hove": "Descontar Hove/Houve",

        # Descontar Reversa
        "descontar reversa centauro envios": "Descontar Reversa Centauro Envios",
        "descontar - reversa centauro envios": "Descontar Reversa Centauro Envios",

        # Ajuste de ciclo
        "ajuste de ciclo": "Ajuste de Ciclo",

        # Descontar Retroativo (várias variações)
        "descontar retroativo": "Descontar Retroativo",
        "descontar - retroativo": "Descontar Retroativo",
        "descontar retroativo sac": "Descontar Retroativo",
        "descontar - retroativo sac": "Descontar Retroativo",
    }

    return mapping.get(evento, "Outros")


def checar_erro_comissao(row):
    """
    Exemplo: se quiser validar a comissão usando repasse_liquido_evento,
    troque row["valor_final"] por row["repasse_liquido_evento"] abaixo.
    """
    if row["tipo_evento_normalizado"] != "Repasse Normal":
        return ""  # Ignorar se não for repasse normal

    if pd.isnull(row["porcentagem"]):
        return ""  # Sem porcentagem => não valida

    vl_liquido = round(row["valor_liquido"], 2)
    # Se você quer comparar com 'repasse_liquido_evento', descomente a linha abaixo
    # vl_final = round(row["repasse_liquido_evento"], 2)
    vl_final = round(row["valor_final"], 2)  # MANTENDO do jeito que estava

    porcent = round(row["porcentagem"], 4)
    valor_calc = round(vl_liquido - (vl_liquido * porcent), 2)

    if valor_calc != vl_final:
        return "ERRO"
    else:
        return ""



def checar_erros_adicionais(row):
    """
    Se quiser marcar erro para repasse_liquido_evento < 0, troque row["valor_final"] por row["repasse_liquido_evento"].
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
    Retorna apenas linhas que contenham algum dos erros selecionados,
    se 'erros_selecionados' não estiver vazio.
    """
    if not erros_selecionados:
        return df  # sem filtro de erro

    mask = df["lista_erros"].apply(
        lambda lista: any(e in lista for e in erros_selecionados)
    )
    return df[mask]


def verificar_descontar_hove(df):
    """
    Verifica, para cada numero_pedido que tenha eventos 'Repasse Normal' e
    'Descontar Hove/Houve',
    se o |valor_liquido| da 'Repasse Normal' é diferente do |repasse_liquido_evento| da 'Descontar Hove/Houve'.
    Se diferente, marca como "ERRO_DEVOLUCAO".
    Retorna um DataFrame com os resultados (numero_pedido, valor_liquido_repasse_normal, repasse_liquido_evento_descontar_houve, erro_descontar).
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

    df_result = pd.DataFrame(grupos, columns=[
        "numero_pedido",
        "valor_liquido_repasse_normal",
        "repasse_liquido_evento_descontar_houve",
        "erro_descontar"
    ])

    df_result = df_result.drop_duplicates(subset=["numero_pedido"])
    return df_result

# =========================================================================
# 3. Função específica para Descontar Retroativo
# =========================================================================

def verificar_descontar_retroativo(df):
    """
    Agrupa (numero_pedido) para 'Descontar Retroativo', soma repasse_liquido_evento.
    - Se o valor absoluto dessa soma for igual ao valor_liquido do pedido (e valor_liquido != 0),
      marca como erro ("ERRO_DESCONTAR_RETROATIVO").
    - Gera coluna de Diferença = valor_liquido + soma repasse_liquido_evento
      (verde se >0, vermelho se <0).
    Retorna DataFrame com: numero_pedido, valor_liquido, soma_descontar_retroativo,
    diferença e flag de erro.
    """
    # Filtrar somente quem é "Descontar Retroativo"
    subset = df[df["tipo_evento_normalizado"] == "Descontar Retroativo"].copy()
    if subset.empty:
        return pd.DataFrame(columns=[
            "numero_pedido",
            "valor_liquido",
            "soma_descontar_retroativo",
            "Diferenca",
            "erro_descontar_retroativo"
        ])

    # Agrupar
    grouped = subset.groupby("numero_pedido").agg({
        "valor_liquido": "first",  # Pega o primeiro valor_liquido do pedido
        "repasse_liquido_evento": "sum"
    }).reset_index()

    # Renomeia a soma para ficar claro
    grouped.rename(columns={"repasse_liquido_evento": "soma_descontar_retroativo"}, inplace=True)

    # Calcula diferença
    grouped["Diferenca"] = grouped["valor_liquido"] + grouped["soma_descontar_retroativo"]

    # Função para verificar erro
    def verificar_erro(row):
        if round(abs(row["soma_descontar_retroativo"]), 2) == round(abs(row["valor_liquido"]), 2) and round(row["valor_liquido"], 2) != 0:
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
        df_filtrado = df_filtrado[
            df_filtrado["numero_pedido"].astype(str).str.contains(pedido_filtro, na=False)
        ]

    # Filtro por Tipo de Evento
    if evento_filtro:
        df_filtrado = df_filtrado[
            df_filtrado["tipo_evento_normalizado"].isin(evento_filtro)
        ]

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
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Visão Geral",
        "Erros de Comissão",
        "Erros de Descontar Hove/Houve",
        "Descontar Retroativo",
        "Duplicatas",
        "Gráficos"
    ])

    # ------------------- ABA 1: VISÃO GERAL -------------------
    with tab1:
        st.markdown("## Visão Geral dos Dados Filtrados")

        # Observação importante:
        # Aqui, vamos exibir repasse_liquido_evento no lugar de valor_final.
        # Então podemos excluir a coluna "valor_final" do "sku_marketplace"
        # e usar no DataFrame "Valor Final" = repasse_liquido_evento.

        colunas_visao_geral = [
            "numero_pedido",
            "valor_liquido",
            "repasse_liquido_evento",  # <--- substitui o valor_final
            "tipo_evento",
            "data_evento",
            "porcentagem",
            "comissao_calc",
            "data_ciclo",
            "lista_erros"
        ]
        df_visao_geral = df_filtrado[colunas_visao_geral].copy()

        # Renomeia repasse_liquido_evento para "Valor Final"
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

        st.dataframe(df_visao_geral_styled)

        # --- Resumo ---
        st.markdown("### Resumo de Registros")
        qtd_total = len(df_filtrado)
        qtd_erro_comissao = sum(df_filtrado["erro_comissao"] == "ERRO")

        # Erros de Devolução (sem duplicar pedidos)
        df_err_devolucao = df_filtrado[df_filtrado["erro_descontar"] == "ERRO_DEVOLUCAO"]
        df_err_devolucao = df_err_devolucao.drop_duplicates(subset=["numero_pedido"])
        qtd_erro_devolucao = len(df_err_devolucao)

        # Aqui soma_val_final será a soma de repasse_liquido_evento ao invés de sku_marketplace.valor_final
        soma_val_final = df_filtrado["repasse_liquido_evento"].sum()

        colA, colB, colC, colD = st.columns(4)
        colA.metric("Qtd. Registros (filtro)", qtd_total)
        colB.metric("Erros de Comissão", qtd_erro_comissao)
        colC.metric("Erros de Devolução", qtd_erro_devolucao)
        colD.metric("Soma Valor Final", f"{soma_val_final:,.2f}")

        # Quantos têm algum erro adicional
        df_filtrado["tem_algum_erro"] = df_filtrado["lista_erros"].apply(lambda x: len(x) > 0)
        qtd_qualquer_erro = df_filtrado["tem_algum_erro"].sum()
        st.info(f"Registros com *qualquer erro*: {qtd_qualquer_erro}")

    

    # ---------------------------------------------------------------------
    # ABA 2: ERROS DE COMISSÃO
    # ---------------------------------------------------------------------
    with tab2:
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
    # ABA 3: ERROS DE DESCONTAR HOVE/HOUVE
    # ---------------------------------------------------------------------
    with tab3:
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

        # Criar coluna de Diferença
        df_descontar_hove_erro["Diferença"] = df_descontar_hove_erro["Valor Final"] + df_descontar_hove_erro["Valor Pedido"]

        def color_diff(val):
            color = 'green' if val > 0 else 'red' if val < 0 else 'black'
            return f'color: {color}'

        styled_df_hove = df_descontar_hove_erro.style.format({
            "Valor Pedido": "{:.2f}",
            "Valor Final": "{:.2f}",
            "Diferença": "{:.2f}"
        }).applymap(color_diff, subset=["Diferença"])

        if df_descontar_hove_erro.empty:
            st.info("Nenhum erro de Descontar Hove/Houve com base nos filtros.")
        else:
            st.error(f"{len(df_descontar_hove_erro)} registro(s) com erro de Descontar Hove/Houve.")
            st.dataframe(styled_df_hove)

    # ---------------------------------------------------------------------
    # ABA 4: DESCONTAR RETROATIVO
    # ---------------------------------------------------------------------
    with tab4:
        st.markdown("## Verificação: Descontar Retroativo")
        # Agrupar e verificar se a soma do repasse_liquido_evento bate com o valor_liquido
        df_retroativo = verificar_descontar_retroativo(df_filtrado)

        if df_retroativo.empty:
            st.info("Nenhum registro de 'Descontar Retroativo' encontrado com base nos filtros.")
        else:
            st.markdown("### Registros de Descontar Retroativo Agrupados")

            def color_diff(val):
                color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                return f'color: {color}'

            # Renomear colunas para exibir
            df_retroativo_exibe = df_retroativo.rename(columns={
                "numero_pedido": "Número do Pedido",
                "valor_liquido": "Valor Pedido",
                "soma_descontar_retroativo": "Soma Retroativo",
                "Diferenca": "Diferença",
                "erro_descontar_retroativo": "Erro Retroativo"
            })

            # Aplicar estilo
            df_retroativo_styled = df_retroativo_exibe.style.format({
                "Valor Pedido": "{:.2f}",
                "Soma Retroativo": "{:.2f}",
                "Diferença": "{:.2f}"
            }).applymap(color_diff, subset=["Diferença"])

            # Quantos têm erro
            qtd_erro_retro = (df_retroativo_exibe["Erro Retroativo"] == "ERRO_DESCONTAR_RETROATIVO").sum()
            if qtd_erro_retro > 0:
                st.error(f"{qtd_erro_retro} registro(s) com erro de Descontar Retroativo.")
            else:
                st.info("Nenhum erro de Descontar Retroativo detectado na soma absoluta.")

            st.dataframe(df_retroativo_styled)

    # ---------------------------------------------------------------------
    # ABA 5: DUPLICATAS
    # ---------------------------------------------------------------------
    with tab5:
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

            # Se quiser formato float:
            df_dups["Quantidade"] = df_dups["Quantidade"].astype(float)

            df_dups_styled = df_dups.style.format({
                "Quantidade": "{:.2f}"
            })
            st.dataframe(df_dups_styled)

    # ---------------------------------------------------------------------
    # ABA 6: GRÁFICOS
    # ---------------------------------------------------------------------
    with tab6:
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
