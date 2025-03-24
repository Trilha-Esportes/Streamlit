# Documentação Técnica do Código

## Visão Geral
Este código é uma aplicação em Streamlit que se conecta a um banco de dados PostgreSQL para analisar e exibir dados financeiros e comerciais, identificando possíveis erros em transações, cálculos de comissão e descontos diversos. A aplicação permite a interação por meio de filtros personalizados, visualização detalhada dos dados, resumo financeiro consolidado e gráficos representativos.

## Configurações de Conexão ao Banco
O banco de dados PostgreSQL é acessado utilizando credenciais armazenadas em variáveis de ambiente, com valores padrão específicos para desenvolvimento.

## Funções Principais

### carregar_dados_geral
Carrega e consolida os dados das tabelas `sku_marketplace`, `marketplaces`, `vendas`, `comissoes_pedido` e `evento_centauro`. Realiza limpeza, normalização e preenchimento de valores nulos para garantir integridade dos dados.

### normalizar_tipo_evento
Padroniza os nomes dos tipos de eventos para evitar inconsistências causadas por pequenas diferenças na escrita.

### checar_erro_comissao
Verifica discrepâncias no cálculo das comissões com tolerância de até R$0,05 para eventos de "Repasse Normal".

### checar_erros_adicionais
Identifica erros adicionais relacionados a valores negativos, falta de comissão, ausência de data ou cálculos incorretos.

### verificar_descontar_hove
Compara valores dos eventos "Repasse Normal" com "Descontar Hove/Houve" para detectar inconsistências.

### verificar_descontar_retroativo
Agrupa e verifica erros nos eventos do tipo "Descontar Retroativo".

### carregar_vendas
Carrega de forma otimizada (cacheada) os dados de vendas do banco.

### montar_resumo_financeiro
Cria um DataFrame consolidado que resume as informações financeiras por pedido, incluindo valor total, comissão, valores recebidos e descontados, situação do pagamento e situação final.

## Interface Streamlit
A interface é organizada em quatro abas principais:

### 1. Visão Geral
Exibe informações detalhadas dos dados filtrados, incluindo métricas gerais, erros identificados e visão específica para transações relacionadas ao marketplace Anymarket.

### 2. Resumo Financeiro
Apresenta um resumo consolidado das transações financeiras, permitindo filtragem adicional por situação financeira (correto, pago, erro, etc.).

### 3. Erros de Descontar Hove/Houve
Lista especificamente os erros relacionados aos descontos de devoluções identificados pela função `verificar_descontar_hove`.

### 4. Gráficos
Disponibiliza representações visuais dos dados filtrados, como gráficos de barras e pizza, que mostram a distribuição de tipos de eventos e erros encontrados.

## Uso de Cache
A aplicação utiliza a funcionalidade de cache do Streamlit (`@st.cache_data`) para otimizar o carregamento repetido de dados do banco, melhorando significativamente o desempenho.

## Considerações Técnicas
- O código adota técnicas robustas para tratamento de valores monetários e porcentagens, considerando arredondamentos e tolerâncias.
- Uso eficiente de Pandas para manipulação e análise de dados.
- Streamlit para uma interface interativa e visualmente agradável.

Este documento facilita o entendimento e manutenção do código, auxiliando no desenvolvimento futuro e na identificação rápida de pontos importantes da aplicação.

