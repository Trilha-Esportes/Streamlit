# Documentação do Projeto (Visão Geral)

Este documento descreve **passo a passo** o funcionamento do aplicativo Streamlit que carrega e analisa dados de vendas, comissões e eventos de repasse.

## 1. Configuração de Conexão ao Banco

No início do código, definimos variáveis de ambiente para configurar a conexão com o banco PostgreSQL:
- `DB_USER`
- `DB_PASS`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`

Caso não sejam encontradas no ambiente, assumimos valores padrão. Em seguida, é criada uma `engine` do SQLAlchemy para se comunicar com o banco de dados.

## 2. Funções Auxiliares

### `carregar_dados_geral()`
- Executa uma query SQL que faz LEFT JOIN em várias tabelas: 
  - `sku_marketplace` (informações sobre os SKUs e pedidos)
  - `marketplaces` (nome do marketplace)
  - `vendas` (informações sobre o valor do pedido)
  - `comissoes_pedido` (porcentagem e datas de comissão)
  - `evento_centauro` (repasse, tipo de evento, data de repasse)
- Cria um DataFrame resultante unificando todos esses dados.
- Preenche valores nulos e normaliza o tipo de evento (ex.: "repasse normal", "repassse normal") em "Repasse Normal" etc.

### `normalizar_tipo_evento(evento)`
- Recebe uma string que descreve o tipo de evento e retorna um valor padronizado (exemplo: "Repasse Normal", "Descontar Retroativo", etc.).
- Serve para unificar variações ortográficas comuns.

### `checar_erro_comissao(row)`
- Verifica se, nos casos de "Repasse Normal", o valor repassado (`valor_final`) está de acordo com a conta: `valor_liquido - (valor_liquido * porcentagem)`.
- Usa tolerância de R\$0,05 para não marcar diferenças mínimas como erro.
- Se a diferença for maior que 5 centavos, retorna "ERRO".

### `checar_erros_adicionais(row)`
- Lista diversos erros adicionais:
  - Valor Final Negativo
  - Falta de Comissão
  - Falta de Data de Comissão
  - Erro Cálculo Comissão (quando `checar_erro_comissao` já marcou "ERRO")
  - Erro Devolução (quando a verificação de “Descontar Hove/Houve” falha)

### `filtrar_por_erros(df, erros_selecionados)`
- Recebe o DataFrame e uma lista de erros marcados (ex.: "Falta de Comissão", "Erro Cálculo Comissão").
- Retorna apenas as linhas em que `lista_erros` contém ao menos um dos itens selecionados.

### `verificar_descontar_hove(df)`
- Verifica se, em um mesmo pedido, existe um "Repasse Normal" e um "Descontar Hove/Houve".
- Checa se o valor do "Descontar Hove/Houve" bate exatamente com o valor do pedido repassado, para fins de devolução.
- Se não bater, marca `erro_descontar` = "ERRO_DEVOLUCAO".

### `verificar_descontar_retroativo(df)`
- Similar ao anterior, mas para o caso de “Descontar Retroativo”.
- Soma todos os valores dos eventos "Descontar Retroativo" e checa se é igual ao valor do pedido. Se for, marca "ERRO_DESCONTAR_RETROATIVO".

### `carregar_vendas()`
- Realiza uma query simples para ler a tabela `vendas` e retorna num DataFrame com colunas `venda_id`, `sku_marketplace_id` e `valor_vendas`.

### `montar_resumo_financeiro(df_geral, df_vendas)`
- Faz um merge (`df_geral` + `df_vendas`) para obter `valor_vendas`.
- Para cada pedido, calcula:
  - Valor total do pedido (maior valor encontrado de `valor_vendas`).
  - Comissão esperada = maior `comissao_calc`.
  - Valor a receber = valor_total - comissão.
  - Valor recebido = max(`valor_final`) onde `tipo_evento_normalizado` = "Repasse Normal".
  - Valor descontado (soma de eventos "Descontar Hove/Houve" e "Descontar Retroativo").
  - Desconto frete (eventos de "Descontar Reversa Centauro Envios").
  - Situação do pagamento:
    - "pago" se a diferença for < 0.05
    - "pago a maior"
    - "pago a menor"
    - "nao pago"
  - Situação final (pode ser "Erro Devolução", se for detectado erro, ou a própria situação do pagamento)
- Retorna um DataFrame para exibição.

## 3. Interface Streamlit (Função `main()`)

1. **Título**  
   Apresenta o título "Painel de Análises e Filtros (Com Data/Ciclo)".

2. **Sidebar**  
   - Campos de filtro:
     - Número do Pedido (`pedido_filtro`)
     - Tipo(s) de Evento (`evento_filtro`)
     - Data inicial/final de Comissão
     - Erros a exibir
   - Esses filtros impactam o DataFrame antes da exibição.

3. **Carregamento de Dados**  
   - Chama `carregar_dados_geral()` para obter o DataFrame principal (`df`).
   - Cria `df["erro_comissao"]` a partir de `checar_erro_comissao()`.
   - Executa `verificar_descontar_hove(df)` e mescla no DataFrame para identificar divergências na devolução.
   - Cria `df["lista_erros"]` com `checar_erros_adicionais()`.

4. **Filtros**  
   - Aplica cada filtro (pedido, tipo de evento, data, erros) em `df_filtrado`.

5. **Abas**  
   - **Aba 1 (Visão Geral)**: exibe uma tabela com colunas selecionadas e algumas métricas. Inclui também a “Visão Geral Anymarket”, comparando `valor_liquido` e `valor_vendas`.
   - **Aba 2 (Resumo Financeiro)**: constrói `df_financeiro` usando `montar_resumo_financeiro()` e exibe a tabela resultante, com métricas agregadas.
   - **Aba 3 (Erros de Descontar Hove/Houve)**: exibe apenas os pedidos marcados com “ERRO_DEVOLUCAO”.
   - **Aba 4 (Gráficos)**: mostra gráficos de barras e pizza sobre tipos de evento e erros encontrados.

6. **Execução**  
   - Se o arquivo for executado diretamente (`__main__`), chama `main()`.

## 4. Pontos de Atenção

- Ao configurar em produção, lembre-se de ajustar as variáveis de ambiente de banco de dados.
- Se a performance cair, pode ser necessário otimizar as queries ou remover alguns LEFT JOINs.
- Os filtros de data baseiam-se em `data_comissao`. Caso seja necessário filtrar por outra data (ex.: `data_evento`), é preciso ajustar o código.

## 5. Possíveis Melhorias Futuras

- Implementar paginação para grandes volumes de dados.
- Implementar caching mais avançado (diferenciar datas, pedidos e etc.).
- Adicionar testes automáticos (pytest) para validar se os cálculos de comissão e repasses estão corretos.
- Exibir mais detalhes sobre divergências de valores (por exemplo, logs de por que a divergência ocorreu).

---

Qualquer dúvida, entre em contato com a equipe de desenvolvimento ou consulte a documentação interna dos endpoints e tabelas.

