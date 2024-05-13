# Kaggle Supermarket sales

[Supermarket sales](https://www.kaggle.com/datasets/aungpyaeap/supermarket-sales)

O conjunto de dados é um registros histórico de vendas de uma empresa de supermercados (ficticio), que foi registrado em 3 filiais diferentes ao longo de 3 meses. 

### schema
 - ``Invoice`` ID: Número de identificação da fatura de venda gerado pelo computador
 - ``Branch``: Filial do supermercado (3 filiais disponíveis identificadas por A, B e C).
 - ``City``: Localização dos supermercados
 - ``Customer`` type: Tipo de clientes, registrados como _Member_ para clientes que usam cartão de membro e Normal para aqueles sem cartão de membro.
 - ``Gender``: Tipo de gênero do cliente
 - ``Product`` line: Grupos de categorização geral de itens - Acessórios eletrônicos, Acessórios de moda, Alimentos e bebidas, Saúde e beleza, Casa e estilo de vida, Esportes e viagens
 - ``Unit price``: Preço de cada produto em USD ($)
 - ``Quantity``: Número de produtos comprados pelo cliente
 - ``Tax 5%``: Taxa de imposto de 5% para clientes que compraram
 - ``Total``: Preço total incluindo impostos
 - ``Date``: Data da compra (Registro disponível de janeiro de 2019 a março de 2019)
 - ``Time``: timestamp bugado, Hora da compra (das 10h às 21h)
 - ``Payment``: Método de pagamento usado pelo cliente na compra (3 métodos estão disponíveis - Dinheiro, Cartão de crédito e Ewallet)
 - ``cogs``: Custo dos bens vendidos
 - ``gross margin percentage``: Porcentagem de margem bruta
 - ``gross income``: Renda bruta
 - ``Rating``: Classificação de estratificação do cliente em sua experiência de compra geral (Em uma escala de 1 a 10)