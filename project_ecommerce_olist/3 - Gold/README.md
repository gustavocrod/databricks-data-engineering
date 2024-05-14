## 🥇 Gold

**camada para aplicação de regras de negócio**
---

### [1 - gold_orders](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/3%20-%20Gold/gold_orders.ipynb)
Conforme o schema disponibilizado, iremos agregar os dados em uma big table que permitirá ~quase~ todas as analises subsequentes

Apenas para fins de teste, iremos agregar apenas reviews e payments à table "fact" orders;
Portanto, iremos carregar essas tabelas

### [2 - gold_customer_orders](https://dbc-95ac872f-197a.cloud.databricks.com/?o=3400972147665339#notebook/4240245637785921/command/4240245637786376)
Essa é uma tabela de sumarização.

O objetivo dela é responder sobre as compras dos clientes.

Conseguiríamos responder questões como:
 - Quantas vendas ocorreram por estado
 - poderiamos ver as vendas por mes e ano
 - poderiamos ver dados sobre valores das vendas
 - dados sobre as entregas, como a relação do dia da compra e atraso na entrega

### [3 - gold_multiple_order](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/3%20-%20Gold/gold_multiple_orders.ipynb)
Essa é uma tabela sumarizada analítica.

O objetivo dela é informar os meses em que tiveram mais pedidos;
 - multiple_orders: informar se o cliente comprou mais de uma vez

### [4 - gold_total_orders_by_month_year](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/3%20-%20Gold/gold_total_orders_month_year.ipynb)
Essa é uma tabela sumarizada analítica.
Nessa tabela, agrupamos por mes/ano e contamos o total de orders

### [5 - gold_total_orders_profit_by_seller_city](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/3%20-%20Gold/gold_total_orders_profit_by_seller_city.ipynb)
Essa é uma tabela sumarizada analítica.
O objetivo dela é informar o total de venda bruta por cada vendedor (aqui temos cidade vendedora)