## 🥈 Silver
**camada de limpeza, normalização e enriquecimento de dados.**

-----

### Data cleaning

Aqui, a grande maioria das tabelas não foi alterada em relação a bronze.

#### 1 - silver_geolocation
Somente iremos realizar um ajuste no outlier seller_city "04482255"

``df = df.filter("seller_city != '04482255'")``

### Transformações

Nessa layer aplicamos "enriquecimento" de dados. Fizemos isso agregando e manipulando campos como "data de entrega" e "data do envio" para calculado o "tempo de entrega".

#### [1 - silver_customers](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_customers.ipynb)
#### [2 - silver_geolocation](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_geolocation.ipynb)
#### [3 - silver_order_items](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_order_items.ipynb)
#### [4 - silver_order_payments](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_order_payments.ipynb)
#### [5 - silver_order_reviews](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_order_reviews.ipynb)
#### [6 - silver_orders](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_orders.ipynb)

Inicialmente iremos carregar dados para agregações:
 - Campo mes/ano para calcular vendas mensais, trimestrais e etc

Podemos nos focar no tempo decorrido de cada etapa, por exemplo:
 - tempo até a aprovação (em minutos ou segundos)
 - tempo de entrega (em dias)
 - tempo total da compra até a entrega (em dias)
 - atraso (divergencia entre tempo estimado e o entregue)

 Além disso, podemos trazer dados que auxiliem na analise do padrão de compra por data
  - dia da semana
  - é fim de semana?

aqui poderia estressar e ir até para coisas do tipo:
pandas_market_calendars
 - é feriado?
 - qual feriado
 - dias até o próximo feriado - para entender padrões de compra próximo a feriados

#### [7 - silver_products](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_products.ipynb)
#### [8 - silver_sellers](https://github.com/gustavocrod/databricks-data-engineering/blob/main/project_ecommerce_olist/2%20-%20Silver/silver_sellers.ipynb)