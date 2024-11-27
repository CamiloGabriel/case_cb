# Projeto para case de engenheiro de dados: pipeline de dados de varejo

![image](docs/images/fluxograma.png)


*Usando Airflow, BigQuery, Google Cloud Storage, dbt, Soda, Metabase e Python*

## Objetivo
O objetivo deste projeto é criar um pipeline de dados de ponta a ponta a partir de um conjunto de dados extraídos de endpoints e do arquivo ERP.json fornecido. Isso envolve modelar os dados em tabelas de dimensão de fatos, implementar etapas de qualidade de dados, utilizar tecnologias modernas de pilha de dados (dbt, Soda e Airflow) e armazenar os dados na nuvem (Google Cloud Platform). Por fim, a camada de relatórios é consumida no Metabase. O projeto é conteinerizado via Docker e versionado no GitHub.
- Tech stack de tecnologias usadas
- Python
- Docker e Docker-compose
- Soda.io
- Metabase
- Google Cloud Storage
- Google BigQuery
- Airflow (versão Astronomer)
- dbt
- GitHub (Este Repositório)

[Aqui estão todos os detalhes sobre o objetivo](https://drive.google.com/file/d/1ynEBD7ZldXa2o4yfM16eO4QI1eC8vi4g/view?usp=sharing)
---------

## Contexto:
Como já descrito no link acima, foi dado um arquivo ERP.json contendo a resposta do endpoint /res/getGuestChecks, onde, seu conteúdo corresponde a um determinado poedido com um único item referente a um único item de menu.



# O equema referente ao JSON é este abaixo:

## Nível Raiz
| Campo | Tipo | Formato | Descrição |
|--- |--- |--- |--- |
| curUTC | string | date-time | Horário atual em UTC. |
| locRef | string |	Referência de localização. |
| guestChecks | array |	 | Lista de checks de hóspedes. |



## Campos dentro de guestChecks
| Campo	| Tipo | Formato | Descrição |
|--- |--- |--- |--- |
| guestCheckId | integer |  | Identificador único do check de hóspede. |
| chkNum | integer |  | Número do check. |
| opnBusDt | string | date | Data de abertura do check (negócios). |
| opnUTC | string | date-time | Horário de abertura em UTC. |
| opnLcl | string | date-time | Horário de abertura em horário local. |
| clsdBusDt	| string | date | Data de fechamento do check (negócios). |
| clsdUTC | string | date-time | Horário de fechamento em UTC. |
| clsdLcl | string | date-time | Horário de fechamento em horário local. |
| lastTransUTC | string | date-time | Horário da última transação em UTC. |
| lastTransLcl | string | date-time | Horário da última transação em horário local. |
| lastUpdatedUTC | string | date-time | Última atualização em UTC. |
| lastUpdatedLcl | string | date-time | Última atualização em horário local. |
| clsdFlag | boolean |  | Indica se o check foi fechado. |
| gstCnt | integer |  | Número de hóspedes. |
| subTtl | number |  | Subtotal. |
| nonTxblSlsTtl | number - null |  | Total de vendas não tributáveis (pode ser nulo). |
| chkTtl | number |  | Total do check. |
| dscTtl | number |  | Total de descontos aplicados. |
| payTtl | number |	 | Total pago. |
| balDueTtl | number - null |  | Total devido (pode ser nulo). |
| rvcNum | integer |  | Número do ponto de venda (revenue center). |
| otNum | integer |  | Número do tipo de operação. |
| ocNum | integer - null |  | Número de origem do check (pode ser nulo). |
| tblNum | integer |  |	Número da mesa. |
| tblName |	string |  |	Nome da mesa. |
| empNum | integer |  |	Número do funcionário responsável. |
| numSrvcRd | integer |  | Número de rodadas de serviço. |
| numChkPrntd |	integer |  | Número de impressões do check. |
| taxes | array |  | Lista de impostos aplicados. |
| detailLines |	array |  | Detalhes das linhas do check. |



## Campos dentro de taxes (em guestChecks)
| Campo | Tipo | Formato | Descrição |
|--- |--- |--- |--- |
|taxNum | integer |	 | Número do imposto. |
| txblSlsTtl | number |  | Total de vendas tributáveis. |
| taxCollTtl | number |	 | Total de imposto coletado. |
| taxRate |	number |  |	Taxa de imposto aplicada. |
| type | integer |  | Tipo de imposto. |



## Campos dentro de detailLines (em guestChecks)
| Campo | Tipo | Formato | Descrição |
|--- |--- |--- |--- |
| guestCheckLineItemId | integer |  | Identificador único do item de linha. |
| rvcNum | integer |  | Número do ponto de venda (revenue center). |
| dtlOtNum | integer |  | Número do tipo de operação da linha. |
| dtlOcNum | integer - null |  | Número de origem do detalhe da linha (pode ser nulo). |
| lineNum | integer |  | Número da linha. |
| dtlId | integer |  | Identificador do detalhe. |
| detailUTC | string | date-time | Horário do detalhe em UTC. |
| detailLcl | string | date-time | Horário do detalhe em horário local. |
| lastUpdateUTC | string | date-time | Última atualização do detalhe em UTC. |
| lastUpdateLcl | string | date-time | Última atualização do detalhe em horário local. |
| busDt | string | date | Data de negócios do detalhe. |
| wsNum | integer |  | Número da estação de trabalho (workstation). |
| dspTtl | number |	 | Total exibido. |
| dspQty | number |  | Quantidade exibida. |
| aggTtl | number |  | Total agregado. |
| aggQty | number |  | Quantidade agregada. |
| chkEmpId | integer |  | ID do funcionário responsável pelo check. |
| chkEmpNum | integer |  | Número do funcionário responsável pelo check. |
| svcRndNum | integer |  | Número da rodada de serviço. |
| seatNum | integer |  | Número do assento. |
| type | string | enum | Tipo de detalhe na linha (menuItem, discount, serviceCharge, tenderMedia, errorCode). |



## Possíveis objetos em detailLines 
1. menuItem
| Propriedade |	Tipo | Descrição |
|--- |--- |--- |
| miNum | integer |	Número do item do menu. |
| modFlag | boolean | Indica se o item foi modificado. |
| inclTax | number | Imposto incluído no item. |
| activeTaxes | string | Impostos ativos para o item. |
| prcLvl | integer | Nível de preço do item. |


2. discount
| Propriedade | Tipo | Descrição |
|--- |--- |--- |
| discountId | integer | Identificador único do desconto. |
| amount | number | Valor do desconto. |
| reason | string |	Motivo do desconto. |


3. serviceCharge
| Propriedade |	Tipo | Descrição |
|--- |--- |--- |
| chargeId | integer | Identificador único da taxa de serviço. |
| amount | number |	Valor da taxa de serviço. |


4. tenderMedia
| Propriedade |	Tipo | Descrição |
|--- |--- |--- |
| mediaId |	integer | Identificador único do meio de pagamento. |
| type | string | Tipo do meio de pagamento (ex.: dinheiro, cartão). |
| amount | number | Valor pago. |


5. errorCode
| Propriedade | Tipo | Descrição |
|--- |--- |--- |
| code | integer | Código do erro. |
| message |	string | Mensagem descritiva do erro. |


# Pode-se notar que nele, se encontra as instâncias que também podem conter, isso por conta que foi incluída uma função dentro do arquivo "extract_data_erp.py", onde, caso não forem encontrada essas colunas, ela fará a inclusão e as tratará como "Not Provided". 




# Abordagem Escolhida: Modelo Dimensional com Tabela Fato e Tabelas Dimensão
## Descrição da Abordagem
A abordagem escolhida é o uso de um modelo dimensional, mais conhecido como Star Schema, composto por:

### Tabela Fato (fct_guest_checks):

Contém os eventos centrais do sistema de restaurante, como os check-ins (transações) realizados.
Consolida métricas relevantes para análises, como total de vendas, quantidade de itens vendidos,impostos, descontos e detalhes sobre o atendimento. Conecta-se às tabelas dimensão através de chaves estrangeiras.


### Tabelas Dimensão:

Dimensões temporais (dim_datetime): Organizam dados por ano, mês, dia, e permitem análises agregadas por períodos específicos. Dimensões geográficas e organizacionais (dim_restaurant, dim_table): Fornecem informações sobre restaurantes, regiões, e mesas usadas. Dimensões operacionais (dim_product, dim_employee, dim_tax): Detalham produtos vendidos, funcionários envolvidos e impostos aplicados, tendo assim, alinhamento com as operações do restaurante, onde existe grandes volumes de dados em diferentes áreas.


# Explicação das tecnologias usadas:

## Airflow Astronomer - Orquestração de Pipelines
Utilizado para gerenciar e automatizar o fluxo de trabalho de dados.

# DBT (Data Build Tool) - Transformação de Dados
Permite transformar dados em um formato estruturado e útil.

# Soda - Monitoramento e Validação de Dados
Monitora a qualidade dos dados em tempo real, garante confiabilidade para a tomada de decisão.

# Metabase - Business Intelligence
Fornece uma interface intuitiva para análise e visualização dos dados.



# Por que armazenar as respostas das APIs?
Armazenar respostas de APIs garante disponibilidade, resiliência e melhora o desempenho ao reduzir dependências de chamadas externas e evitar latências. Isso permite criar um histórico valioso para análises estratégicas, como tendências de consumo e otimização operacional. Também facilita auditorias, conformidade legal e integração com pipelines de dados, como dbt e Astronomer. Além disso, ajuda a reduzir custos operacionais, especialmente em APIs pagas, e oferece controle sobre mudanças futuras nas APIs.


# Como você armazenaria os dados? Crie uma estrutura de pastas capaz de armazenar as respostas da API. Ela deve permitir manipulaçõe, verificações, buscas e pesquisas rápidas.
## estrutura pastas gcp
/data-lake/
    ├── raw/                  # Dados brutos (sem transformações)
    │   ├── silver/
    |   |   ├── endpoint_name
    │   │   |   ├── 2024/
    │   │   │   |   ├── 11/
    │   │   │   │   |   ├── store_001/  # Identificador da loja
    │   │   │   │   │   └── erp_2024-11-23.json
    │   │   │   │   ├── store_002/
    │   │   │   │   │   └── erp_2024-11-23.json
    │   │   │   ├── ...
    │   ├── bronze/
    │   │   ├── fiscal_invoices/
    │   │   │   ├── 2024/
    │   │   │   │   ├── 11/
    │   │   │   │   │   └── fiscal_invoices_store_001_2024-11-23.json
    │   │   │   │   ├── ...
    │   │   ├── guest_checks/
    │   │   │   ├── 2024/
    │   │   │   │   ├── 11/
    │   │   │   │   │   └── guest_checks_store_001_2024-11-23.json
    │   │   │   │   ├── ...
    │   │   ├── ...
    ├── gold/            # Dados tratados e prontos para modelagem
    │   ├── erp/
    │   │   ├── invoices/     # Dados organizados em tabelas processadas
    │   │   │   ├── 2024/
    │   │   │   │   ├── 11/
    │   │   │   │   │   └── invoices_store_001.parquet
    │   │   │   │   ├── ...
    │   ├── bi/
    │   │   ├── revenue/
    │   │   │   ├── 2024/
    │   │   │   │   ├── 11/
    │   │   │   │   │   └── revenue_store_001.parquet
    │   │   │   │   ├── ...
    │   │   ├── ...
    ├── analytics/            # Dados transformados para análise (modelos finais)
    │   ├── fct_guest_checks
    │   ├── report_employee_guest_checks
    │   ├── report_product_guest_checks
    │   ├── report_year_guest_checks


Essa estrutura hierárquica organiza dados brutos de forma temporal e lógica, facilitando manipulações, verificações e buscas rápidas. O particionamento temporal e o isolamento de fontes tornam o gerenciamento eficiente e escalável, essencial para sistemas que lidam com grandes volumes de dados, como uma rede de restaurantes.


# Considere que a resposta do endpoint getGuestChecks foi alterada, por exemplo, guestChecks.taxes foi renomeado para guestChecks.taxation. O que isso implicaria?
Em um pipeline comum, pode impactar diretamente a ingestão, o pipeline de dados e os modelos analíticos. Porém, no meu fiz uma verificação antes mesmo de subir o arquivo bruto pra GCP
---------
## Para executar este projeto, você deve

### Instalar o Docker
[Instalar o Docker para seu SO](https://docs.docker.com/desktop/)

### Instalar o Astro CLI
[Instalar o Astro CLI para seu SO](https://www.astronomer.io/docs/astro/cli/install-cli)

### Clone o repositório GitHub

No seu terminal:

Clone o repositório usando o Github CLI ou o Git CLI
```bash
gh gh repo clone CamiloGabriel/case_cb
```
ou
```bash
git clone https://github.com/CamiloGabriel/case_cb.git
```

Abra a pasta com seu editor de código.

### Reinicialize o projeto Airflow
Abra o terminal do editor de código:
```bash
astro dev init
```
Ele perguntará: ```Você não está em um diretório vazio. Tem certeza de que deseja inicializar um projeto? (s/n)```
Digite ```y``` e o projeto será reinicializado.

### Crie o projeto
No terminal do editor de código, digite:

```bash
astro dev start
```
O ponto de extremidade padrão do Airflow é http://localhost:8080/
- Nome de usuário padrão: admin
- Senha padrão: admin

### Crie o projeto GCP

No seu navegador, vá para https://console.cloud.google.com/ e crie um projeto, recomendado algo como: ```airflow-dbt-soda-pipeline```

Copie o ID do seu projeto e salve-o para mais tarde.

#### Usando o ID do projeto do GCP

Altere os seguintes arquivos:
- .env (GCP_PROJECT_ID)
- include\dbt\models\sources\sources.yml (banco de dados)
- include\dbt\profiles.yml (projeto)

#### Crie um Bucket no GCP

Com o projeto selecionado, acesse https://console.cloud.google.com/storage/browser e crie um Bucket.
Use o nome ```<yourname>_online_retail```.
E altere o valor da variável ```bucket_name``` para o nome do seu bucket no arquivo ```dags\retail.py```.

#### Crie uma conta de serviço para o projeto

Acesse a guia IAM e crie a conta de serviço com o nome ```airflow-retail```.
Dê acesso de administrador ao GCS e ao BigQuery e exporte as chaves json. Renomeie o arquivo para service_account.json e coloque dentro da pasta ```include/gcp/``` (você terá que criar esta pasta).

#### Crie uma conexão no seu fluxo de ar

No seu fluxo de ar, em http://localhost:8080/, faça login e vá para Admin → Connections.
Crie uma nova conexão e use estas configurações:
- id: gcp
- type: Google Cloud
- Keypath Path: `/usr/local/airflow/include/gcp/service_account.json`

Salve.

### Crie sua conta SODA e chaves de API

Vá para https://www.soda.io/ e clique em "start a trial" e crie uma conta. Em seguida, faça login e vá para seu perfil, chaves de API e crie uma nova chave de API.

Copie o código soda_cloud, ele ficará assim:
```
soda_cloud:
host: cloud.us.soda.io
api_key_id: <KEY>
api_key_secret: <SECRET>
```
E cole em ```include\soda\configuration.yml``` ou edite o arquivo .env com os respectivos valores.
Note que, neste exemplo, a conta criada estava na região dos EUA, se sua conta estiver na região da UE, você terá que alterar a variável "host".

### Tudo pronto, inicie o DAG

Com seu Airflow em execução, vá para http://localhost:8080/ e clique em DAGs, e clique no DAG de varejo.
Então, inicie o DAG (botão de reprodução no canto superior direito).

Ele irá passo a passo, e se tudo foi seguido, você obterá uma execução verde no final.
Verifique na sua conta do GCP Storage se o arquivo foi carregado com sucesso, na sua aba do BigQuery se as tabelas foram criadas e no seu painel do Soda se está tudo bem.

Então, vá para o Metabase e crie seu próprio painel. O serviço do Metabase está no http://localhost:3000/

### Metabase

Vá para o http://localhost:3000/ e crie sua conta local.
Quando a opção `Adicionar seus dados` aparecer, escolha BigQuery e insira seus detalhes.
Ele pedirá um nome de exibição, eu recomendo `BigQuery_DW` ou algo assim.
O `ID do projeto` é o nome do seu projeto do GCP (o meu era `airflow-dbt-soda-pipeline`). E finalmente o `service_account.json` que você salvou na pasta `include/gcp/`.

Conecte o banco de dados e está tudo pronto.

### Contato
Se você tiver dúvidas, sinta-se à vontade para me perguntar.
- [LinkedIn](https://www.linkedin.com/in/camilogabriel/)
- [GitHub](https://github.com/CamiloGabriel/)
- [alan.lanceloth@gmail.com](mailto:camilo.resende99@gmail.com)

### Referências
Projeto baseado em vídeos de [Marc Lamberti](https://www.youtube.com/@MarcLamberti).