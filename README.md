# Projeto Agro

## Objetivos
- Avaliar relação quantidade produzida de determinada commoditie por país.
- Avaliar o percentual colhido (Área plantada/Quantidade Colhida) de determinada commoditie por safra
- Avaliar percentual exportado (Quantidade exportado/ Quantidade colhida) em determinado país por anos
- Visualizar relação das commodities com o PIB de determinado país conforme o passar dos anos

## Datasets brutos
### Fonte: FAOSTAT

####  Crops and livestock products
- Domain Code : Código do dataset
- Domain : Nome dataset
- Area Code (M49): Código do país pelo padrão M49
- Area: País
- Element Code: Código do elemento
- Element: Elemento
- Item Code (CPC): Código do item
- Item: Nome do item 
- Year Code: Código do ano
- Year: Ano
- Unit: Unidade de medida
- Value: Valor
- Flag: Flag(A,E,I,M,T)
- Flag Description: Descrição do que é a flag

### FONTE: ONU
-  Region/Country/Area: Código da região ou país
- Country: Nome do país ou região
- Year: Ano 
- Series: Informação do valor
- Value: Valor
- Footnotes: Observações
- Source: Fonte do dado

## Modelagem banco de dados (Data Warehouse)
Para realizar a modelagem de dados foi utilizado o aplicativo DbSchema. O banco modelado encontra-se abaixo:

![Banco de dados modelado](modelagem_dados.png)



## Arquitetura do projeto
A fim de viabilizar o desenvolvimento do projeto, fez-se necessário instanciar os recursos que foram utilizados do GCP. Para isso,
utilizou-se a ferramenta *Terraform* que provisiona os recursos via código facilitando na manutenção e implementação 
da infraestrutura.

A partir deste momento, inicia-se a criação do ETL. Dessa forma, o dado foi extraído das APIs disponibilizadas
pelas fontes citadas, e em seguida o mesmo foi tratado e a inserção é realizada em formato *.parquet* no *Cloud Storage* e dentro das tabelas do *Data Warehouse*.

Para análise desses dados foi utilizado a ferramenta *Metabase*.

Todo esse fluxo de desenvolvimento está representado pelo diagrama abaixo.

![Arquitetura do projeto](diagram_projeto_agro.png)
