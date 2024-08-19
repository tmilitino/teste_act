# Teste de Conhecimentos em PySpark

## Pré-requisitos

- Crie um ambiente virtual
  - `python3 -m venv venv`
- Ative o ambiente virtual
  - `source venv/bin/activate`
- Instale as bibliotecas do arquivo `requirements.txt`
  - `pip install -r ./requirements.txt`

## Massa de dados

Todas as bases utilizadas estão disponiveis no diretorio `inputs_data`, caso queram gerar novos dados, basta substiiur os dados, preservando seus repectivos nomes.

- Para as Partes 1 e 2 foi usado a base sugerida nas suas expectivas questões.

- Para as Partes 3 e 4 foram gerados 2 massas de dados para resolução desse teste
  - [action_logs](https://www.mockaroo.com/e58bafd0)
  - [car_makers](https://www.mockaroo.com/ce885200)
  - [owners](https://www.mockaroo.com/246752f0)

- Para a Parte 5 foi gerado uma base seguindo as expecificações da questão
  - [action_logs](https://www.mockaroo.com/e58bafd0)

## Questionario

### Parte 1: Manipulação de Dados

#### Criação de DataFrame

- Crie um DataFrame a partir do seguinte conjunto de dados:
data = [
    ("Alice", 34, "Data Scientist"),
    ("Bob", 45, "Data Engineer"),
    ("Cathy", 29, "Data Analyst"),
    ("David", 35, "Data Scientist")
]
columns = ["Name", "Age", "Occupation"]
  - [Código](./exec_base.py)

#### Filtragem e Seleção

- Selecione apenas as colunas "Name" e "Age" do DataFrame criado.
Filtre as linhas onde a "Age" é maior que 30.
  - [Código](./part_1/exec_1.py)

#### Agrupamento e Agregação

- Agrupe os dados pelo campo "Occupation" e calcule a média de "Age" para cada grupo.
  - [Código](./part_1/exec_2.py)

#### Ordenação

Ordene o DataFrame resultante da questão anterior pela média de "Age" em ordem decrescente.

- [Código](./part_1/exec_3.py)

### Parte 2: Funções Avançadas

#### Uso de UDFs (User Defined Functions)

- Crie uma função em Python que converte idades para categorias:
Menor que 30: "Jovem"
Entre 30 e 40: "Adulto"
Maior que 40: "Senior"
Aplique essa função ao DataFrame usando uma UDF.
  - [Código](./part_2/exec_1.py)

#### Funções de Janela

- Use funções de janela para adicionar uma coluna que mostre a diferença de idade entre cada indivíduo e a média de idade do seu "Occupation".
  - [Código](./part_2/exec_2.py)

### Parte 3: Performance e Otimização

#### Particionamento

- Explique como o particionamento pode ser usado para melhorar a performance em operações de leitura e escrita de dados em PySpark. Dê um exemplo de código que particiona um DataFrame por uma coluna específica.
  - [Resposta](part_3/readme.md#particionamento)
  - [Código](./part_3/exec_1.py)

#### Broadcast Join

- Descreva o conceito de Broadcast Join em PySpark e como ele pode ser usado para otimizar operações de join. Implemente um exemplo de Broadcast Join entre dois DataFrames.
  - [Resposta](part_3/readme.md#broadcast-join)
  - [Código](./part_3/exec_2.py)

### 4: Integração com Outras Tecnologias

#### Leitura e Escrita de Dados

- Demonstre como ler dados de um arquivo CSV e escrever o resultado em um formato Parquet.
  - [Código](./part_4/exec_1.py)

#### Integração com Hadoop

- Explique como PySpark se integra com o Hadoop HDFS para leitura e escrita de dados. Dê um exemplo de código que leia um arquivo do HDFS e salve o resultado de volta no HDFS.
  - [Resposta](part_4/readme.md#relação-haddop---spark)
  - [Código](./part_4/exec_2.py)

### Parte 5: Problema de Caso

#### Processamento de Logs

- Considere que você tem um grande arquivo de log com as seguintes colunas: "timestamp", "user_id", "action". Cada linha representa uma ação realizada por um usuário em um determinado momento.
  - Carregue o arquivo de log em um DataFrame.
  - Conte o número de ações realizadas por cada usuário.
  - Encontre os 10 usuários mais ativos.
  - Salve o resultado em um arquivo CSV.
- [Código](./part_5/action_log.py)
