# Particionamento

- O particionamento é um recurso essencial para otimizar o processamento de grandes volumes de dados. Quando utilizado corretamente, ele reduz significativamente o tempo de leitura e processamento ao dividir os dados em subgrupos baseados em colunas específicas, como data, departamento, etc. Isso permite que as consultas sejam direcionadas, evitando assim fullscan da base de dados, melhorando assim significamente o tempo de consulta desses dados.

# Broadcast Join

- É um recurso utilizado para otimiza as operações de join entre duas bases um grande e outra relativamente menor, ja que ele carrega na memoria dos `wokers` a base menor, com isso diminui o tempo de leitura durante o join, pois o dado ja esta carregado na memória. É importante ficar atento, caso a base seja do tamanho da memória definida para o `woker`, ira apersentar um erro de que a toda a memória foi consumida, por isso é importante limitar o uso de memória pelo `broadcast`.