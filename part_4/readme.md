# Resposta do Exec_2
- Considere que há um ambiente Hadoop disponivel para a análise desse código.

# Relação Hadoop - Spark

- O Spark tem um relacionamento nativo com o hadoop, pois permite que sejam feitas ooperações de escrita e leitura utilizando os path do proprio hdfs, como por exemplo `hdfs:///path/to/output`. Outro recurso que o Spark e o Hadoop HDFS compartilham é o YARN que é um gerenciador de recursos para computação distribuida. é possivel configurar o Spark para utilizar o YARN para gerenciar seus recurso em vez do `Standalone Mode`.