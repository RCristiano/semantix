* Qual o objetivo do comando cache em Spark?
 - Criar um cache nativo capaz de armazenar dataframes para otimização do processamento.

* O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
 - MapReduce Trabalha escrevendo em disco, em todos os maps e todos os reduces, enquanto Spark trabalha in memory 

* Qual é a função do SparkContext ?
 - SparkContext representa a conexão com o cluster, criando um contexto de execução, a partir do Spark 2.0 SparkSession se tornou o principal entrypoint para o Spark Core. 

* Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
 - Trata-se, como o nome indica, de um conjunto de Dados Distribuidos e Resilientes. RDD é tolerante a falhas e torna transparente para o Spark a manipulação de dados distribuidos em varios nós além de permitir a computação paralela dos dados.

* GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
 - Isso acontece porque GroupByKey reune todos os dados antes de agregar os resultados, enquanto reducebyKey faz as agregações nas partições e depois agrega apenas os resultados, reduzindo a transferencia de dados pela rede.

* Explique o que o código Scala abaixo faz.
    ```
    val textFile = sc.textFile("hdfs://...")
    val counts = textFile.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://...")
    ```
    - O código lê um arquvio no hdfs, faz uma contagem de palavras e salva em um arquivo no hdfs

### Bibliografia
> https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html