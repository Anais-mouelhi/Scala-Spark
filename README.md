# TP5 — Pipeline Big Data End-to-End

## Architecture Lambda implementee

```
[ Producteur Python ]
         |
         v
[ Kafka : topic 'ecommerce-events' (3 partitions) ]
         |              |
         v              v
[ Spark Structured    [ HDFS : archivage
  Streaming ]           evenements bruts ]
  (foreachBatch)
         |
         v
[ HBase : table 'sales' ]
  stats:total_amount | stats:purchase_count | meta:last_update
```

## Contenu de la branche

| Fichier | Description |
|---------|-------------|
| `ecommerce-pipeline/producer.py` | Producteur Kafka Python |
| `ecommerce-pipeline/pom.xml` | Configuration Maven |
| `ecommerce-pipeline/src/.../EcommercePipeline.java` | Job Spark Structured Streaming |

## Resultats obtenus

### HBase — table `sales`

| Categorie | CA cumule (EUR) | Nb achats |
|-----------|----------------|-----------|
| Electronique | 2012.51 | 8 |
| Livres | 2955.90 | 9 |
| Maison | 1466.53 | 8 |
| Sport | 1514.27 | 7 |
| Vetements | 1503.43 | 8 |

### HDFS — evenements bruts

```
/data/ecommerce/raw/part-00000-*.json   ~31 KB
/data/ecommerce/raw/part-00001-*.json   ~34 KB
/data/ecommerce/raw/part-00002-*.json   ~29 KB
```

## Demarrage rapide

```bash
# 1. Demarrer l'environnement
docker start hadoop-master hadoop-worker1 hadoop-worker2
docker exec -it hadoop-master bash
./start-hadoop.sh && ./start-kafka-zookeeper.sh && start-hbase.sh

# 2. Creer le topic Kafka
kafka-topics.sh --create --topic ecommerce-events \
  --replication-factor 1 --partitions 3 \
  --bootstrap-server localhost:9092

# 3. Creer la table HBase
echo "create 'sales','stats','meta'" | hbase shell -n

# 4. Creer le repertoire HDFS
hdfs dfs -mkdir -p /data/ecommerce/raw

# 5. Lancer le producteur
pip install kafka-python
python ecommerce-pipeline/producer.py &

# 6. Compiler et lancer Spark
cd ecommerce-pipeline
mvn clean package -DskipTests
spark-submit --class tn.insat.tp5.EcommercePipeline \
  --master local[4] --driver-memory 512m \
  --conf 'spark.sql.shuffle.partitions=4' \
  target/ecommerce-pipeline-1-jar-with-dependencies.jar
```

## Technologies

- Apache Kafka 3.6.1 (2.13)
- Apache Spark 3.5.0 (Structured Streaming, foreachBatch)
- Apache HBase 2.5.8
- Apache Hadoop 3.3.6 (HDFS)
- Java 1.8 / Maven 3.x
- Python 3.x / kafka-python
