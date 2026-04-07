# TP5 — Pipeline Big Data End-to-End

## Architecture Lambda

```
[ Producteur Python ]
         |
         v
[ Kafka : topic 'ecommerce-events' (3 partitions) ]
         |            |
         v            v
[ Spark Structured   [ HDFS : archivage
  Streaming ]          evenements bruts ]
         |
         v
[ HBase : table 'sales' ]
  (agregats par categorie)
```

| Composant | Rôle |
|-----------|------|
| Kafka `ecommerce-events` | Transport des événements en temps réel |
| `producer.py` | Simule des clics / ajouts panier / achats |
| Spark Structured Streaming | Filtre les achats, agrège par catégorie / fenêtre 1 min |
| HBase table `sales` | Stocke le CA cumulé et le nombre d'achats par catégorie |
| HDFS `/data/ecommerce/raw` | Archive tous les événements bruts (JSON) |

## Démarrage

### 1. Démarrer le cluster

```bash
docker start hadoop-master hadoop-worker1 hadoop-worker2
docker exec -it hadoop-master bash
./start-hadoop.sh
./start-kafka-zookeeper.sh
start-hbase.sh
```

### 2. Créer le topic Kafka

```bash
kafka-topics.sh --create \
  --topic ecommerce-events \
  --replication-factor 1 \
  --partitions 3 \
  --bootstrap-server localhost:9092
```

### 3. Créer la table HBase

```bash
echo "create 'sales','stats','meta'" | hbase shell -n
```

### 4. Créer le répertoire HDFS

```bash
hdfs dfs -mkdir -p /data/ecommerce/raw
```

### 5. Lancer le producteur Python

```bash
pip install kafka-python
python producer.py &
```

### 6. Compiler et lancer le job Spark

```bash
cd ecommerce-pipeline
mvn clean package -DskipTests

spark-submit \
  --class tn.insat.tp5.EcommercePipeline \
  --master local[4] \
  --driver-memory 512m \
  --conf 'spark.sql.shuffle.partitions=4' \
  target/ecommerce-pipeline-1-jar-with-dependencies.jar \
  >> pipeline.log 2>&1 &
```

## Vérification

### HBase — agrégats par catégorie

```bash
echo "scan 'sales'" | hbase shell -n
```

Résultat attendu :
```
ROW          COLUMN+CELL
Electronique column=meta:last_update, value=1710000000000
Electronique column=stats:purchase_count, value=12
Electronique column=stats:total_amount, value=2847.50
Livres       column=stats:purchase_count, value=8
...
```

### HDFS — événements bruts archivés

```bash
hdfs dfs -ls /data/ecommerce/raw
hdfs dfs -cat /data/ecommerce/raw/part-*.json | head -5
```

## Schema HBase — table `sales`

| RowKey | Famille | Qualifieur | Description |
|--------|---------|-----------|-------------|
| `Electronique` | `stats` | `total_amount` | CA cumulé (€) |
| `Electronique` | `stats` | `purchase_count` | Nb d'achats |
| `Electronique` | `meta` | `last_update` | Timestamp dernière MAJ |

## Technologies

- Apache Kafka 3.6.1 (2.13)
- Apache Spark 3.5.0 (Structured Streaming)
- Apache HBase 2.5.8
- Apache Hadoop 3.3.6 (HDFS)
- Java 1.8
- Python 3.x / kafka-python
- Maven 3.x
