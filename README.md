# TP4 - Stockage de Données dans une Base NoSQL avec HBase

## Objectifs

Manipulation de données avec **Apache HBase** et traitement co-localisé avec **Apache Spark**.

## Stack technique

| Outil           | Version |
|-----------------|---------|
| Apache HBase    | 2.5.8   |
| Apache Hadoop   | 3.3.6   |
| Apache Spark    | 3.5.0   |
| Java            | 1.8     |
| Python          | 3.x     |
| Docker          | latest  |

## Structure du projet

```
TP-HBase/
├── hello-hbase/              # Partie 2 : API Java HBase
│   ├── pom.xml
│   └── src/main/java/tn/insat/tp4/HelloHBase.java
│
├── hbase-spark/              # Partie 3 : Spark + HBase
│   ├── pom.xml
│   └── src/main/java/tn/insat/tp4/HbaseSparkProcess.java
│
├── sensor-pipeline/          # Projet final : pipeline complet
│   ├── pom.xml
│   └── src/main/java/tn/insat/tp4/SensorPipeline.java
│
└── visualize.py              # Visualisation des resultats (matplotlib)
```

## Demarrage de l'environnement

```bash
# Demarrer les conteneurs Docker
docker start hadoop-master hadoop-worker1 hadoop-worker2

# Entrer dans le master
docker exec -it hadoop-master bash

# Lancer Hadoop + HBase
./start-hadoop.sh
start-hbase.sh

# Verifier les daemons
jps
# NameNode, SecondaryNameNode, ResourceManager (Hadoop)
# HMaster, HRegionServer, HQuorumPeer (HBase)
```

## Partie 1 - HBase Shell

```bash
# Lancer le shell HBase
hbase shell

# Creer la table sales_ledger
create 'sales_ledger','customer','sales'

# Inserer des donnees
put 'sales_ledger','101','customer:name','John White'
put 'sales_ledger','101','customer:city','Los Angeles, CA'
put 'sales_ledger','101','sales:product','Chairs'
put 'sales_ledger','101','sales:amount','$400.00'
# ... (lignes 102, 103, 104)

# Afficher toutes les lignes
scan 'sales_ledger'

# Lire une cellule precise
get 'sales_ledger','102',{COLUMN => 'sales:product'}
```

## Partie 2 - API Java HBase (hello-hbase)

Cree et manipule une table `user` avec deux familles de colonnes :
- `PersonalData` : name, address
- `ProfessionalData` : company, salary

```bash
# Dans le conteneur, depuis /root/hbase-code/hello-hbase
mvn clean package
java -cp 'target/hello-hbase-1.0-SNAPSHOT.jar:lib/*' tn.insat.tp4.HelloHBase
```

**Resultat attendu :**
```
Connecting
Creating Table
Done......
Adding user: user1
Adding user: user2
Reading data...
ahmed
```

## Partie 3 - Spark + HBase (hbase-spark)

Lit la table `sales_ledger` via Spark et compte les enregistrements.

```bash
# Copier les libs HBase dans Spark
cp -r $HBASE_HOME/lib/* $SPARK_HOME/jars

# Depuis /root/hbase-code/hbase-spark
mvn clean package

spark-submit \
  --class tn.insat.tp4.HbaseSparkProcess \
  --master local[4] \
  target/hbase-spark-1.jar
```

**Resultat :** `nombre d'enregistrements: 4`

## Activite ImportTsv

Import d'un fichier TSV dans HBase via MapReduce.

```bash
# Creer le fichier TSV et l'uploader dans HDFS
hdfs dfs -put sensors.tsv /hbase-import/

# Creer la table cible
echo "create 'sensors','info','readings'" | hbase shell -n

# Lancer l'import
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.columns=HBASE_ROW_KEY,info:city,readings:temperature,readings:humidity \
  -Dmapreduce.framework.name=local \
  sensors /hbase-import/sensors.tsv
```

**Resultat :** 10 lignes importees, 0 Bad Lines.

## Projet Final - Pipeline Capteurs

### Architecture

```
sensors.tsv (HDFS)
     |
  ImportTsv
     |
  HBase (table: sensors)
     |
  Spark (SensorPipeline.java)
  - Lecture de la table sensors
  - Calcul des moyennes par ville (avg temp, avg humidity)
     |
  HBase (table: city_stats)
     |
  Python / matplotlib (visualize.py)
     |
  Graphes PNG
```

### Execution

```bash
# Dans le conteneur, depuis /root/hbase-code/sensor-pipeline
mvn clean package

spark-submit \
  --class tn.insat.tp4.SensorPipeline \
  --master local[4] \
  target/sensor-pipeline.jar
```

**Resultats dans HBase (city_stats) :**

| Ville     | Temp. moy. | Humidite moy. | Mesures |
|-----------|-----------|---------------|---------|
| Paris     | 29.30 C   | 64.35 %       | 2       |
| Lyon      | 23.50 C   | 53.55 %       | 2       |
| Marseille | 32.10 C   | 69.65 %       | 2       |
| Bordeaux  | 20.65 C   | 59.15 %       | 2       |
| Lille     | 16.10 C   | 73.95 %       | 2       |

### Visualisation

```bash
# Sur la machine hote (requiert matplotlib et numpy)
pip install matplotlib numpy

python3 visualize.py
```

Genere 4 graphes :
- `chart_temperature.png` : Temperature moyenne par ville (barres)
- `chart_humidity.png` : Humidite moyenne par ville (barres)
- `chart_combined.png` : Temperature + Humidite (double axe)
- `chart_radar.png` : Profil climatique par ville (radar)

## Lien avec le TP Kafka

Ce TP s'inscrit dans la suite du TP Kafka (`SensorProducer.java`) :
- **TP Kafka** : Streaming temps reel de donnees capteurs (ville, temperature, humidite)
- **TP HBase** : Stockage et analyse batch des resultats dans une base NoSQL

Le pipeline complet serait :
```
Capteurs (SensorProducer) -> Kafka -> Spark Streaming -> HBase -> Visualisation
```
