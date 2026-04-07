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

## Questions Tech Lead

### Q1 — En production avec 1 million d'événements/seconde, quelle modification apporteriez-vous ?

Le pipeline actuel écrit dans HBase toutes les 30 secondes avec des `Put` individuels. À 1M events/s, plusieurs adaptations sont nécessaires :

**Côté Spark :**
- Passer de `local[4]` à un cluster YARN multi-workers (`--master yarn --num-executors 10`)
- Augmenter le nombre de partitions Kafka et Spark (`spark.sql.shuffle.partitions=50+`)
- Réduire le trigger à `10 seconds` pour limiter la taille des micro-batches

**Côté HBase :**
- Remplacer les `Put` unitaires par un `BufferedMutator` (écriture asynchrone en batch) pour réduire les round-trips réseau
- Pré-splitter la table `sales` sur les row keys pour éviter les hotspots d'écriture
- Activer la compression des colonnes (`SNAPPY`)

**Côté Kafka :**
- Augmenter les partitions du topic (ex. 12 ou 24) pour paralleliser la lecture Spark
- Configurer la rétention des messages en cohérence avec la fenêtre de traitement

**Côté infrastructure :**
- Déployer sur un vrai cluster (10+ workers) au lieu d'un Docker local
- Surveiller le pipeline avec Grafana + Prometheus

---

### Q2 — Auriez-vous pu utiliser Redis, Cassandra ou PostgreSQL à la place de HBase ?

Oui, chaque technologie est techniquement possible, mais le choix dépend des contraintes du projet :

| Base | Avantages | Inconvénients | Adapté ici ? |
|------|-----------|---------------|:------------:|
| **HBase** | Intégration native HDFS/Hadoop, scale horizontal, lecture/écriture par clé en O(1) | API verbeuse, pas de SQL, opérationnel complexe | ✅ Oui |
| **Redis** | Latence sub-milliseconde, structures de données riches (sorted sets) | Données en mémoire uniquement (coûteux à grande échelle), pas de persistance native | ⚠️ Pour du cache, pas du stockage long terme |
| **Cassandra** | Très haute disponibilité, pas de SPOF, bon pour les séries temporelles | Pas d'intégration Hadoop native, modèle de données rigide | ✅ Alternative sérieuse |
| **PostgreSQL** | SQL standard, facile à interroger, écosystème riche | Ne scale pas horizontalement, écriture concurrente limitée à fort volume | ❌ Non adapté à 1M events/s |

**Critères de choix :**
1. **Volume** : HBase et Cassandra gèrent les pétaoctets ; PostgreSQL non
2. **Intégration** : HBase s'intègre nativement avec HDFS déjà présent dans notre cluster
3. **Accès** : lecture par catégorie (row key) = O(1) dans HBase, parfait pour notre cas
4. **Coût** : tout est open-source sur notre cluster Hadoop existant, pas de coût additionnel

---

### Q3 — Comment justifier cette architecture à votre DSI en 5 minutes ?

**Le problème métier :**
> Notre plateforme e-commerce génère des milliers d'événements par seconde. Le DSI veut savoir, **en temps réel**, quel rayon génère le plus de chiffre d'affaires, et pouvoir **relancer des analyses historiques** sur 1 an de données.

**L'architecture Lambda répond aux deux besoins :**

```
Besoin temps réel  → Speed layer  : Kafka + Spark + HBase  (latence < 1 min)
Besoin historique  → Batch layer  : HDFS                   (analyses sur 1 an)
```

**Justification de chaque choix :**

| Technologie | Pourquoi ce choix | Valeur métier |
|-------------|-------------------|---------------|
| **Kafka** | File d'attente distribuée tolérante aux pannes ; si Spark tombe, les messages sont conservés | Zéro perte d'événements même en cas de panne |
| **Spark Structured Streaming** | Traitement en micro-batches, SQL-like, s'intègre nativement avec Kafka et HBase | Agrégation du CA par catégorie toutes les 30 s |
| **HBase** | Lecture par clé en millisecondes même avec des milliards de lignes | Dashboard temps réel alimenté instantanément |
| **HDFS** | Stockage distribué et pas cher (commodity hardware) | Rejouer n'importe quelle analyse batch sur 1 an d'historique |

**Coût :** infrastructure open-source sur cluster interne, pas de licence.

**Complexité :** pipeline en Java/Python, déployable avec `spark-submit`, monitorable avec les outils Hadoop standards.

**Valeur métier :** détection immédiate d'une chute de CA (ex. catégorie Sport -50 % en 5 min), alertes automatisables, historique conservé pour le marketing et la direction.

---

---

## Pour aller plus loin — Extensions implementees

### 1. Visualisation (matplotlib)

Le script `visualize_hbase.py` lit la table `sales` et genere 4 graphiques :

```bash
pip install matplotlib numpy
python3 visualize_hbase.py
```

| Fichier | Description |
|---------|-------------|
| `chart_ca.png` | CA par categorie (barres) |
| `chart_purchases.png` | Nombre d'achats par categorie (barres) |
| `chart_combined.png` | CA + tendance achats (double axe Y) |
| `chart_radar.png` | Radar chart toutes metriques (normalise) |

---

### 2. Detection d'anomalies

Integree dans `EcommercePipeline.java` — a chaque batch, le CA courant est compare au batch precedent. Si la chute depasse **50%**, une alerte est emise :

```
!!! ANOMALIE DETECTEE - Batch 3 !!!
!!! Categorie  : Livres
!!! CA precedent : 3011.09 EUR
!!! CA actuel    : 636.69 EUR
!!! Variation    : -78.9%
!!! ACTION : verifier la disponibilite produit !
```

Extrait du code (`EcommercePipeline.java`, methode `detectAnomaly`) :
```java
if (changePct < -50.0) {
    System.out.println("!!! ANOMALIE DETECTEE - Batch " + batchId + " !!!");
    System.out.println("!!! Categorie : " + category);
    System.out.printf("!!! Variation : %.1f%%%n", changePct);
}
```

---

### 3. Chiffrement AES-128 des montants

`AESUtil.java` chiffre le `total_amount` avant ecriture dans HBase.

Dans HBase, `stats:total_amount` contient le montant chiffre en Base64 :
```
stats:total_amount  value=d7sXPKvLLj2jqF/eYF6VSA==   <-- chiffre AES
stats:total_amount_plain  value=636.69               <-- clair (debug)
```

Pour dechiffrer :
```java
String montant = AESUtil.decrypt("d7sXPKvLLj2jqF/eYF6VSA==");
// -> "636.69"
```

---

### 4. Architecture Kappa (`KappaPipeline.java`)

**Principe** : supprimer la batch layer HDFS et tout traiter via Spark.
Kafka est la source de verite (retention longue = 30 jours).

```
Lambda  : Kafka -> Spark -> HBase  ET  Kafka -> HDFS
Kappa   : Kafka -> Spark -> HBase (events_raw + sales)
```

| Critere | Lambda | Kappa |
|---------|--------|-------|
| Nombre de codebases | 2 (batch + streaming) | 1 |
| Complexite ops | Haute | Moyenne |
| Rejoue historique | HDFS | Kafka (retention longue) |
| Latence batch | Haute (H+1) | Basse (streaming) |

Pour lancer l'architecture Kappa :
```bash
# Creer la table events_raw dans HBase
echo "create 'events_raw','data'" | hbase shell -n

spark-submit --class tn.insat.tp5.KappaPipeline \
  --master local[4] --driver-memory 512m \
  target/ecommerce-pipeline-1-jar-with-dependencies.jar
```

---

### 5. Alternative Cassandra (`CassandraSink.java`)

`CassandraSink.java` montre comment remplacer HBase par Cassandra.

Schema CQL equivalent :
```sql
CREATE KEYSPACE ecommerce WITH replication = {
  'class': 'SimpleStrategy', 'replication_factor': 1
};
CREATE TABLE ecommerce.sales (
  category       text PRIMARY KEY,
  total_amount   double,
  purchase_count bigint,
  last_update    bigint
);
```

| Critere | HBase | Cassandra |
|---------|-------|-----------|
| Latence ecriture | ~2-5 ms | ~1-3 ms |
| Schema | Flexible | CQL obligatoire |
| Integration Hadoop | Native | Independant |
| Disponibilite | SPOF possible | Peer-to-peer, pas de SPOF |
| Cas ideal | Cluster Hadoop existant | Haute dispo, ecriture massive |

---

## Technologies

- Apache Kafka 3.6.1 (2.13)
- Apache Spark 3.5.0 (Structured Streaming)
- Apache HBase 2.5.8
- Apache Hadoop 3.3.6 (HDFS)
- Java 1.8
- Python 3.x / kafka-python
- Maven 3.x
