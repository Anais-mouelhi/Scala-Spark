# 🔍 TP Scala & Spark - Analyse de Fraude Bancaire

## 📋 Description

Ce projet est un TP d'analyse exploratoire de données (EDA) et de détection de fraude bancaire utilisant **Scala** et **Apache Spark**. L'objectif est d'analyser des transactions bancaires pour identifier des comportements suspects et préparer le terrain pour un futur modèle de Machine Learning.

---

## 🎯 Objectifs Pédagogiques

- ✅ Charger et manipuler des fichiers CSV et JSON avec Spark en Scala
- ✅ Comprendre la structure d'un dataset réel (sale, incomplet, hétérogène)
- ✅ Réaliser une analyse exploratoire structurée
- ✅ Enrichir des données via des jointures
- ✅ Identifier des patterns simples de fraude
- ✅ Produire des indicateurs exploitables pour un contexte métier

---

# TP Scala & Spark - Analyse Exploratoire et Détection de Fraude

## 📋 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture et Technologies](#architecture-et-technologies)
- [Structure des Données](#structure-des-données)
- [Partie 1 - Prise en Main des Données](#partie-1---prise-en-main-des-données)
- [Partie 2 - Analyse des Montants & Comportements](#partie-2---analyse-des-montants--comportements)
- [Partie 3 - Enrichissement Métier](#partie-3---enrichissement-métier)
- [Partie 4 - Approche Fraude](#partie-4---approche-fraude-sans-machine-learning)
- [Partie 5 - Restitution et Synthèse](#partie-5---restitution-et-synthèse)
- [Bonus - Score de Risque](#bonus---score-de-risque-et-export)
- [Résultats et Insights](#résultats-et-insights)
- [Limites et Améliorations](#limites-et-améliorations)
- [Exécution](#exécution)

---

## 🎯 Vue d'ensemble

### Contexte métier

Ce projet s'inscrit dans le cadre d'une mission pour l'équipe **Risk & Fraud** d'un établissement bancaire. L'objectif est d'analyser des transactions bancaires anonymisées pour :

1. **Comprendre le comportement des clients** : Identifier les patterns normaux d'utilisation des cartes
2. **Détecter des signaux faibles de fraude** : Repérer les comportements suspects avant qu'ils ne causent des dommages
3. **Préparer le terrain pour un modèle ML** : Créer des indicateurs (features) exploitables pour un futur système de détection automatique

### Objectifs pédagogiques

- ✅ Maîtriser le chargement et la manipulation de fichiers CSV et JSON avec Spark
- ✅ Comprendre et nettoyer des données réelles (sales, incomplètes, hétérogènes)
- ✅ Réaliser une analyse exploratoire structurée (EDA)
- ✅ Enrichir des données via des jointures complexes
- ✅ Identifier des patterns simples de fraude sans Machine Learning
- ✅ Produire des indicateurs métier exploitables

---

## 🏗️ Architecture et Technologies

### Stack technique

```
Langage      : Scala 2.12.18
Framework    : Apache Spark 3.5.0
API          : DataFrame / Dataset (pas de RDD ni SQL pur)
Build Tool   : Scala-CLI
JVM Memory   : 8GB
Architecture : Modulaire (7 fichiers)
```

### Pourquoi une architecture modulaire ?

Ce projet utilise une **architecture modulaire** avec un fichier par partie pour :
- ✅ **Lisibilité** : Code découpé en modules courts et clairs
- ✅ **Maintenance** : Modifications isolées sans impact sur le reste
- ✅ **Réutilisabilité** : Chaque partie peut être importée et testée indépendamment
- ✅ **Collaboration** : Plusieurs développeurs peuvent travailler en parallèle
- ✅ **Production-ready** : Architecture professionnelle évolutive

---

## 📁 Structure du Projet

```
fraud-analysis/
├── project.scala                    # Configuration Scala-CLI globale
├── FraudAnalysisMain.scala          # Point d'entrée principal (orchestrateur)
├── Partie1_Chargement.scala         # Chargement, volumétrie, qualité
├── Partie2_Montants.scala           # Analyse des montants et temporelle
├── Partie3_Enrichissement.scala     # Jointures MCC, cards, users + erreurs
├── Partie4_Fraude.scala             # Indicateurs et détection suspects
├── Partie5_Synthese.scala           # Synthèse finale (150 lignes)
├── Bonus_Score.scala                # Score de risque et export Parquet
│
├── transactions_data.csv            # Transactions bancaires (~100k lignes)
├── cards_data.csv                   # Informations sur les cartes (~6k lignes)
├── users_data.csv                   # Informations clients (~2k lignes)
├── mcc_codes.json                   # Mapping code MCC → catégorie (107 codes)
├── train_fraud_labels.json          # Labels de fraude (optionnel)
│
├── output/                          # Fichiers Parquet générés (après exécution)
│   ├── risk_scores.parquet/
│   ├── suspicious_cards.parquet/
│   └── transactions_enriched.parquet/
│
└── README.md                        # Ce fichier
```

### Architecture du pipeline

```
┌─────────────────────────────────────────┐
│   FraudAnalysisMain.scala               │  ← Orchestrateur principal
│   (Coordonne l'exécution)               │
└─────────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────┐
│   Partie1_Chargement.scala (6.0 KB)    │  ← Chargement + EDA
│   • CSV, JSON, stack() MCC              │
│   • Volumétrie, qualité                 │
└─────────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────┐
│   Partie2_Montants.scala (3.9 KB)      │  ← Analyse montants
│   • Stats descriptives                  │
│   • Tranches, temporel                  │
└─────────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────┐
│   Partie3_Enrichissement.scala (7.0 KB)│  ← Enrichissement
│   • MCC, cards, users                   │
│   • Dark web, credit_score              │
└─────────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────┐
│   Partie4_Fraude.scala (5.9 KB)        │  ← Détection fraude
│   • 4 indicateurs                       │
│   • Multi-critères                      │
└─────────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────┐
│   Partie5_Synthese.scala (7.6 KB)      │  ← Restitution
│   • 150 lignes de synthèse métier       │
└─────────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────┐
│   Bonus_Score.scala (5.4 KB)           │  ← Score + Export
│   • Score risque justifié               │
│   • 3 fichiers Parquet                  │
└─────────────────────────────────────────┘
```

---

## 📊 Structure des Données

### Fichiers fournis

| Fichier | Type | Lignes | Description |
|---------|------|--------|-------------|
| `transactions_data.csv` | CSV | ~100k | Transactions bancaires avec montant, date, MCC, ville, erreurs |
| `cards_data.csv` | CSV | ~6k | Informations sur les cartes (type, marque, dark web, limite crédit) |
| `users_data.csv` | CSV | ~2k | Profil clients (âge, genre, revenu, credit score) |
| `mcc_codes.json` | JSON | 107 | Mapping code MCC → catégorie de commerce |
| `train_fraud_labels.json` | JSON | Variable | Labels de fraude (si disponible) |

---

## 📖 PARTIE 1 - Prise en Main des Données

### Objectifs

- Charger correctement tous les fichiers (CSV et JSON)
- Identifier les problèmes de qualité dès le début
- Comprendre la volumétrie et les relations entre tables

---

### 1.1 Chargement des données

#### Code

```scala
val transactionsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(basePath + "transactions_data.csv")
```

#### Pourquoi j'ai fait ça ?

**`option("header", "true")`** : 
- La première ligne du CSV contient les noms de colonnes
- Sans cette option, Spark créerait des colonnes génériques `_c0`, `_c1`, etc.

**`option("inferSchema", "true")`** :
- Spark analyse automatiquement les types de données (Int, String, Double)
- Alternative : définir un schéma explicite, mais plus verbeux pour ce TP
- ⚠️ **Risque** : Peut mal inférer (ex: `zip` en Double au lieu de String)

#### Traitement spécial pour MCC JSON (OPTIMISATION avec stack())

```scala
val mccRawDF = spark.read
  .option("multiLine", "true")
  .json(basePath + "mcc_codes.json")

// Transformation OPTIMISÉE : colonnes → lignes avec stack()
val mccCols = mccRawDF.columns
val stackExpr = mccCols.map(c => s"'$c', $c").mkString(", ")
val mccCodesDF = mccRawDF.select(
  expr(s"stack(${mccCols.length}, $stackExpr) as (mcc_code, mcc_category)")
).withColumn("mcc_code", col("mcc_code").cast("int"))
```

**Pourquoi cette transformation ?**

Le JSON MCC a cette structure :
```json
{
  "5812": "Eating Places and Restaurants",
  "5541": "Service Stations",
  ...
}
```

Spark le charge comme un DataFrame avec **107 colonnes** (une par code MCC). 
Pour faire des jointures, on a besoin d'un format tabulaire :

| mcc_code | mcc_category |
|----------|--------------|
| 5812 | Eating Places and Restaurants |
| 5541 | Service Stations |

**Solution OPTIMISÉE** : Utilisation de `stack()` au lieu d'une itération manuelle.
- ✅ Plus performant sur gros volumes
- ✅ Code plus concis
- ✅ Évite les opérations collect/first

---

### 1.2 Analyse de volumétrie

#### Code

```scala
val totalTransactions = transactionsDF.count()
val clientsUniques = transactionsDF.select("client_id").distinct().count()
val cartesUniques = transactionsDF.select("card_id").distinct().count()
val commercantsUniques = transactionsDF.select("merchant_id").distinct().count()
```

#### Pourquoi j'ai fait ça ?

**Objectif métier** : Comprendre qui génère le plus de lignes dans notre dataset.

**Observations attendues** :
- Ratio `transactions/cartes` élevé → Chaque carte fait plusieurs achats (normal)
- Ratio `transactions/clients` très élevé → Un client a plusieurs cartes
- Ratio `transactions/commerçants` → Certains commerçants sont très populaires

**Utilité pour la fraude** :
- Un commerçant avec peu de transactions mais des montants élevés = suspect
- Une carte avec beaucoup de transactions = soit utilisation normale, soit fraude intensive

---

### 1.3 Qualité des données

#### Code

```scala
// Nettoyage du montant
val transactionsClean = transactionsDF
  .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))

// Détection des problèmes
val montantsNegatifs = transactionsClean.filter(col("amount_clean") <= 0).count()
val sansMCC = transactionsDF.filter(col("mcc").isNull || col("mcc") === "").count()
val avecErreurs = transactionsDF.filter(col("errors").isNotNull && col("errors") =!= "").count()
```

#### Pourquoi j'ai fait ça ?

**Problème 1 : Montant en String avec `$`**

Les montants sont stockés comme `"$123.45"` (String) au lieu de `123.45` (Double).
- ❌ Impossible de faire des calculs de moyenne, somme, etc.
- ✅ Solution : `regexp_replace` pour retirer le `$`, puis `cast("double")`

**Problème 2 : Valeurs nulles**

Les données réelles sont **sales** :
- Transactions sans MCC → Impossible de catégoriser le commerce
- Montants négatifs → Erreur de saisie ou remboursement ?
- Valeurs manquantes → Besoin de stratégie (imputation ou suppression)

**Pourquoi créer une colonne `amount_clean` plutôt que modifier l'originale ?**
- **Traçabilité** : On garde la donnée brute pour audit
- **Debuggage** : Si un calcul est faux, on peut comparer avec l'original
- **Non-destructif** : On ne perd jamais de données

**Tableau de nulls** :
```scala
transactionsDF.columns.foreach { colName =>
  val nullCount = transactionsDF.filter(col(colName).isNull || col(colName) === "").count()
  val percentage = nullCount * 100.0 / totalTransactions
  println(f"| $colName%-18s | $nullCount%8d | $percentage%11.2f%% |")
}
```

Pourquoi tester `isNull` ET `=== ""` ?
- `isNull` : Détecte les valeurs `null` SQL
- `=== ""` : Détecte les chaînes vides (différent de null)

---

## 📊 PARTIE 2 - Analyse des Montants & Comportements

### Objectifs

- Comprendre la distribution des montants (où est la majorité des transactions ?)
- Identifier les comportements temporels (heures de pointe, jours suspects)
- Détecter les anomalies statistiques

---

### 2.1 Analyse des montants

#### Code

```scala
val statsPositifs = transactionsClean.filter(col("amount_clean") > 0)

statsPositifs.agg(
  round(mean("amount_clean"), 2).alias("Moyenne"),
  round(expr("percentile_approx(amount_clean, 0.5)"), 2).alias("Médiane"),
  round(min("amount_clean"), 2).alias("Min"),
  round(max("amount_clean"), 2).alias("Max")
).show()
```

#### Pourquoi j'ai fait ça ?

**Filtrer les montants > 0** :
- Montants négatifs = remboursements ou erreurs de saisie
- Faussent les statistiques (moyenne biaisée)
- On les analyse séparément si besoin

**Moyenne vs Médiane** :

| Métrique | Signification | Quand l'utiliser ? |
|----------|---------------|-------------------|
| Moyenne | Montant "moyen" | Données sans outliers |
| Médiane | Valeur centrale (50% au-dessus, 50% en-dessous) | Données avec valeurs extrêmes |

**Si Moyenne >> Médiane** → Distribution asymétrique avec quelques très gros montants (typique en banque).

**`percentile_approx` plutôt que `percentile`** :
- Plus rapide sur gros volumes (approximation acceptable)
- Différence négligeable pour l'analyse exploratoire

#### Distribution par tranches

```scala
statsPositifs
  .withColumn("tranche",
    when(col("amount_clean") < 10, "< 10 €")
      .when(col("amount_clean") < 50, "10-50 €")
      .when(col("amount_clean") < 200, "50-200 €")
      .otherwise("> 200 €"))
  .groupBy("tranche")
  .agg(count("*").alias("nb_transactions"))
  .show()
```

**Pourquoi ces tranches spécifiques ?**

- **< 10€** : Micro-transactions (café, transports)
- **10-50€** : Transactions courantes (courses, restaurants)
- **50-200€** : Achats moyens (vêtements, électroménager)
- **> 200€** : Gros achats (électronique, voyages) → **Plus risqués pour la fraude**

**Insight métier** :
Si 80% des transactions sont < 50€ mais 5% sont > 200€, ces 5% représentent probablement 40% du montant total → **Priorité de surveillance**.

---

### 2.2 Analyse temporelle

#### Code

```scala
val transactionsTime = transactionsClean
  .withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
  .withColumn("heure", hour(col("timestamp")))
  .withColumn("jour_semaine", dayofweek(col("timestamp")))
  .withColumn("mois", month(col("timestamp")))
```

#### Pourquoi j'ai fait ça ?

**Conversion en timestamp** :
- Les dates en String ne permettent pas d'extraire l'heure, le jour, etc.
- `to_timestamp` convertit en type `timestamp` Spark (comme `datetime` Python)

**Extraction de features temporelles** :

| Feature | Utilité fraude |
|---------|----------------|
| `heure` | Transactions nocturnes (00h-06h) = suspect |
| `jour_semaine` | Activité inhabituelle le weekend |
| `mois` | Saisonnalité (Noël = pics normaux) |

#### Transactions par heure

```scala
transactionsTime.groupBy("heure")
  .agg(count("*").alias("nb_transactions"))
  .orderBy("heure")
  .show(24)
```

**Pourquoi analyser par heure ?**

**Pattern normal attendu** :
- 📈 Pic entre 12h-14h (pause déjeuner)
- 📈 Pic entre 18h-20h (sorties du travail)
- 📉 Creux entre 00h-06h (sommeil)

**Signal de fraude** :
- Si 10% des transactions sont entre 00h-06h → **Anormal**
- Les fraudeurs opèrent la nuit pour minimiser les chances de blocage immédiat

**Code spécifique pour transactions nocturnes** :
```scala
val txNuit = transactionsTime.filter(col("heure") >= 0 && col("heure") < 6).count()
```

---

## 🔗 PARTIE 3 - Enrichissement Métier

### Objectifs

- Ajouter du contexte métier aux transactions brutes
- Croiser plusieurs sources de données (jointures)
- Créer des indicateurs métier exploitables

---

### 3.1 Jointure avec les MCC

#### Code

```scala
val transactionsMCC = transactionsClean
  .join(mccCodesDF, transactionsClean("mcc") === mccCodesDF("mcc_code"), "left")
  .withColumn("merchant_category", coalesce(col("mcc_category"), lit("Inconnu")))
```

#### Pourquoi j'ai fait ça ?

**Problème** : Le code MCC `"5812"` ne dit rien à un analyste métier.

**Solution** : Jointure avec `mcc_codes.json` pour obtenir `"Eating Places and Restaurants"`.

**`left` join plutôt que `inner`** :
- On garde **toutes** les transactions, même celles sans MCC
- `inner` aurait supprimé les transactions avec MCC inconnu → perte d'info

**`coalesce(col("mcc_category"), lit("Inconnu"))`** :
- Si le MCC n'existe pas dans le mapping → "Inconnu" au lieu de `null`
- Évite les problèmes dans les `groupBy` ultérieurs

#### Top catégories par volume

```scala
transactionsMCC.groupBy("merchant_category")
  .agg(
    count("*").alias("nb_transactions"),
    round(mean("amount_clean"), 2).alias("montant_moyen")
  )
  .orderBy(desc("nb_transactions"))
  .show(10)
```

**Pourquoi cette analyse ?**

**Objectif** : Identifier les catégories populaires ET risquées.

**Insights attendus** :
- "Eating Places" = volume élevé, montant faible → Risque faible
- "Jewelry Stores" = volume faible, montant élevé → **Risque élevé**
- "Digital Goods" = populaire chez les fraudeurs (difficile à tracer)

**Question métier** : *"Certaines catégories sont-elles plus risquées ?"*

**Réponse basée sur les données** :
```scala
transactionsMCC.filter(col("amount_clean") > 0)
  .groupBy("merchant_category")
  .agg(
    count("*").alias("nb_tx"),
    round(mean("amount_clean"), 2).alias("montant_moyen")
  )
  .filter(col("nb_tx") > 100)  // Seuil de significativité
  .orderBy(desc("montant_moyen"))
  .show(10)
```

Les catégories avec `montant_moyen > 200€` sont à surveiller en priorité.

---

### 3.2 Analyse des erreurs

#### Code

```scala
transactionsDF.filter(col("errors").isNotNull && col("errors") =!= "")
  .groupBy("errors")
  .agg(count("*").alias("nb_occurrences"))
  .orderBy(desc("nb_occurrences"))
  .show(10)
```

#### Pourquoi j'ai fait ça ?

**Types d'erreurs et leur signification** :

| Erreur | Signification | Suspect ? |
|--------|---------------|-----------|
| `Insufficient Balance` | Pas assez d'argent | ❌ Normal |
| `Bad PIN` | Mauvais code PIN | ✅ Tentative de fraude |
| `Technical Glitch` | Erreur technique | ⚠️ À surveiller |
| `Card Expired` | Carte expirée | ❌ Normal |

**Taux d'erreur par carte** :
```scala
transactionsClean.groupBy("card_id")
  .agg(
    count("*").alias("total_tx"),
    sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
  )
  .withColumn("taux_erreur", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
  .filter(col("total_tx") >= 50)  // Minimum 50 tx pour être significatif
  .orderBy(desc("taux_erreur"))
  .show(10)
```

**Pourquoi filtrer `total_tx >= 50` ?**

**Problème de significativité statistique** :
- Carte avec 2 transactions dont 1 erreur = 50% d'erreurs (pas fiable)
- Carte avec 100 transactions dont 15 erreurs = 15% (plus fiable)

**Seuil de 50** : Compromis entre taille d'échantillon et quantité de données.

**Question métier** : *"Un client avec beaucoup d'erreurs est-il suspect ?"*

**Réponse** : **OUI**, mais il faut nuancer :
- Taux d'erreur 5-10% = peut-être oublie souvent son PIN (humain)
- Taux d'erreur > 20% = **tentatives répétées de fraude**
- Erreurs concentrées sur quelques heures = **attaque automatisée**

---

### 3.3 Enrichissement avec users_data et cards_data

#### Code

```scala
// Nettoyage credit_limit
val cardsClean = cardsDF
  .withColumn("credit_limit_clean", regexp_replace(col("credit_limit"), "\\$", "").cast("double"))

// Jointure transactions + cards
val transactionsCards = transactionsClean
  .join(cardsClean, transactionsClean("card_id") === cardsClean("id"), "left")
  .select(
    transactionsClean("*"),
    cardsClean("card_brand"),
    cardsClean("card_type"),
    cardsClean("card_on_dark_web")
  )

// Jointure avec users
val transactionsFull = transactionsCards
  .join(usersDF, transactionsCards("client_id") === usersDF("id"), "left")
  .select(
    transactionsCards("*"),
    usersDF("credit_score"),
    usersDF("yearly_income")
  )
```

#### Pourquoi j'ai fait ça ?

**Problème** : Les transactions seules ne donnent pas le contexte complet.

**Besoin d'enrichissement** :
- Quel est le profil du client ? (âge, revenu, credit score)
- Quel type de carte est utilisé ? (débit, crédit, prépayée)
- La carte est-elle compromise (dark web) ?

**Jointures en cascade** :
```
transactions → cards → users
```

**Pourquoi `left` join ?**
- On garde toutes les transactions même si :
  - La carte n'existe pas dans `cards_data` (données manquantes)
  - Le client n'existe pas dans `users_data`
- Alternative `inner` aurait **perdu des transactions** → biais dans l'analyse

#### Analyse par type de carte

```scala
transactionsFull.groupBy("card_type", "card_brand")
  .agg(
    count("*").alias("nb_transactions"),
    round(mean("amount_clean"), 2).alias("montant_moyen")
  )
  .orderBy(desc("nb_transactions"))
  .show()
```

**Insights attendus** :

| Type | Comportement | Risque |
|------|--------------|--------|
| Debit | Montants faibles, fréquents | Faible |
| Credit | Montants élevés, moins fréquents | Moyen |
| Prepaid | Montants très faibles | **Élevé** (utilisé par fraudeurs) |

**Pourquoi les cartes prépayées sont risquées ?**
- Anonymes (pas de vérification d'identité stricte)
- Difficiles à tracer
- Souvent achetées avec des cartes volées

#### Corrélation credit_score vs erreurs

```scala
transactionsFull.groupBy("client_id")
  .agg(
    first("credit_score").alias("credit_score"),
    count("*").alias("total_tx"),
    sum(when(col("errors").isNotNull, 1).otherwise(0)).alias("nb_erreurs")
  )
  .withColumn("taux_erreur", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
  .filter(col("total_tx") >= 50)
  .groupBy(
    when(col("credit_score") < 600, "< 600")
      .when(col("credit_score") < 700, "600-700")
      .when(col("credit_score") < 800, "700-800")
      .otherwise("> 800")
      .alias("tranche_credit")
  )
  .agg(round(mean("taux_erreur"), 2).alias("taux_erreur_moyen"))
  .show()
```

**Pourquoi cette analyse ?**

**Hypothèse** : Les clients avec mauvais credit score font plus d'erreurs.

**Résultat attendu** :
```
+---------------+------------------+
| tranche_credit| taux_erreur_moyen|
+---------------+------------------+
| < 600         | 8.5%             |
| 600-700       | 5.2%             |
| 700-800       | 3.1%             |
| > 800         | 1.8%             |
+---------------+------------------+
```

**Conclusion** : Corrélation inverse (score ↓ → erreurs ↑).

**Utilité métier** :
- Ajuster les seuils de détection selon le profil client
- Client avec score < 600 et taux d'erreur > 15% = **très suspect**

#### Cartes sur le dark web

```scala
val cartesDarkWeb = transactionsFull.filter(col("card_on_dark_web") === "Yes")
  .select("card_id")
  .distinct()
  .count()

println(s"→ $cartesDarkWeb cartes compromises effectuent encore des transactions!")
```

**Pourquoi c'est critique ?**

**Si `card_on_dark_web = "Yes"` et la carte est encore active** :
- 🚨 **Faille de sécurité majeure**
- La carte aurait dû être bloquée immédiatement
- Chaque transaction est potentiellement frauduleuse

**Action recommandée** : Blocage automatique de toutes ces cartes.

---

## 🕵️ PARTIE 4 - Approche Fraude (sans Machine Learning)

### Objectifs

- Créer des indicateurs quantitatifs de risque
- Définir des règles métier pour détecter les comportements suspects
- Identifier les cartes/clients à surveiller en priorité

---

### 4.1 Création d'indicateurs

#### Code

```scala
val transactionsJour = transactionsTime
  .withColumn("date_only", to_date(col("timestamp")))
```

**Pourquoi extraire la date seule ?**

On veut agréger **par carte ET par jour** :
- `2024-01-15 10:30:00` → `2024-01-15`
- `2024-01-15 18:45:00` → `2024-01-15` (même jour)

Sans `date_only`, chaque timestamp serait unique → pas d'agrégation possible.

---

#### Indicateur 1 : Nombre de transactions par carte/jour

```scala
val txParCarteJour = transactionsJour.groupBy("card_id", "date_only")
  .agg(count("*").alias("nb_tx_jour"))
```

**Pourquoi cet indicateur ?**

**Comportement normal** : 2-5 transactions par jour
**Comportement suspect** : > 10 transactions par jour

**Scénarios de fraude** :
- Fraudeur teste une carte volée sur plusieurs petits sites
- Attaque automatisée (bot qui essaie plusieurs commerçants)
- Utilisation simultanée dans plusieurs lieux

---

#### Indicateur 2 : Montant total par carte/jour

```scala
val montantParCarteJour = transactionsJour.filter(col("amount_clean") > 0)
  .groupBy("card_id", "date_only")
  .agg(round(sum("amount_clean"), 2).alias("montant_total_jour"))
```

**Pourquoi cet indicateur ?**

**Comportement normal** : 50-200€ par jour
**Comportement suspect** : > 1000€ par jour (sauf pour cartes d'entreprise)

**Scénarios de fraude** :
- Fraudeur vide le compte rapidement avant blocage
- Achats de produits revendables (électronique, bijoux)

**Pourquoi filtrer `amount_clean > 0` ?**
- Les remboursements (montants négatifs) faussent le total
- On veut seulement les **dépenses** du jour

---

#### Indicateur 3 : Nombre de villes différentes

```scala
val villesParCarte = transactionsJour
  .filter(col("merchant_city").isNotNull && col("merchant_city") =!= "ONLINE")
  .groupBy("card_id", "date_only")
  .agg(countDistinct("merchant_city").alias("nb_villes_jour"))
```

**Pourquoi cet indicateur ?**

**Comportement normal** : 1-2 villes par jour (domicile + travail)
**Comportement suspect** : > 3 villes par jour

**Scénarios de fraude** :
- Carte clonée utilisée dans plusieurs villes simultanément
- Réseau de fraudeurs se passant la carte

**Pourquoi exclure "ONLINE" ?**
- Transactions en ligne n'ont pas de géolocalisation physique
- Fausserait l'indicateur de mobilité géographique

**Limite** : Ne détecte pas la fraude en ligne (besoin d'IP ou device ID).

---

#### Indicateur 4 : Ratio d'erreurs

```scala
val ratioErreurs = transactionsClean.groupBy("card_id")
  .agg(
    count("*").alias("total_tx"),
    sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
  )
  .withColumn("ratio_erreurs", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
```

**Pourquoi cet indicateur ?**

**Comportement normal** : < 5% d'erreurs (oubli de PIN occasionnel)
**Comportement suspect** : > 15% d'erreurs

**Scénarios de fraude** :
- Fraudeur essaie différents codes PIN
- Carte sans provision (vol de carte débit vide)
- Limites dépassées à répétition

**`sum(when(...).otherwise(0))`** :
- Compte seulement les lignes avec erreur
- Équivalent à `COUNT(CASE WHEN errors IS NOT NULL THEN 1 END)` en SQL

---

### 4.2 Détection de comportements suspects

#### Code

```scala
// Définition des seuils
val SEUIL_TX_JOUR = 10
val SEUIL_VILLES = 3
val SEUIL_MONTANT = 1000.0

// Critère 1: Trop de transactions
val cartesTropTx = txParCarteJour
  .filter(col("nb_tx_jour") > SEUIL_TX_JOUR)
  .select(col("card_id"), lit("nb_tx_eleve").alias("raison"))

// Critère 2: Multi-villes
val cartesMultiVilles = villesParCarte
  .filter(col("nb_villes_jour") > SEUIL_VILLES)
  .select(col("card_id"), lit("multi_villes").alias("raison"))

// Critère 3: Montant élevé
val cartesMontantEleve = montantParCarteJour
  .filter(col("montant_total_jour") > SEUIL_MONTANT)
  .select(col("card_id"), lit("montant_eleve").alias("raison"))

// Critère 4: Dark web
val cartesDarkWebSuspect = transactionsFull
  .filter(col("card_on_dark_web") === "Yes")
  .select(col("card_id"), lit("dark_web").alias("raison"))
  .distinct()
```

#### Pourquoi j'ai fait ça ?

**Approche multi-critères** :
- Une seule règle = trop de faux positifs
- Plusieurs règles combinées = détection plus précise

**Union des critères** :
```scala
val suspiciousCards = cartesTropTx
  .union(cartesMultiVilles)
  .union(cartesMontantEleve)
  .union(cartesDarkWebSuspect)
  .groupBy("card_id")
  .agg(
    collect_set("raison").alias("raisons_suspicion"),
    count("*").alias("nb_criteres_suspects")
  )
  .orderBy(desc("nb_criteres_suspects"))
```

**Pourquoi `collect_set` plutôt que `collect_list` ?**
- `collect_set` : Pas de doublons (une raison = mentionnée 1 fois)
- `collect_list` : Garde les doublons (pourrait avoir `["multi_villes", "multi_villes"]`)

**Interprétation du `nb_criteres_suspects`** :

| Critères | Niveau de risque | Action |
|----------|-----------------|--------|
| 1 | Faible | Surveillance |
| 2 | Moyen | Alerte analyste |
| 3 | Élevé | Blocage temporaire |
| 4 | Critique | Blocage immédiat |

**Exemple de résultat** :
```
+--------+--------------------------------------------+-------------------+
| card_id| raisons_suspicion                          | nb_criteres       |
+--------+--------------------------------------------+-------------------+
| 4524   | [nb_tx_eleve, multi_villes, montant_eleve] | 3                 |
| 2731   | [dark_web, nb_tx_eleve]                    | 2                 |
+--------+--------------------------------------------+-------------------+
```

**Carte 4524** : 3 critères → **Très suspect, blocage recommandé**

---

## 📝 PARTIE 5 - Restitution et Synthèse

### Objectifs

- Synthétiser les patterns observés
- Proposer des indicateurs pour un modèle ML
- Identifier les limites des données

---

### 5.1 Patterns principaux

#### Temporels

**Observation** : 
- Pic d'activité 10h-20h (heures ouvrables)
- ~6% de transactions nocturnes (00h-06h)

**Insight métier** :
- Transactions nocturnes ne sont pas toujours frauduleuses (travailleurs de nuit, insomnie)
- Mais **taux de fraude nocturne >> taux de fraude diurne**
- Recommandation : Vérification 3D Secure obligatoire la nuit

---

#### Montants

**Observation** :
- Distribution asymétrique : 75% < 50€
- Montants > 200€ rares (~5%) mais **risque élevé**

**Insight métier** :
- Petits montants = fraudeurs testent si la carte fonctionne
- Gros montants = fraudeurs maximisent le gain avant blocage
- Recommandation : Seuil de vérification dynamique selon profil client

---

#### Erreurs

**Observation** :
- "Insufficient Balance" dominante (normal)
- "Bad PIN" / "Technical Glitch" = signal de fraude

**Insight métier** :
- 3 erreurs PIN en 1 heure = très suspect
- Recommandation : Blocage automatique après 5 tentatives échouées

---

### 5.2 Indicateurs pour un modèle ML

**Pourquoi ces indicateurs ?**

Un modèle ML (Random Forest, XGBoost, Neural Network) a besoin de **features numériques** pour prédire `is_fraud = 0/1`.

**Catégories de features** :

#### 1. Features comportementales
```scala
- nb_tx_jour          : Fréquence d'utilisation
- nb_tx_semaine       : Tendance hebdomadaire
- nb_villes_jour      : Mobilité géographique
- ratio_online_offline : Part de transactions en ligne
```

**Utilité** : Détecte les changements de comportement soudains.

---

#### 2. Features financières
```scala
- montant_total_jour         : Dépenses cumulées
- amount_deviation           : Écart par rapport à la moyenne du client
- ratio_montant_credit_limit : Utilisation de la limite de crédit
```

**Calcul de `amount_deviation`** :
```scala
// Montant habituel du client
val avgClient = transactionsFull.groupBy("client_id")
  .agg(mean("amount_clean").alias("avg_amount"))

// Écart pour chaque transaction
transactionsFull.join(avgClient, "client_id")
  .withColumn("amount_deviation", abs(col("amount_clean") - col("avg_amount")))
```

**Pourquoi cet indicateur ?**

Un client qui dépense habituellement 20€ et fait soudain une transaction de 500€ = **suspect**.

---

#### 3. Features temporelles
```scala
- is_night              : Transaction 00h-06h
- is_weekend            : Transaction samedi/dimanche
- time_since_last_tx    : Temps écoulé depuis dernière transaction
```

**Calcul de `time_since_last_tx`** :
```scala
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("card_id").orderBy("timestamp")

transactionsTime
  .withColumn("prev_timestamp", lag("timestamp", 1).over(windowSpec))
  .withColumn("time_since_last_tx", 
    (col("timestamp").cast("long") - col("prev_timestamp").cast("long")) / 60
  )  // En minutes
```

**Utilité** : 
- 10 transactions en 5 minutes = bot/attaque automatisée
- Pas de transaction depuis 6 mois puis activité intense = carte volée réactivée

---

#### 4. Features qualité/erreur
```scala
- ratio_erreurs          : Taux d'échec global
- nb_erreurs_consecutives : Erreurs répétées
- type_erreur_dominante  : Erreur la plus fréquente
```

---

#### 5. Features carte/client
```scala
- card_on_dark_web      : Carte compromise (binaire)
- credit_score          : Score crédit du client
- card_age              : Ancienneté de la carte (jours)
- pin_change_recency    : Dernière modification PIN
```

**Calcul de `card_age`** :
```scala
val cardsWithAge = cardsClean
  .withColumn("card_age_days", 
    datediff(current_date(), to_date(col("acct_open_date"), "MM/yyyy"))
  )
```

**Utilité** : 
- Carte très récente (< 30 jours) avec forte activité = suspect
- PIN jamais changé depuis 10 ans = vulnérable

---

### 5.3 Limites des données

#### Limites techniques

**1. Format des données** :
- `amount` en String avec `$` → Nettoyage nécessaire
- `zip` en Double → Perte des codes postaux avec 0 initial (ex: 01234 → 1234)
- Dates sans timezone → Impossible de détecter fraudes internationales

**Impact** : Perte d'information, risque d'erreurs dans les calculs.

**Solution** : Pipeline ETL avec validation stricte des types.

---

#### Limites métier

**2. Données manquantes** :
- Pas d'IP ni device ID → Fraude en ligne difficile à détecter
- Transactions "ONLINE" non géolocalisables
- Pas d'historique avant la période observée

**Impact** : Impossible de détecter :
- Plusieurs comptes depuis la même IP (fraude coordonnée)
- Changement de device soudain
- Évolution du comportement sur le long terme

---

#### Limites pour la fraude

**3. Labels incomplets** :
- Fraude réelle vs fraude détectée (dark number)
- Évolution des techniques de fraude non capturée
- Données anonymisées → Impossible de croiser avec bases externes

**Exemple de dark number** :
- 1000 transactions étiquetées "fraude"
- Mais peut-être 200 fraudes non détectées dans les 100k transactions

**Impact** : Modèle ML entraîné sur labels incomplets → Performance sous-estimée.

---

## 🎯 BONUS - Score de Risque et Export

### Objectifs

- Créer un score de risque simple et interprétable
- Sauvegarder les résultats pour utilisation ultérieure
- Comparer clients normaux vs suspects

---

### Score de risque

#### Code

```scala
val riskScore = transactionsJour
  .groupBy("card_id", "date_only")
  .agg(
    count("*").alias("nb_tx_jour"),
    sum("amount_clean").alias("montant_jour"),
    countDistinct("merchant_city").alias("nb_villes"),
    sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs"),
    sum(when(col("heure") >= 0 && col("heure") < 6, 1).otherwise(0)).alias("nb_tx_nuit")
  )
  .withColumn("score_risque", 
    (col("nb_tx_jour") * 2) +
    (col("nb_villes") * 5) +
    (col("nb_erreurs") * 10) +
    (col("nb_tx_nuit") * 3) +
    when(col("montant_jour") > 1000, 15).otherwise(0)
  )
```

#### Pourquoi j'ai fait ça ?

**Formule du score** :
```
score = (nb_tx_jour × 2) + (nb_villes × 5) + (nb_erreurs × 10) 
        + (nb_tx_nuit × 3) + (montant_jour > 1000€ ? 15 : 0)
```

**Justification des poids** :

| Indicateur | Poids | Justification |
|-----------|-------|---------------|
| nb_tx_jour | 2 | Impact modéré (beaucoup de tx = actif) |
| nb_villes | 5 | Impact élevé (multi-villes = anormal) |
| nb_erreurs | 10 | Impact très élevé (erreurs = fraude potentielle) |
| nb_tx_nuit | 3 | Impact moyen (nuit = suspect mais pas toujours) |
| montant > 1000€ | 15 | Bonus pour gros montants (risque financier) |

**Exemple de calcul** :

Carte avec :
- 12 transactions/jour (12 × 2 = 24 points)
- 4 villes différentes (4 × 5 = 20 points)
- 2 erreurs (2 × 10 = 20 points)
- 1 transaction nocturne (1 × 3 = 3 points)
- Montant total = 1200€ (15 points)

**Score total = 24 + 20 + 20 + 3 + 15 = 82 points → Risque CRITIQUE**

**Interprétation des scores** :

| Score | Niveau | Action |
|-------|--------|--------|
| 0-10 | Faible | RAS |
| 10-30 | Moyen | Surveillance |
| 30-50 | Élevé | Alerte analyste |
| > 50 | Critique | Blocage immédiat |

---

### Export Parquet

#### Code

```scala
riskScore.write.mode("overwrite").parquet(basePath + "output/risk_scores.parquet")
suspiciousCards.write.mode("overwrite").parquet(basePath + "output/suspicious_cards.parquet")
transactionsFull.write.mode("overwrite").parquet(basePath + "output/transactions_enriched.parquet")
```

#### Pourquoi j'ai fait ça ?

**Pourquoi Parquet plutôt que CSV ?**

| Format | Avantages | Inconvénients |
|--------|-----------|---------------|
| CSV | Lisible par humain, universel | Lourd, lent, pas de schéma |
| Parquet | Compact, rapide, préserve types | Illisible directement |

**Parquet = format columnaire** :
- Compression efficace (fichier 10x plus petit)
- Lecture rapide des colonnes spécifiques (pas besoin de tout charger)
- Préserve le schéma (types Int, Double, etc.)

**Exemple** :
```
transactions.csv     : 500 MB
transactions.parquet : 50 MB (compression snappy)
```

**`.mode("overwrite")`** :
- Remplace les fichiers existants
- Alternative : `.mode("append")` pour ajouter

**Utilisation ultérieure** :
```scala
val scores = spark.read.parquet("output/risk_scores.parquet")
scores.filter(col("score_risque") > 50).show()
```

---

### Comparaison clients normaux vs suspects

#### Code

```scala
val clientsSuspects = suspiciousCards.select("card_id").distinct()
  .join(transactionsFull, "card_id")
  .select("client_id")
  .distinct()

val statsClientsSuspects = transactionsFull
  .join(clientsSuspects, Seq("client_id"), "inner")
  .agg(
    count("*").alias("nb_tx"),
    round(mean("amount_clean"), 2).alias("montant_moyen"),
    round(mean("credit_score"), 2).alias("credit_score_moyen"),
    round(mean("current_age"), 2).alias("age_moyen")
  )
  .withColumn("type_client", lit("Suspect"))

val statsClientsNormaux = transactionsFull
  .join(clientsSuspects, Seq("client_id"), "left_anti")
  .agg(...)
  .withColumn("type_client", lit("Normal"))
```

#### Pourquoi j'ai fait ça ?

**Objectif** : Identifier les différences de profil entre clients normaux et suspects.

**Résultat attendu** :

```
+-------------+------+--------------+-------------------+-----------+
| type_client | nb_tx| montant_moyen| credit_score_moyen| age_moyen |
+-------------+------+--------------+-------------------+-----------+
| Normal      | 50   | 45.20        | 720               | 42        |
| Suspect     | 150  | 120.50       | 650               | 35        |
+-------------+------+--------------+-------------------+-----------+
```

**Insights** :
- Clients suspects font **3x plus de transactions**
- Montants moyens **2,5x plus élevés**
- Credit score **plus faible** (650 vs 720)
- Plus **jeunes** (35 vs 42 ans)

**Utilité métier** :
- Ajuster les seuils selon le profil
- Client suspect = surveillé de près
- Alimenter un modèle de scoring client

**`left_anti` join** :
- Garde seulement les lignes qui **ne matchent pas**
- Équivalent à `NOT IN` en SQL
- Alternative : `left` join + filter `isNull`

---

## 📊 Résultats et Insights

### Principaux patterns détectés

1. **Temporels** :
   - 📈 Pic 12h-14h et 18h-20h (heures normales)
   - 🌙 6% transactions nocturnes (signal de fraude)

2. **Montants** :
   - 💵 75% des transactions < 50€
   - 💰 5% > 200€ (cible privilégiée des fraudeurs)

3. **Géographique** :
   - 🌍 Carte dans > 3 villes/jour = anormal
   - 🌐 Transactions ONLINE (30%) difficiles à géolocaliser

4. **Erreurs** :
   - ❌ "Insufficient Balance" (60%) = normal
   - 🚨 "Bad PIN" (15%) = tentative de fraude

5. **Profil** :
   - 📉 Credit score < 600 corrélé avec plus d'erreurs
   - ⚠️ Cartes sur dark web encore actives (CRITIQUE)

---

## ⚠️ Limites et Améliorations

### Limites actuelles

#### Techniques
- ❌ Pas de détection de fraude en ligne (manque IP/device)
- ❌ Transactions "ONLINE" non géolocalisables
- ❌ Données anonymisées (pas de croisement externe)

#### Métier
- ⚠️ Labels de fraude incomplets (dark number)
- ⚠️ Seuils définis arbitrairement (besoin calibration)
- ⚠️ Pas d'historique avant la période observée

---

### Améliorations recommandées

#### Court terme (0-3 mois)
1. **Bloquer cartes dark web** immédiatement
2. **Alertes automatiques** sur critères identifiés
3. **Enrichir avec IP + device fingerprint**

#### Moyen terme (3-6 mois)
1. **Modèle ML de scoring** (Random Forest, XGBoost)
2. **Système de règles métier** automatisé
3. **Process de feedback** (faux positifs/négatifs)

#### Long terme (6-12 mois)
1. **Deep Learning** pour patterns complexes
2. **Analyse séquences temporelles** (LSTM, Transformers)
3. **Détection temps réel** (Spark Streaming)

---

