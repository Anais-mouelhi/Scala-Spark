import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FraudAnalysis {
  def main(args: Array[String]): Unit = {
    // Création de la session Spark
    val spark = SparkSession.builder()
      .appName("Fraud Analysis - Risk & Fraud Team")
      .master("local[*]")
      .getOrCreate()

    // Réduire les logs
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // =============================================================================
    // PARTIE 1 – Prise en main des données (EDA brute)
    // =============================================================================

    println("=" * 80)
    println("PARTIE 1 – PRISE EN MAIN DES DONNÉES (EDA)")
    println("=" * 80)

    // -------------------------------------------------------------------------
    // 1. CHARGEMENT DES DONNÉES
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("1. CHARGEMENT DES DONNÉES")
    println("=" * 80)

    // Chemin de base des données
    val basePath = "exoTP/"

    // Chargement des fichiers CSV avec schéma inféré
    println("\n>>> Chargement de transactions_data.csv...")
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "transactions_data.csv")

    println("\n>>> Chargement de cards_data.csv...")
    val cardsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "cards_data.csv")

    println("\n>>> Chargement de users_data.csv...")
    val usersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "users_data.csv")

    // Chargement des fichiers JSON
    println("\n>>> Chargement de mcc_codes.json...")
    // Le fichier JSON est un objet simple, on le lit différemment
    val mccCodesRaw = spark.read
      .option("multiLine", "true")
      .json(basePath + "mcc_codes.json")
    
    // Transformer le JSON en DataFrame exploitable (code MCC -> catégorie)
    val mccColumns = mccCodesRaw.columns
    val mccCodesDF = mccColumns.map { col =>
      (col, mccCodesRaw.select(col).first().getString(0))
    }.toSeq.toDF("mcc_code", "mcc_category")

    println("\n>>> Chargement de train_fraud_labels.json...")
    val fraudLabelsRaw = spark.read
      .option("multiLine", "true")
      .json(basePath + "train_fraud_labels.json")
    
    // Transformer en DataFrame exploitable (transaction_id -> is_fraud)
    val fraudColumns = fraudLabelsRaw.columns
    val fraudLabelsDF = fraudColumns.map { col =>
      (col.toLong, fraudLabelsRaw.select(col).first().getString(0))
    }.toSeq.toDF("transaction_id", "is_fraud")

    // -------------------------------------------------------------------------
    // Affichage des schémas
    // -------------------------------------------------------------------------
    println("\n" + "-" * 60)
    println("SCHÉMA DE transactions_data:")
    println("-" * 60)
    transactionsDF.printSchema()
    println(s"Nombre de colonnes: ${transactionsDF.columns.length}")
    println("Colonnes: " + transactionsDF.columns.mkString(", "))

    println("\n" + "-" * 60)
    println("SCHÉMA DE cards_data:")
    println("-" * 60)
    cardsDF.printSchema()
    println(s"Nombre de colonnes: ${cardsDF.columns.length}")
    println("Colonnes: " + cardsDF.columns.mkString(", "))

    println("\n" + "-" * 60)
    println("SCHÉMA DE users_data:")
    println("-" * 60)
    usersDF.printSchema()
    println(s"Nombre de colonnes: ${usersDF.columns.length}")
    println("Colonnes: " + usersDF.columns.mkString(", "))

    println("\n" + "-" * 60)
    println("SCHÉMA DE mcc_codes:")
    println("-" * 60)
    mccCodesDF.printSchema()
    println(s"Nombre de colonnes: ${mccCodesDF.columns.length}")

    println("\n" + "-" * 60)
    println("SCHÉMA DE fraud_labels:")
    println("-" * 60)
    fraudLabelsDF.printSchema()
    println(s"Nombre de colonnes: ${fraudLabelsDF.columns.length}")

    // -------------------------------------------------------------------------
    // Affichage des 10 premières lignes
    // -------------------------------------------------------------------------
    println("\n" + "-" * 60)
    println("10 PREMIÈRES LIGNES DE transactions_data:")
    println("-" * 60)
    transactionsDF.show(10, truncate = false)

    println("\n" + "-" * 60)
    println("10 PREMIÈRES LIGNES DE cards_data:")
    println("-" * 60)
    cardsDF.show(10, truncate = false)

    println("\n" + "-" * 60)
    println("10 PREMIÈRES LIGNES DE users_data:")
    println("-" * 60)
    usersDF.show(10, truncate = false)

    println("\n" + "-" * 60)
    println("10 PREMIÈRES LIGNES DE mcc_codes:")
    println("-" * 60)
    mccCodesDF.show(10, truncate = false)

    println("\n" + "-" * 60)
    println("10 PREMIÈRES LIGNES DE fraud_labels:")
    println("-" * 60)
    fraudLabelsDF.show(10, truncate = false)

    // -------------------------------------------------------------------------
    // Analyse des types suspects
    // -------------------------------------------------------------------------
    println("\n" + "-" * 60)
    println("ANALYSE DES TYPES DE DONNÉES SUSPECTS:")
    println("-" * 60)
    println("""
    |OBSERVATIONS:
    |
    |1. transactions_data:
    |   - 'amount' est de type String (contient '$' et possiblement des valeurs négatives)
    |   - 'zip' est de type Double (devrait être String car c'est un code postal)
    |   - 'mcc' est de type Integer (ok, mais devra être joint avec mcc_codes)
    |   - 'date' est de type String (devrait être Timestamp)
    |
    |2. cards_data:
    |   - 'credit_limit' est de type String (contient '$')
    |   - 'expires' est de type String (format MM/YYYY)
    |   - 'card_number' pourrait poser des problèmes de précision (Long)
    |
    |3. users_data:
    |   - 'per_capita_income', 'yearly_income', 'total_debt' sont de type String (contiennent '$')
    |   - 'latitude', 'longitude' sont de type Double (ok)
    """.stripMargin)

    // -------------------------------------------------------------------------
    // 2. ANALYSE DE VOLUMÉTRIE
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("2. ANALYSE DE VOLUMÉTRIE")
    println("=" * 80)

    val totalTransactions = transactionsDF.count()
    val totalClients = transactionsDF.select("client_id").distinct().count()
    val totalCardsUsed = transactionsDF.select("card_id").distinct().count()
    val totalMerchants = transactionsDF.select("merchant_id").distinct().count()

    val totalCardsInDB = cardsDF.count()
    val totalUsersInDB = usersDF.count()

    println(s"""
    |VOLUMÉTRIE DES DONNÉES:
    |
    |  - Nombre total de transactions:     $totalTransactions
    |  - Nombre de clients uniques:        $totalClients
    |  - Nombre de cartes utilisées:       $totalCardsUsed
    |  - Nombre de commerçants uniques:    $totalMerchants
    |
    |  - Nombre de cartes dans cards_data: $totalCardsInDB
    |  - Nombre d'utilisateurs dans users: $totalUsersInDB
    |  - Nombre de codes MCC:              ${mccCodesDF.count()}
    |  - Nombre de labels de fraude:       ${fraudLabelsDF.count()}
    """.stripMargin)

    // Qui génère le plus de lignes ?
    val transactionsPerClient = transactionsDF
      .groupBy("client_id")
      .agg(count("*").alias("nb_transactions"))
      .orderBy(desc("nb_transactions"))

    println("\n" + "-" * 60)
    println("TOP 10 CLIENTS PAR NOMBRE DE TRANSACTIONS:")
    println("-" * 60)
    transactionsPerClient.show(10)

    val transactionsPerMerchant = transactionsDF
      .groupBy("merchant_id")
      .agg(count("*").alias("nb_transactions"))
      .orderBy(desc("nb_transactions"))

    println("\n" + "-" * 60)
    println("TOP 10 COMMERÇANTS PAR NOMBRE DE TRANSACTIONS:")
    println("-" * 60)
    transactionsPerMerchant.show(10)

    val transactionsPerCard = transactionsDF
      .groupBy("card_id")
      .agg(count("*").alias("nb_transactions"))
      .orderBy(desc("nb_transactions"))

    println("\n" + "-" * 60)
    println("TOP 10 CARTES PAR NOMBRE DE TRANSACTIONS:")
    println("-" * 60)
    transactionsPerCard.show(10)

    println("""
    |INTERPRÉTATION:
    |Les transactions sont générées par les clients. Chaque client peut avoir
    |plusieurs cartes et effectuer des achats chez différents commerçants.
    |Les commerçants de type 'chaînes' (grandes enseignes) génèrent naturellement
    |plus de transactions que les petits commerces locaux.
    """.stripMargin)

    // -------------------------------------------------------------------------
    // 3. QUALITÉ DES DONNÉES
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("3. QUALITÉ DES DONNÉES")
    println("=" * 80)

    // Fonction pour nettoyer le montant (enlever $ et convertir en Double)
    val cleanAmount = regexp_replace(col("amount"), "[$,]", "").cast(DoubleType)

    // Ajouter une colonne amount_clean pour l'analyse
    val transactionsWithCleanAmount = transactionsDF
      .withColumn("amount_clean", cleanAmount)

    // 3.1 Colonnes avec valeurs nulles
    println("\n" + "-" * 60)
    println("3.1 ANALYSE DES VALEURS NULLES - TRANSACTIONS:")
    println("-" * 60)

    val transactionNullCounts = transactionsDF.columns.map { colName =>
      val nullCount = transactionsDF.filter(col(colName).isNull || col(colName) === "").count()
      val percentage = (nullCount.toDouble / totalTransactions) * 100
      (colName, nullCount, f"$percentage%.2f%%")
    }

    println("\n| Colonne              | Nb Nulls    | Pourcentage |")
    println("|" + "-" * 22 + "|" + "-" * 13 + "|" + "-" * 13 + "|")
    transactionNullCounts.foreach { case (col, count, pct) =>
      println(f"| $col%-20s | $count%11d | $pct%11s |")
    }

    // 3.2 Transactions avec montant <= 0
    println("\n" + "-" * 60)
    println("3.2 TRANSACTIONS AVEC MONTANT <= 0:")
    println("-" * 60)

    val negativeOrZeroTransactions = transactionsWithCleanAmount
      .filter(col("amount_clean") <= 0)
      .count()

    val negativeTransactions = transactionsWithCleanAmount
      .filter(col("amount_clean") < 0)
      .count()

    val zeroTransactions = transactionsWithCleanAmount
      .filter(col("amount_clean") === 0)
      .count()

    println(s"""
    |  - Transactions avec montant <= 0: $negativeOrZeroTransactions
    |    - Montants négatifs:            $negativeTransactions
    |    - Montants à zéro:              $zeroTransactions
    |  - Pourcentage du total:           ${f"${(negativeOrZeroTransactions.toDouble / totalTransactions) * 100}%.2f"}%%
    """.stripMargin)

    println("\nExemples de transactions avec montants négatifs:")
    transactionsWithCleanAmount
      .filter(col("amount_clean") < 0)
      .select("id", "date", "client_id", "amount", "amount_clean", "merchant_id")
      .show(10, truncate = false)

    // 3.3 Transactions sans MCC
    println("\n" + "-" * 60)
    println("3.3 TRANSACTIONS SANS CODE MCC:")
    println("-" * 60)

    val transactionsWithoutMCC = transactionsDF
      .filter(col("mcc").isNull || col("mcc") === "")
      .count()

    println(s"""
    |  - Transactions sans MCC:  $transactionsWithoutMCC
    |  - Pourcentage du total:   ${f"${(transactionsWithoutMCC.toDouble / totalTransactions) * 100}%.2f"}%%
    """.stripMargin)

    // 3.4 Transactions avec erreurs
    println("\n" + "-" * 60)
    println("3.4 TRANSACTIONS AVEC ERREURS:")
    println("-" * 60)

    val transactionsWithErrors = transactionsDF
      .filter(col("errors").isNotNull && col("errors") =!= "")
      .count()

    println(s"""
    |  - Transactions avec erreurs:  $transactionsWithErrors
    |  - Pourcentage du total:       ${f"${(transactionsWithErrors.toDouble / totalTransactions) * 100}%.2f"}%%
    """.stripMargin)

    // Distribution des types d'erreurs
    println("\nDistribution des types d'erreurs:")
    transactionsDF
      .filter(col("errors").isNotNull && col("errors") =!= "")
      .groupBy("errors")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
      .show(20, truncate = false)

    // 3.5 Analyse de qualité pour cards_data
    println("\n" + "-" * 60)
    println("3.5 ANALYSE DES VALEURS NULLES - CARDS:")
    println("-" * 60)

    val cardsNullCounts = cardsDF.columns.map { colName =>
      val nullCount = cardsDF.filter(col(colName).isNull || col(colName) === "").count()
      val percentage = (nullCount.toDouble / totalCardsInDB) * 100
      (colName, nullCount, f"$percentage%.2f%%")
    }

    println("\n| Colonne              | Nb Nulls    | Pourcentage |")
    println("|" + "-" * 22 + "|" + "-" * 13 + "|" + "-" * 13 + "|")
    cardsNullCounts.foreach { case (col, count, pct) =>
      println(f"| $col%-20s | $count%11d | $pct%11s |")
    }

    // 3.6 Analyse de qualité pour users_data
    println("\n" + "-" * 60)
    println("3.6 ANALYSE DES VALEURS NULLES - USERS:")
    println("-" * 60)

    val usersNullCounts = usersDF.columns.map { colName =>
      val nullCount = usersDF.filter(col(colName).isNull || col(colName) === "").count()
      val percentage = (nullCount.toDouble / totalUsersInDB) * 100
      (colName, nullCount, f"$percentage%.2f%%")
    }

    println("\n| Colonne              | Nb Nulls    | Pourcentage |")
    println("|" + "-" * 22 + "|" + "-" * 13 + "|" + "-" * 13 + "|")
    usersNullCounts.foreach { case (col, count, pct) =>
      println(f"| $col%-20s | $count%11d | $pct%11s |")
    }

    // -------------------------------------------------------------------------
    // TABLEAU RÉCAPITULATIF
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("TABLEAU RÉCAPITULATIF - QUALITÉ DES DONNÉES")
    println("=" * 80)

    println(s"""
    |+----------------------------------+----------------+------------------+
    || Indicateur                       | Nombre         | Pourcentage      |
    |+----------------------------------+----------------+------------------+
    || Total transactions               | $totalTransactions%14d |                  |
    || Montants <= 0                    | $negativeOrZeroTransactions%14d | ${f"${(negativeOrZeroTransactions.toDouble / totalTransactions) * 100}%.2f"}%%             |
    || Montants négatifs                | $negativeTransactions%14d | ${f"${(negativeTransactions.toDouble / totalTransactions) * 100}%.2f"}%%             |
    || Sans code MCC                    | $transactionsWithoutMCC%14d | ${f"${(transactionsWithoutMCC.toDouble / totalTransactions) * 100}%.2f"}%%             |
    || Avec erreurs                     | $transactionsWithErrors%14d | ${f"${(transactionsWithErrors.toDouble / totalTransactions) * 100}%.2f"}%%             |
    |+----------------------------------+----------------+------------------+
    """.stripMargin)

    // Fermeture de la session Spark
    println("\n" + "=" * 80)
    println("FIN DE LA PARTIE 1 - ANALYSE EXPLORATOIRE COMPLÈTE")
    println("=" * 80)

    spark.stop()
  }
}

