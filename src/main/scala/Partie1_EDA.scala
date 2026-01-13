//> using scala 2.12.18
//> using dep org.apache.spark::spark-core:3.5.0
//> using dep org.apache.spark::spark-sql:3.5.0
//> using javaOpt -Xmx8g
//> using javaOpt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang=ALL-UNNAMED
//> using javaOpt -Dio.netty.tryReflectionSetAccessible=true

/**
 * ============================================================================
 * PARTIE 1 – PRISE EN MAIN DES DONNÉES (EDA BRUTE)
 * ============================================================================
 * 
 * Objectifs:
 *   1. Charger les fichiers CSV et JSON
 *   2. Analyser la volumétrie des données
 *   3. Évaluer la qualité des données
 * 
 * Fichiers utilisés:
 *   - transactions_data.csv
 *   - cards_data.csv
 *   - users_data.csv
 *   - mcc_codes.json
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Partie1_EDA {
  def main(args: Array[String]): Unit = {
    // =========================================================================
    // INITIALISATION SPARK
    // =========================================================================
    val spark = SparkSession.builder()
      .appName("Partie 1 - EDA")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val basePath = "./"

    println("\n" + "=" * 80)
    println("PARTIE 1 – PRISE EN MAIN DES DONNÉES (EDA BRUTE)")
    println("=" * 80)

    // =========================================================================
    // 1. CHARGEMENT DES DONNÉES
    // =========================================================================
    println("\n--- 1. CHARGEMENT DES DONNÉES ---\n")
    
    // Chargement des fichiers CSV avec schéma inféré
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "transactions_data.csv")
    
    val cardsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "cards_data.csv")
    
    val usersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "users_data.csv")
    
    // Chargement du fichier JSON MCC
    val mccRawDF = spark.read
      .option("multiLine", "true")
      .json(basePath + "mcc_codes.json")
    
    // Transformation du JSON MCC en format tabulaire
    val mccColumns = mccRawDF.columns
    val mccCodesDF = mccColumns.map { code =>
      (code, mccRawDF.select(col(s"`$code`")).first().getString(0))
    }.toSeq.toDF("mcc_code", "mcc_category")

    // Affichage des schémas et des 10 premières lignes
    println("=" * 60)
    println("[transactions_data.csv]")
    println("=" * 60)
    println(s"Nombre de colonnes: ${transactionsDF.columns.length}")
    println(s"Colonnes: ${transactionsDF.columns.mkString(", ")}")
    transactionsDF.printSchema()
    transactionsDF.show(10, truncate = false)

    println("\n" + "=" * 60)
    println("[cards_data.csv]")
    println("=" * 60)
    println(s"Nombre de colonnes: ${cardsDF.columns.length}")
    println(s"Colonnes: ${cardsDF.columns.mkString(", ")}")
    cardsDF.printSchema()
    cardsDF.show(10, truncate = false)

    println("\n" + "=" * 60)
    println("[users_data.csv]")
    println("=" * 60)
    println(s"Nombre de colonnes: ${usersDF.columns.length}")
    println(s"Colonnes: ${usersDF.columns.mkString(", ")}")
    usersDF.printSchema()
    usersDF.show(10, truncate = false)

    println("\n" + "=" * 60)
    println("[mcc_codes.json]")
    println("=" * 60)
    println(s"Nombre de colonnes: ${mccCodesDF.columns.length}")
    println(s"Nombre de codes MCC: ${mccCodesDF.count()}")
    mccCodesDF.printSchema()
    mccCodesDF.show(10, truncate = false)

    // Réponse à la question: types suspects
    println("\n" + "=" * 60)
    println("📌 TYPES SUSPECTS IDENTIFIÉS")
    println("=" * 60)
    println("""
      | Colonne          | Type actuel | Problème                          | Type attendu
      | ---------------- | ----------- | --------------------------------- | ------------
      | amount           | String      | Contient '$' (ex: "$45.67")       | Double
      | zip              | Double      | Code postal (ex: 01234)           | String
      | credit_limit     | String      | Contient '$'                      | Double
      | yearly_income    | String      | Contient '$' et format spécial    | Double
    """.stripMargin)

    // =========================================================================
    // 2. ANALYSE DE VOLUMÉTRIE
    // =========================================================================
    println("\n--- 2. ANALYSE DE VOLUMÉTRIE ---\n")
    
    val totalTransactions = transactionsDF.count()
    val clientsUniques = transactionsDF.select("client_id").distinct().count()
    val cartesUniques = transactionsDF.select("card_id").distinct().count()
    val commercantsUniques = transactionsDF.select("merchant_id").distinct().count()

    println("+" + "-" * 40 + "+" + "-" * 15 + "+")
    println("| Métrique                               | Valeur         |")
    println("+" + "-" * 40 + "+" + "-" * 15 + "+")
    println(f"| Transactions totales                   | $totalTransactions%13d |")
    println(f"| Clients uniques                        | $clientsUniques%13d |")
    println(f"| Cartes uniques                         | $cartesUniques%13d |")
    println(f"| Commerçants uniques                    | $commercantsUniques%13d |")
    println("+" + "-" * 40 + "+" + "-" * 15 + "+")

    println(s"\n💡 INTERPRÉTATION: Qui génère le plus de lignes?")
    println(s"   → Ratio transactions/carte   = ${totalTransactions.toDouble / cartesUniques}")
    println(s"   → Ratio transactions/client  = ${totalTransactions.toDouble / clientsUniques}")
    println(s"   → Les CARTES génèrent le plus de lignes (chaque carte a plusieurs transactions)")

    // =========================================================================
    // 3. QUALITÉ DES DONNÉES
    // =========================================================================
    println("\n--- 3. QUALITÉ DES DONNÉES ---\n")
    
    // Nettoyage du montant
    val transactionsClean = transactionsDF
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))

    // Tableau des valeurs nulles
    println("📋 TABLEAU DES VALEURS NULLES (transactions_data.csv):")
    println("+" + "-" * 20 + "+" + "-" * 12 + "+" + "-" * 14 + "+")
    println("| Colonne            | Nb Nulls   | Pourcentage  |")
    println("+" + "-" * 20 + "+" + "-" * 12 + "+" + "-" * 14 + "+")
    
    transactionsDF.columns.foreach { colName =>
      val nullCount = transactionsDF.filter(col(colName).isNull || col(colName) === "").count()
      val percentage = nullCount * 100.0 / totalTransactions
      println(f"| $colName%-18s | $nullCount%10d | $percentage%11.4f%% |")
    }
    println("+" + "-" * 20 + "+" + "-" * 12 + "+" + "-" * 14 + "+")

    // Problèmes de qualité
    val montantsNegatifs = transactionsClean.filter(col("amount_clean") <= 0).count()
    val sansMCC = transactionsDF.filter(col("mcc").isNull || col("mcc") === "").count()
    val avecErreurs = transactionsDF.filter(col("errors").isNotNull && col("errors") =!= "").count()

    println(s"\n⚠️  PROBLÈMES DE QUALITÉ:")
    println(f"   • Transactions avec montant ≤ 0 : $montantsNegatifs (${montantsNegatifs * 100.0 / totalTransactions}%.4f%%)")
    println(f"   • Transactions sans MCC         : $sansMCC (${sansMCC * 100.0 / totalTransactions}%.4f%%)")
    println(f"   • Transactions avec erreurs     : $avecErreurs (${avecErreurs * 100.0 / totalTransactions}%.2f%%)")

    // Tableau récapitulatif
    println("\n" + "=" * 60)
    println("TABLEAU RÉCAPITULATIF - PARTIE 1")
    println("=" * 60)
    println(s"""
      | Fichier              | Lignes    | Colonnes
      | -------------------- | --------- | --------
      | transactions_data    | $totalTransactions     | ${transactionsDF.columns.length}
      | cards_data           | ${cardsDF.count()}      | ${cardsDF.columns.length}
      | users_data           | ${usersDF.count()}      | ${usersDF.columns.length}
      | mcc_codes            | ${mccCodesDF.count()}       | ${mccCodesDF.columns.length}
    """.stripMargin)

    println("\n✅ FIN DE LA PARTIE 1")
    spark.stop()
  }
}

