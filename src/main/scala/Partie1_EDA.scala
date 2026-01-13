import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Partie1_EDA {
  
  def execute(spark: SparkSession, basePath: String): 
    (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    
    import spark.implicits._
    
    // =========================================================================
    // PARTIE 1 – PRISE EN MAIN DES DONNÉES (EDA BRUTE)
    // =========================================================================
    println("\n" + "=" * 80)
    println("PARTIE 1 – PRISE EN MAIN DES DONNÉES (EDA BRUTE)")
    println("=" * 80)

    // -------------------------------------------------------------------------
    // 1. CHARGEMENT DES DONNÉES
    // -------------------------------------------------------------------------
    println("\n--- 1. CHARGEMENT DES DONNÉES ---\n")
    
    // Chargement des fichiers CSV
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
    
    // Chargement du fichier JSON MCC (structure: {"code": "description", ...})
    val mccRawDF = spark.read
      .option("multiLine", "true")
      .json(basePath + "mcc_codes.json")
    
    // Transformation du JSON MCC en format tabulaire (OPTIMISATION avec stack())
    val mccCols = mccRawDF.columns
    val stackExpr = mccCols.map(c => s"'$c', $c").mkString(", ")
    val mccCodesDF = mccRawDF.select(
      expr(s"stack(${mccCols.length}, $stackExpr) as (mcc_code, mcc_category)")
    ).withColumn("mcc_code", col("mcc_code").cast("int"))

    // Affichage des schémas et des 10 premières lignes
    println("[transactions_data.csv]")
    println(s"Nombre de colonnes: ${transactionsDF.columns.length}")
    transactionsDF.printSchema()
    transactionsDF.show(10, truncate = false)

    println("\n[cards_data.csv]")
    println(s"Nombre de colonnes: ${cardsDF.columns.length}")
    cardsDF.printSchema()
    cardsDF.show(10, truncate = false)

    println("\n[users_data.csv]")
    println(s"Nombre de colonnes: ${usersDF.columns.length}")
    usersDF.printSchema()
    usersDF.show(10, truncate = false)

    println("\n[mcc_codes.json]")
    println(s"Nombre de colonnes: ${mccCodesDF.columns.length}")
    mccCodesDF.printSchema()
    mccCodesDF.show(10, truncate = false)

    println("\n📌 TYPES SUSPECTS IDENTIFIÉS:")
    println("  ❌ 'amount' est en String et contient '$' → doit être converti en Double")
    println("  ❌ 'zip' est en Double → devrait être String (code postal)")
    println("  ❌ 'credit_limit' dans cards_data contient '$' → doit être nettoyé")

    // -------------------------------------------------------------------------
    // 2. ANALYSE DE VOLUMÉTRIE
    // -------------------------------------------------------------------------
    println("\n--- 2. ANALYSE DE VOLUMÉTRIE ---\n")
    
    val totalTransactions = transactionsDF.count()
    val clientsUniques = transactionsDF.select("client_id").distinct().count()
    val cartesUniques = transactionsDF.select("card_id").distinct().count()
    val commercantsUniques = transactionsDF.select("merchant_id").distinct().count()

    println(s"📊 Transactions totales : $totalTransactions")
    println(s"👤 Clients uniques      : $clientsUniques")
    println(s"💳 Cartes uniques       : $cartesUniques")
    println(s"🏪 Commerçants uniques  : $commercantsUniques")
    println(s"\n💡 INTERPRÉTATION: Les cartes génèrent le plus de lignes")
    println(f"   → Ratio tx/carte = ${totalTransactions.toDouble / cartesUniques}%.2f")
    println(f"   → Ratio tx/client = ${totalTransactions.toDouble / clientsUniques}%.2f")

    // -------------------------------------------------------------------------
    // 3. QUALITÉ DES DONNÉES
    // -------------------------------------------------------------------------
    println("\n--- 3. QUALITÉ DES DONNÉES ---\n")
    
    // Nettoyage du montant (retirer le '$' et convertir en Double)
    val transactionsClean = transactionsDF
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))

    // Analyse des valeurs nulles pour transactions_data
    println("📋 TABLEAU DES VALEURS NULLES (transactions_data.csv):")
    println("+" + "-" * 20 + "+" + "-" * 10 + "+" + "-" * 14 + "+")
    println("| Colonne              | Nulls    | Pourcentage  |")
    println("+" + "-" * 20 + "+" + "-" * 10 + "+" + "-" * 14 + "+")
    
    transactionsDF.columns.foreach { colName =>
      val nullCount = transactionsDF.filter(col(colName).isNull || col(colName) === "").count()
      val percentage = nullCount * 100.0 / totalTransactions
      println(f"| $colName%-18s | $nullCount%8d | $percentage%11.2f%% |")
    }
    println("+" + "-" * 20 + "+" + "-" * 10 + "+" + "-" * 14 + "+")

    // Détection des problèmes de qualité
    val montantsNegatifs = transactionsClean.filter(col("amount_clean") <= 0).count()
    val sansMCC = transactionsDF.filter(col("mcc").isNull || col("mcc") === "").count()
    val avecErreurs = transactionsDF.filter(col("errors").isNotNull && col("errors") =!= "").count()

    println(s"\n⚠️  PROBLÈMES DE QUALITÉ DÉTECTÉS:")
    println(f"   • Transactions avec montant ≤ 0 : $montantsNegatifs (${montantsNegatifs * 100.0 / totalTransactions}%.2f%%)")
    println(f"   • Transactions sans MCC         : $sansMCC (${sansMCC * 100.0 / totalTransactions}%.2f%%)")
    println(f"   • Transactions avec erreurs     : $avecErreurs (${avecErreurs * 100.0 / totalTransactions}%.2f%%)")

    // Retourner les DataFrames pour les parties suivantes
    (transactionsDF, cardsDF, usersDF, mccCodesDF, transactionsClean)
  }
}