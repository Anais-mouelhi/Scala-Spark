import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Partie3_Enrichissement {
  
  def execute(spark: SparkSession, transactionsTime: DataFrame, mccCodesDF: DataFrame, 
              cardsDF: DataFrame, usersDF: DataFrame): (DataFrame, DataFrame) = {
    
    import spark.implicits._
    
    // =========================================================================
    // PARTIE 3 – ENRICHISSEMENT MÉTIER (MCC, ERREURS, USERS, CARDS)
    // =========================================================================
    println("\n" + "=" * 80)
    println("PARTIE 3 – ENRICHISSEMENT MÉTIER")
    println("=" * 80)

    // -------------------------------------------------------------------------
    // 6. JOINTURE AVEC LES MCC
    // -------------------------------------------------------------------------
    println("\n--- 6. JOINTURE AVEC LES MCC ---\n")
    
    val transactionsMCC = transactionsTime
      .join(mccCodesDF, transactionsTime("mcc") === mccCodesDF("mcc_code"), "left")
      .withColumn("merchant_category", coalesce(col("mcc_category"), lit("Inconnu")))

    println("🏆 TOP 10 CATÉGORIES PAR VOLUME:")
    transactionsMCC.groupBy("merchant_category")
      .agg(
        count("*").alias("nb_transactions"),
        round(mean("amount_clean"), 2).alias("montant_moyen")
      )
      .orderBy(desc("nb_transactions"))
      .show(10, truncate = false)

    println("💰 CATÉGORIES AVEC MONTANTS MOYENS LES PLUS ÉLEVÉS:")
    transactionsMCC.filter(col("amount_clean") > 0)
      .groupBy("merchant_category")
      .agg(
        count("*").alias("nb_tx"),
        round(mean("amount_clean"), 2).alias("montant_moyen")
      )
      .filter(col("nb_tx") > 100)
      .orderBy(desc("montant_moyen"))
      .show(10, truncate = false)

    println("💡 QUESTION: Certaines catégories sont-elles plus risquées?")
    println("   → OUI: Les catégories avec montants élevés (bijouteries, électronique, voyages)")
    println("   → Ces catégories sont des cibles privilégiées pour la fraude")

    // -------------------------------------------------------------------------
    // 7. ANALYSE DES ERREURS
    // -------------------------------------------------------------------------
    println("\n--- 7. ANALYSE DES ERREURS ---\n")
    
    println("🚨 TYPES D'ERREURS LES PLUS FRÉQUENTS:")
    transactionsMCC.filter(col("errors").isNotNull && col("errors") =!= "")
      .groupBy("errors")
      .agg(count("*").alias("nb_occurrences"))
      .orderBy(desc("nb_occurrences"))
      .show(10, truncate = false)

    println("💳 TOP 10 CARTES AVEC LE PLUS HAUT TAUX D'ERREUR:")
    transactionsMCC.groupBy("card_id")
      .agg(
        count("*").alias("total_tx"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
      )
      .withColumn("taux_erreur", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
      .filter(col("total_tx") >= 50)
      .orderBy(desc("taux_erreur"))
      .show(10)

    println("💡 INDICE: Un client avec beaucoup d'erreurs est-il suspect?")
    println("   → OUI: Taux d'erreur élevé = tentatives de fraude répétées")
    println("   → 'Insufficient Balance' peut être normal, mais 'Technical Glitch' ou 'Bad PIN' sont suspects")

    // -------------------------------------------------------------------------
    // ENRICHISSEMENT AVEC USERS_DATA ET CARDS_DATA
    // -------------------------------------------------------------------------
    println("\n--- ENRICHISSEMENT AVEC DONNÉES CLIENTS & CARTES ---\n")

    // Nettoyage credit_limit dans cards_data
    val cardsClean = cardsDF
      .withColumn("credit_limit_clean", regexp_replace(col("credit_limit"), "\\$", "").cast("double"))

    // Jointure transactions + cards
    val transactionsCards = transactionsMCC
      .join(cardsClean, transactionsMCC("card_id") === cardsClean("id"), "left")
      .select(
        transactionsMCC("*"),
        cardsClean("card_brand"),
        cardsClean("card_type"),
        cardsClean("card_on_dark_web"),
        cardsClean("credit_limit_clean"),
        cardsClean("has_chip"),
        cardsClean("year_pin_last_changed")
      )

    // Jointure avec users
    val transactionsFull = transactionsCards
      .join(usersDF, transactionsCards("client_id") === usersDF("id"), "left")
      .select(
        transactionsCards("*"),
        usersDF("current_age"),
        usersDF("gender"),
        usersDF("credit_score"),
        usersDF("yearly_income"),
        usersDF("num_credit_cards")
      )

    println("💳 ANALYSE PAR TYPE DE CARTE:")
    transactionsFull.groupBy("card_type", "card_brand")
      .agg(
        count("*").alias("nb_transactions"),
        round(mean("amount_clean"), 2).alias("montant_moyen")
      )
      .orderBy(desc("nb_transactions"))
      .show(truncate = false)

    println("📊 CORRÉLATION CREDIT_SCORE vs TAUX D'ERREUR:")
    transactionsFull.groupBy("client_id")
      .agg(
        first("credit_score").alias("credit_score"),
        count("*").alias("total_tx"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
      )
      .withColumn("taux_erreur", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
      .filter(col("total_tx") >= 50)
      .filter(col("credit_score").isNotNull)
      .select("credit_score", "taux_erreur")
      .groupBy(
        when(col("credit_score") < 600, "< 600")
          .when(col("credit_score") < 700, "600-700")
          .when(col("credit_score") < 800, "700-800")
          .otherwise("> 800")
          .alias("tranche_credit")
      )
      .agg(round(mean("taux_erreur"), 2).alias("taux_erreur_moyen"))
      .orderBy("tranche_credit")
      .show()

    println("🕵️ CARTES COMPROMISES (DARK WEB):")
    val cartesDarkWeb = transactionsFull.filter(col("card_on_dark_web") === "Yes")
      .select("card_id")
      .distinct()
      .count()
    
    println(s"   → $cartesDarkWeb cartes présentes sur le dark web effectuent des transactions!")
    
    if (cartesDarkWeb > 0) {
      println("   → Ces cartes doivent être bloquées immédiatement")
      transactionsFull.filter(col("card_on_dark_web") === "Yes")
        .groupBy("card_id", "card_on_dark_web")
        .agg(
          count("*").alias("nb_transactions"),
          round(sum("amount_clean"), 2).alias("montant_total")
        )
        .orderBy(desc("nb_transactions"))
        .show(10)
    }

    println("👤 TOP 10 CLIENTS AVEC LE PLUS HAUT TAUX D'ERREUR:")
    transactionsFull.groupBy("client_id")
      .agg(
        count("*").alias("total_tx"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
      )
      .withColumn("taux_erreur", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
      .filter(col("total_tx") >= 100)
      .orderBy(desc("taux_erreur"))
      .show(10)

    // Retourner les DataFrames enrichis
    (transactionsMCC, transactionsFull)
  }
}