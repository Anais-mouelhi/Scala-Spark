import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Partie4_Fraude {
  
  def execute(spark: SparkSession, transactionsFull: DataFrame): 
    (DataFrame, DataFrame, DataFrame) = {
    
    import spark.implicits._
    
    // =========================================================================
    // PARTIE 4 – APPROCHE FRAUDE (SANS MACHINE LEARNING)
    // =========================================================================
    println("\n" + "=" * 80)
    println("PARTIE 4 – APPROCHE FRAUDE (SANS MACHINE LEARNING)")
    println("=" * 80)

    // -------------------------------------------------------------------------
    // 8. CRÉATION D'INDICATEURS
    // -------------------------------------------------------------------------
    println("\n--- 8. CRÉATION D'INDICATEURS ---\n")
    
    val transactionsJour = transactionsFull
      .withColumn("date_only", to_date(col("timestamp")))

    // INDICATEUR 1: Nombre de transactions par carte et par jour
    val txParCarteJour = transactionsJour.groupBy("card_id", "date_only")
      .agg(count("*").alias("nb_tx_jour"))
    
    println("📌 INDICATEUR 1 - Transactions par carte/jour:")
    txParCarteJour.orderBy(desc("nb_tx_jour")).show(5)

    // INDICATEUR 2: Montant total par carte et par jour
    val montantParCarteJour = transactionsJour.filter(col("amount_clean") > 0)
      .groupBy("card_id", "date_only")
      .agg(round(sum("amount_clean"), 2).alias("montant_total_jour"))
    
    println("📌 INDICATEUR 2 - Montant total par carte/jour:")
    montantParCarteJour.orderBy(desc("montant_total_jour")).show(5)

    // INDICATEUR 3: Nombre de villes différentes par carte
    val villesParCarte = transactionsJour
      .filter(col("merchant_city").isNotNull && col("merchant_city") =!= "ONLINE")
      .groupBy("card_id", "date_only")
      .agg(countDistinct("merchant_city").alias("nb_villes_jour"))
    
    println("📌 INDICATEUR 3 - Villes différentes par carte/jour:")
    villesParCarte.orderBy(desc("nb_villes_jour")).show(5)

    // INDICATEUR 4: Ratio de transactions avec erreur
    val ratioErreurs = transactionsJour.groupBy("card_id")
      .agg(
        count("*").alias("total_tx"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
      )
      .withColumn("ratio_erreurs", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
    
    println("📌 INDICATEUR 4 - Ratio d'erreurs par carte:")
    ratioErreurs.orderBy(desc("ratio_erreurs")).show(5)

    // Indicateurs globaux par carte pour comparaison finale
    val indicators_global = transactionsJour.groupBy("card_id")
      .agg(
        count("*").alias("nb_total_trans"),
        round(sum("amount_clean"), 2).alias("montant_total"),
        countDistinct("merchant_city").alias("nb_villes_total"),
        countDistinct(col("date_only")).alias("nb_jours_actifs")
      )

    // Indicateurs journaliers complets
    val indicators_daily = transactionsJour.groupBy("card_id", "date_only")
      .agg(
        count("*").alias("nb_tx_jour"),
        round(sum("amount_clean"), 2).alias("montant_total_jour"),
        countDistinct("merchant_city").alias("nb_villes_jour"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs"),
        sum(when(col("heure") >= 0 && col("heure") < 6, 1).otherwise(0)).alias("nb_tx_nuit")
      )

    // -------------------------------------------------------------------------
    // 9. DÉTECTION DE COMPORTEMENTS SUSPECTS
    // -------------------------------------------------------------------------
    println("\n--- 9. DÉTECTION DE COMPORTEMENTS SUSPECTS ---\n")
    
    // Définition des seuils
    val SEUIL_TX_JOUR = 10
    val SEUIL_VILLES = 3
    val SEUIL_MONTANT = 1000.0

    println(s"🎯 SEUILS UTILISÉS:")
    println(s"   • Transactions/jour > $SEUIL_TX_JOUR")
    println(s"   • Villes/jour > $SEUIL_VILLES")
    println(s"   • Montant/jour > $SEUIL_MONTANT€")

    // Critère 1: Cartes avec trop de transactions/jour
    val cartesTropTx = txParCarteJour
      .filter(col("nb_tx_jour") > SEUIL_TX_JOUR)
      .select(col("card_id"), lit("nb_tx_eleve").alias("raison"))

    // Critère 2: Cartes dans plus de 3 villes/jour
    val cartesMultiVilles = villesParCarte
      .filter(col("nb_villes_jour") > SEUIL_VILLES)
      .select(col("card_id"), lit("multi_villes").alias("raison"))

    // Critère 3: Cartes avec montant journalier élevé
    val cartesMontantEleve = montantParCarteJour
      .filter(col("montant_total_jour") > SEUIL_MONTANT)
      .select(col("card_id"), lit("montant_eleve").alias("raison"))

    // Critère 4: Cartes sur le dark web
    val cartesDarkWebSuspect = transactionsFull
      .filter(col("card_on_dark_web") === "Yes")
      .select(col("card_id"), lit("dark_web").alias("raison"))
      .distinct()

    // Consolidation des cartes suspectes avec raisons
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

    println(s"\n🚨 NOMBRE DE CARTES SUSPECTES: ${suspiciousCards.count()}")
    println("\n📋 DATAFRAME SUSPICIOUS_CARDS (Top 20):")
    suspiciousCards.show(20, truncate = false)

    // Statistiques sur les cartes suspectes
    println("\n📊 RÉPARTITION DES RAISONS DE SUSPICION:")
    suspiciousCards
      .withColumn("raison", explode(col("raisons_suspicion")))
      .groupBy("raison")
      .agg(count("*").alias("nb_cartes"))
      .orderBy(desc("nb_cartes"))
      .show()

    // Retourner les DataFrames pour le bonus
    (suspiciousCards, indicators_daily, indicators_global)
  }
}