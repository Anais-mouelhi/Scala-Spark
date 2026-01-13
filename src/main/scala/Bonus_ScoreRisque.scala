import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Bonus_Score {
  
  def execute(spark: SparkSession, transactionsFull: DataFrame, indicators_daily: DataFrame,
              suspiciousCards: DataFrame, indicators_global: DataFrame, basePath: String): Unit = {
    
    import spark.implicits._
    
    // =========================================================================
    // BONUS – SCORE DE RISQUE ET EXPORT PARQUET
    // =========================================================================
    println("\n" + "=" * 80)
    println("BONUS – SCORE DE RISQUE ET EXPORT")
    println("=" * 80)

    println("\n--- CRÉATION D'UN SCORE DE RISQUE SIMPLE ---\n")

    println("🎯 FORMULE DU SCORE:")
    println("   score = (nb_tx_jour × 2) + (nb_villes × 5) + (nb_erreurs × 10)")
    println("         + (nb_tx_nuit × 3) + (montant_jour > 1000€ ? 15 : 0)")
    println("\n💡 JUSTIFICATION DES POIDS:")
    println("   • nb_tx_jour (×2)   : Impact modéré (beaucoup de tx peut être normal)")
    println("   • nb_villes (×5)    : Impact élevé (multi-villes = anormal)")
    println("   • nb_erreurs (×10)  : Impact très élevé (erreurs = fraude potentielle)")
    println("   • nb_tx_nuit (×3)   : Impact moyen (nuit = suspect mais pas toujours)")
    println("   • montant > 1000€ (+15) : Bonus pour gros montants (risque financier)")

    val riskScore = indicators_daily
      .withColumn("score_risque", 
        (col("nb_tx_jour") * 2) +
        (col("nb_villes_jour") * 5) +
        (col("nb_erreurs") * 10) +
        (col("nb_tx_nuit") * 3) +
        when(col("montant_total_jour") > 1000, 15).otherwise(0)
      )
      .orderBy(desc("score_risque"))

    println("\n🏆 Top 20 cartes avec score de risque le plus élevé:")
    riskScore.show(20)

    // Distribution des scores
    println("📊 DISTRIBUTION DES SCORES DE RISQUE:")
    riskScore
      .withColumn("tranche_score",
        when(col("score_risque") < 10, "Faible (< 10)")
          .when(col("score_risque") < 30, "Moyen (10-30)")
          .when(col("score_risque") < 50, "Élevé (30-50)")
          .otherwise("Critique (> 50)")
      )
      .groupBy("tranche_score")
      .agg(count("*").alias("nb_cartes_jour"))
      .orderBy(
        when(col("tranche_score") === "Faible (< 10)", 1)
          .when(col("tranche_score") === "Moyen (10-30)", 2)
          .when(col("tranche_score") === "Élevé (30-50)", 3)
          .otherwise(4)
      )
      .show()

    println("\n💡 INTERPRÉTATION DES SCORES:")
    println("   • 0-10   : Risque faible → RAS")
    println("   • 10-30  : Risque moyen → Surveillance")
    println("   • 30-50  : Risque élevé → Alerte analyste")
    println("   • > 50   : Risque critique → Blocage immédiat")

    // Export en Parquet
    println("\n💾 SAUVEGARDE DES RÉSULTATS EN FORMAT PARQUET:")
    
    riskScore.write.mode("overwrite").parquet(basePath + "output/risk_scores.parquet")
    println("   ✅ risk_scores.parquet")
    
    suspiciousCards.write.mode("overwrite").parquet(basePath + "output/suspicious_cards.parquet")
    println("   ✅ suspicious_cards.parquet")
    
    transactionsFull.write.mode("overwrite").parquet(basePath + "output/transactions_enriched.parquet")
    println("   ✅ transactions_enriched.parquet")

    println("\n📁 Fichiers sauvegardés dans le dossier: ./output/")

    // Comparaison clients normaux vs suspects
    println("\n--- COMPARAISON CLIENTS NORMAUX vs SUSPECTS ---\n")

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
        round(mean("current_age"), 2).alias("age_moyen"),
        round(mean("yearly_income"), 2).alias("revenu_moyen")
      )
      .withColumn("type_client", lit("Suspect"))

    val statsClientsNormaux = transactionsFull
      .join(clientsSuspects, Seq("client_id"), "left_anti")
      .agg(
        count("*").alias("nb_tx"),
        round(mean("amount_clean"), 2).alias("montant_moyen"),
        round(mean("credit_score"), 2).alias("credit_score_moyen"),
        round(mean("current_age"), 2).alias("age_moyen"),
        round(mean("yearly_income"), 2).alias("revenu_moyen")
      )
      .withColumn("type_client", lit("Normal"))

    println("📊 COMPARAISON DES PROFILS:")
    statsClientsSuspects.union(statsClientsNormaux).show()

    println("\n💡 INSIGHTS:")
    println("   → Les clients suspects ont généralement :")
    println("     • Plus de transactions")
    println("     • Montants moyens plus élevés")
    println("     • Credit score potentiellement plus faible")
    println("     • Profil démographique différent")

    println("\n" + "=" * 80)
    println("✅ FIN DE L'ANALYSE - TP SCALA & SPARK COMPLÉTÉ")
    println("=" * 80)
    println("\n📝 Fichiers générés:")
    println("   • risk_scores.parquet")
    println("   • suspicious_cards.parquet")
    println("   • transactions_enriched.parquet")

  }
}
