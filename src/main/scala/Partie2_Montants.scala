import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Partie2_Montants {
  
  def execute(spark: SparkSession, transactionsClean: DataFrame): (DataFrame, DataFrame) = {
    
    import spark.implicits._
    
    // =========================================================================
    // PARTIE 2 – ANALYSE DES MONTANTS & COMPORTEMENTS
    // =========================================================================
    println("\n" + "=" * 80)
    println("PARTIE 2 – ANALYSE DES MONTANTS & COMPORTEMENTS")
    println("=" * 80)

    // -------------------------------------------------------------------------
    // 4. ANALYSE DES MONTANTS
    // -------------------------------------------------------------------------
    println("\n--- 4. ANALYSE DES MONTANTS ---\n")
    
    val statsPositifs = transactionsClean.filter(col("amount_clean") > 0)

    println("📈 STATISTIQUES DESCRIPTIVES:")
    statsPositifs.agg(
      round(mean("amount_clean"), 2).alias("Moyenne"),
      round(expr("percentile_approx(amount_clean, 0.5)"), 2).alias("Médiane"),
      round(min("amount_clean"), 2).alias("Min"),
      round(max("amount_clean"), 2).alias("Max")
    ).show()

    println("📊 DISTRIBUTION PAR TRANCHES:")
    val distributionTranches = statsPositifs
      .withColumn("tranche",
        when(col("amount_clean") < 10, "< 10 €")
          .when(col("amount_clean") < 50, "10-50 €")
          .when(col("amount_clean") < 200, "50-200 €")
          .otherwise("> 200 €"))
      .groupBy("tranche")
      .agg(
        count("*").alias("nb_transactions"),
        round(count("*") * 100.0 / statsPositifs.count(), 2).alias("pourcentage")
      )
      .orderBy(
        when(col("tranche") === "< 10 €", 1)
          .when(col("tranche") === "10-50 €", 2)
          .when(col("tranche") === "50-200 €", 3)
          .otherwise(4)
      )

    distributionTranches.show()

    val txSup200 = statsPositifs.filter(col("amount_clean") > 200).count()
    val pctSup200 = (txSup200 * 100.0 / statsPositifs.count())
    
    println(s"💡 QUESTION MÉTIER: Les montants élevés sont-ils rares ou fréquents?")
    println(f"   → Les montants > 200€ sont RARES: $txSup200 transactions ($pctSup200%.2f%%)")
    println(f"   → Cela représente un risque financier limité en volume mais potentiellement élevé en impact")

    // -------------------------------------------------------------------------
    // 5. ANALYSE TEMPORELLE
    // -------------------------------------------------------------------------
    println("\n--- 5. ANALYSE TEMPORELLE ---\n")
    
    val transactionsTime = transactionsClean
      .withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("heure", hour(col("timestamp")))
      .withColumn("jour_semaine", dayofweek(col("timestamp")))
      .withColumn("mois", month(col("timestamp")))

    println("⏰ TRANSACTIONS PAR HEURE:")
    transactionsTime.groupBy("heure")
      .agg(count("*").alias("nb_transactions"))
      .orderBy("heure")
      .show(24)

    println("📅 TRANSACTIONS PAR JOUR DE LA SEMAINE:")
    println("(1=Dimanche, 2=Lundi, ..., 7=Samedi)")
    transactionsTime.groupBy("jour_semaine")
      .agg(count("*").alias("nb_transactions"))
      .orderBy("jour_semaine")
      .show()

    val totalTx = transactionsTime.count()
    val txNuit = transactionsTime.filter(col("heure") >= 0 && col("heure") < 6).count()
    val pctNuit = (txNuit * 100.0 / totalTx)
    
    println(s"💡 INTERPRÉTATION: Existe-t-il des heures anormalement actives?")
    println(f"   → Heures nocturnes (00h-06h): $txNuit transactions ($pctNuit%.2f%%)")
    println(f"   → Ces heures peuvent indiquer une activité suspecte (fraude ou utilisation inhabituelle)")

    // Retourner les DataFrames enrichis pour les parties suivantes
    (transactionsTime, statsPositifs)
  }
}