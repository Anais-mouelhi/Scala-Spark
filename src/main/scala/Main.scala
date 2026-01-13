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
 * MAIN - ORCHESTRATION DE TOUTES LES PARTIES DU TP
 * ============================================================================
 * 
 * Ce fichier appelle toutes les parties du TP dans l'ordre :
 *   1. Partie1_EDA        - Chargement et qualité des données
 *   2. Partie2_Montants   - Analyse des montants et temporelle
 *   3. Partie3_Enrichissement - Jointures MCC, cards, users
 *   4. Partie4_Fraude     - Indicateurs et détection suspects
 *   5. Partie5_Synthese   - Synthèse finale
 *   6. Bonus_Score        - Score de risque et export Parquet
 * 
 * Exécution: scala-cli run src/main/scala/
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {
  def main(args: Array[String]): Unit = {
    // =========================================================================
    // INITIALISATION SPARK
    // =========================================================================
    val spark = SparkSession.builder()
      .appName("TP Scala Spark - Analyse de Fraude Bancaire")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val basePath = "./"

    println("\n" + "=" * 80)
    println("TP SCALA & SPARK - ANALYSE EXPLORATOIRE ET DÉTECTION DE FRAUDE")
    println("=" * 80)

    // =========================================================================
    // PARTIE 1 - CHARGEMENT DES DONNÉES (EDA)
    // =========================================================================
    val result1: (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = 
      Partie1_EDA.execute(spark, basePath)
    val (transactionsDF, cardsDF, usersDF, mccCodesDF, transactionsClean) = result1

    // =========================================================================
    // PARTIE 2 - ANALYSE DES MONTANTS & COMPORTEMENTS
    // =========================================================================
    val result2: (DataFrame, DataFrame) = 
      Partie2_Montants.execute(spark, transactionsClean)
    val (transactionsTime, statsPositifs) = result2

    // =========================================================================
    // PARTIE 3 - ENRICHISSEMENT MÉTIER (MCC, ERREURS, USERS, CARDS)
    // =========================================================================
    val result3: (DataFrame, DataFrame) = 
      Partie3_Enrichissement.execute(spark, transactionsTime, mccCodesDF, cardsDF, usersDF)
    val (transactionsMCC, transactionsFull) = result3

    // =========================================================================
    // PARTIE 4 - APPROCHE FRAUDE (SANS ML)
    // =========================================================================
    val result4: (DataFrame, DataFrame, DataFrame) = 
      Partie4_Fraude.execute(spark, transactionsFull)
    val (suspiciousCards, indicatorsDaily, indicatorsGlobal) = result4

    // =========================================================================
    // PARTIE 5 - SYNTHÈSE FINALE
    // =========================================================================
    Partie5_Synthese.execute(spark)

    // =========================================================================
    // BONUS - SCORE DE RISQUE ET EXPORT
    // =========================================================================
    Bonus_Score.execute(spark, transactionsFull, indicatorsDaily, suspiciousCards, indicatorsGlobal, basePath)

    // =========================================================================
    // FIN
    // =========================================================================
    spark.stop()
  }
}
