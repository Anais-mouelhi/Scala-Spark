name := "FraudAnalysis"
version := "1.0"
scalaVersion := "2.12.18"

// Dépendances Apache Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

// Configuration pour éviter les conflits de dépendances
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2"
)

// Options de compilation
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)

// Fork pour l'exécution (nécessaire pour Spark)
fork := true

// Options JVM pour Spark
javaOptions ++= Seq(
  "-Xmx4g",
  "-XX:+UseG1GC"
)

