package com.ecommerce.analytics

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
// Ajoutez ces imports
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object MainApp {
  def main(args: Array[String]): Unit = {
    // Configuration Windows pour éviter les erreurs Hadoop/winutils
    System.setProperty("hadoop.home.dir", "C:\\spark\\spark-3.3.0-bin-hadoop2")
    System.setProperty("hadoop.tmp.dir", "C:\\tmp")
    System.setProperty("java.io.tmpdir", "C:\\tmp")

    // Réduction du niveau de log pour une sortie plus claire
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("=== Démarrage de l'application EcommerceAnalytics ===")

    var spark: SparkSession = null
    var products: org.apache.spark.sql.Dataset[com.ecommerce.models.Product] = null
    var merchants: org.apache.spark.sql.Dataset[com.ecommerce.models.Merchant] = null
    var hasProducts = false
    var hasMerchants = false

    try {
      // 1) Création de la SparkSession
      println("1. Création de SparkSession...")
      spark = SparkSession.builder()
        .appName("EcommerceAnalytics")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

      // Configuration Hadoop
      val hadoopConfig = spark.sparkContext.hadoopConfiguration
      hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      hadoopConfig.set("dfs.replication", "1")
      hadoopConfig.set("parquet.enable.summary-metadata", "false")
      println("SparkSession créée avec succès")

      // 2) Chargement de la configuration
      println("2. Chargement de la configuration...")
      val config = ConfigFactory.load()
      println("Configuration chargée avec succès")

      // 3) Instanciation de la classe DataIngestion
      println("3. Création de l'objet DataIngestion...")
      val ingestion = new DataIngestion(spark, config)
      println("DataIngestion créé avec succès")

      // 4) Lecture des données
      println("\n=== Lecture des données de base ===")

      println("4a. Lecture des transactions...")
      val txRaw = ingestion.readTransactions()
      val txCount = txRaw.count()
      println(s" Transactions brutes chargées: $txCount lignes")

      println("4b. Lecture des utilisateurs...")
      val usersRaw = ingestion.readUsers()
      val usersCount = usersRaw.count()
      println(s" Utilisateurs bruts chargés: $usersCount lignes")

      // 5) Lecture des produits et marchands
      println("\n=== Lecture des données optionnelles ===")

      println("4c. Tentative de lecture des produits...")
      try {
        val productsRaw = ingestion.readProducts()
        val productsCount = productsRaw.count()
        println(s" Produits bruts chargés: $productsCount lignes")
        products = ingestion.validateProducts(productsRaw)
        hasProducts = true
      } catch {
        case e: Exception =>
          println(s" Impossibilité de lire les produits: ${e.getMessage}")
          hasProducts = false
      }

      println("4d. Tentative de lecture des marchands...")
      try {
        val merchantsRaw = ingestion.readMerchants()
        val merchantsCount = merchantsRaw.count()
        println(s" Marchands bruts chargés: $merchantsCount lignes")
        merchants = ingestion.validateMerchants(merchantsRaw)
        hasMerchants = true
      } catch {
        case e: Exception =>
          println(s" Impossibilité de lire les marchands: ${e.getMessage}")
          hasMerchants = false
      }

      // 6) Validation des données disponibles
      println("\n=== Validation des données disponibles ===")

      println("6a. Validation des transactions...")
      val tx = ingestion.validateTransactions(txRaw)
      val validTxCount = tx.count()
      println(s" Transactions valides: validTxCount lignes")

      println("6b. Validation des utilisateurs...")
      val users = ingestion.validateUsers(usersRaw)
      val validUsersCount = users.count()
      println(s" Utilisateurs valides: validUsersCount lignes")

      // 7) Optimisation: Mise en cache des données fréquemment utilisées
      println("\n=== Optimisation: Mise en cache des données ===")
      tx.persist(StorageLevel.MEMORY_AND_DISK_SER)
      users.persist(StorageLevel.MEMORY_AND_DISK_SER)
      if (hasProducts) products.persist(StorageLevel.MEMORY_AND_DISK_SER)
      if (hasMerchants) merchants.persist(StorageLevel.MEMORY_AND_DISK_SER)

      println(s"Transactions en cache: ${tx.count()} lignes")
      println(s"Utilisateurs en cache: ${users.count()} lignes")
      if (hasProducts) println(s"Produits en cache: ${products.count()} lignes")
      if (hasMerchants) println(s"Marchands en cache: ${merchants.count()} lignes")

      // 8) TRANSFORMATIONS DE DONNÉES
      println("\n=== Transformations de données ===")
      val transformation = new DataTransformation(spark)

      if (hasProducts && hasMerchants) {
        println("Enrichissement des données de transaction...")
        val enrichedDF = transformation.enrichTransactionData(tx, users, products, merchants)
        println(s" Données enrichies: ${enrichedDF.count()} lignes")

        println("Ajout des analyses de fenêtrage...")
        val finalDF = transformation.addWindowAnalytics(enrichedDF)
        println(s" Analytics ajoutés: ${finalDF.count()} lignes")

        println("Aperçu des données transformées (5 premières lignes):")
        finalDF.show(5, truncate = false)

        // Sauvegarde des données transformées
        println("Sauvegarde des données transformées...")
        finalDF.write.mode("overwrite").parquet("output/enriched_transactions")
        println(" Données transformées sauvegardées en Parquet")

        // Aperçu final
        println("Aperçu final des données transformées (3 premières lignes):")
        finalDF.show(3, truncate = false)

      } else {
        println(" Transformations complètes ignorées: données manquantes")
        if (!hasProducts) println("  - Données produits manquantes")
        if (!hasMerchants) println("  - Données marchands manquantes")
      }

      // 9) ANALYTIQUE BUSINESS
      println("\n=== ANALYTIQUE BUSINESS ===")
      val analytics = new Analytics(spark)

      // Vérification que les données nécessaires sont disponibles pour chaque rapport
      if (hasMerchants) {
        println("9.1. Génération du rapport par marchand...")
        val merchantReport = analytics.generateMerchantReport(tx, merchants, users)
        println("Rapport marchand (10 premiers):")
        merchantReport.show(10, truncate = false)

        // Sauvegarde multiple formats
        merchantReport.write.mode("overwrite").option("header", "true").csv("output/merchant_report")
        merchantReport.write.mode("overwrite").parquet("output/merchant_report_parquet")
        println(" Rapport marchand sauvegardé (CSV + Parquet)")
      } else {
        println(" Rapport marchand ignoré: données marchands manquantes")
      }

      println("9.2. Analyse de cohortes utilisateurs...")
      val cohortAnalysis = analytics.analyzeUserCohorts(tx, users)
      println("Matrice de rétention par cohorte:")
      cohortAnalysis.show(20, truncate = false)

      // Sauvegarde multiple formats
      cohortAnalysis.write.mode("overwrite").option("header", "true").csv("output/cohort_analysis")
      cohortAnalysis.write.mode("overwrite").parquet("output/cohort_analysis_parquet")
      println("✓ Analyse de cohortes sauvegardée (CSV + Parquet)")

      if (hasProducts) {
        println("9.3. Calcul des top produits par catégorie...")
        val topProducts = analytics.topProductsByCategory(products, tx)
        println("Top produits par catégorie:")
        topProducts.show(20, truncate = false)

        // Sauvegarde multiple formats
        topProducts.write.mode("overwrite").option("header", "true").csv("output/top_products")
        topProducts.write.mode("overwrite").parquet("output/top_products_parquet")
        println(" Top produits sauvegardé (CSV + Parquet)")
      } else {
        println(" Top produits ignoré: données produits manquantes")
      }

      // 10) RAPPORTS SUPPLÉMENTAIRES (uniquement les méthodes existantes)
      println("\n=== RAPPORTS SUPPLÉMENTAIRES ===")

      println("10.1. Analyse de la valeur client (CLV)...")
      val clvAnalysis = analytics.calculateCustomerLifetimeValue(tx, users)
      println("Valeur client (10 premiers):")
      clvAnalysis.show(10, truncate = false)

      clvAnalysis.write.mode("overwrite").option("header", "true").csv("output/clv_analysis")
      println(" Analyse CLV sauvegardée")

      // 11) RÉSUMÉ FINAL DÉTAILLÉ
      println("\n" + "="*50)
      println("=== RÉSUMÉ FINAL ===")
      println("="*50)

      println(" DONNÉES TRAITÉES:")
      println(s"  Transactions: $validTxCount lignes")
      println(s"  Utilisateurs: $validUsersCount lignes")
      if (hasProducts) println(s"  Produits: ${products.count()} lignes") else println("  Produits: non disponibles")
      if (hasMerchants) println(s"  Marchands: ${merchants.count()} lignes") else println("  Marchands: non disponibles")

      println("\n RAPPORTS GÉNÉRÉS:")
      if (hasMerchants) println("  Rapport marchand") else println("  Rapport marchand (données manquantes)")
      println("  Analyse de cohortes")
      if (hasProducts) println("  Top produits par catégorie") else println("  Top produits (données manquantes)")
      println("  Analyse valeur client (CLV)")

      println("\n DONNÉES SAUVEGARDÉES:")
      if (hasProducts && hasMerchants) println("  Transactions enrichies (Parquet)") else println("  Transactions enrichies (données manquantes)")
      println("  Rapports analytiques (CSV + Parquet)")

      println("\n  PERFORMANCE:")
      println("  Données mises en cache")
      println("  Optimisations Spark activées")
      println("  Nettoyage mémoire configuré")

      println("\n" + "="*50)
      println("=== APPLICATION TERMINÉE AVEC SUCCÈS ===")
      println("="*50)

    } catch {
      case e: Exception =>
        println(s"\n ERREUR CRITIQUE: ${e.getMessage}")
        println("Stack trace détaillée:")
        e.printStackTrace()

        // Tentative de sauvegarde des erreurs pour debug
        try {
          if (spark != null) {
            spark.sparkContext.parallelize(Seq(s"Erreur: ${e.getMessage}"))
              .saveAsTextFile("output/errors")
          }
        } catch {
          case _: Exception => // Ignorer les erreurs de sauvegarde
        }

        System.exit(1)
    } finally {
      if (spark != null) {
        println("\n Nettoyage et fermeture de Spark...")
        try {
          // Libération explicite de la mémoire
          spark.catalog.clearCache()

          spark.stop()
          println(" Spark fermé avec succès")
        } catch {
          case e: Exception =>
            println(s" Erreur lors de la fermeture: ${e.getMessage}")
        }
      }
    }
  }
}