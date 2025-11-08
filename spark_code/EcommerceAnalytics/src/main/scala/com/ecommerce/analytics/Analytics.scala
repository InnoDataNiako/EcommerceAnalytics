package com.ecommerce.analytics

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.ecommerce.models._
import org.apache.spark.storage.StorageLevel

class Analytics(spark: SparkSession) {
  import spark.implicits._
  def generateMerchantReport(transactions: Dataset[Transaction],
                             merchants: Dataset[Merchant],
                             users: Dataset[User]): DataFrame = {

    println("Génération du rapport par marchand avec optimisations...")

    // Préparer les DataFrames avec des noms de colonnes explicites
    val transactionsDF = transactions.toDF().select(
      col("transaction_id"),
      col("user_id"),
      col("merchant_id"),
      col("amount")
    )

    val merchantsDF = merchants.toDF().select(
      col("merchant_id").as("merchant_id_m"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region").as("merchant_region"),
      col("commission_rate")
    )

    val usersDF = users.toDF().select(
      col("user_id").as("user_id_u"),
      col("age")
    )

    // OPTIMISATION 5.2 - Broadcast des petites tables
    import org.apache.spark.sql.functions.broadcast
    val broadcastMerchants = broadcast(merchantsDF.cache())
    val broadcastUsers = broadcast(usersDF.cache())

    // Jointure optimisée avec broadcast et colonnes explicites
    val txWithMerchants = transactionsDF
      .join(broadcastMerchants, col("merchant_id") === col("merchant_id_m"), "left")
      .drop("merchant_id_m") // Supprimer la colonne dupliquée
      .join(broadcastUsers, col("user_id") === col("user_id_u"), "left")
      .drop("user_id_u") // Supprimer la colonne dupliquée
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    try {
      // Calcul des KPI de base par marchand avec colonnes explicites
      val merchantKPIs = txWithMerchants
        .groupBy(col("merchant_id"), col("merchant_name"), col("merchant_category"), col("merchant_region"))
        .agg(
          sum("amount").as("chiffre_affaires_total"),
          count("transaction_id").as("nombre_transactions"),
          countDistinct("user_id").as("clients_uniques"),
          when(count("transaction_id") > 0, avg("amount")).otherwise(0.0).as("montant_moyen_transaction"),
          sum(col("amount") * col("commission_rate")).as("commission_totale")
        )
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      try {
        // Calcul du classement par catégorie et région
        val categoryWindow = Window.partitionBy("merchant_category").orderBy(desc("chiffre_affaires_total"))
        val regionWindow = Window.partitionBy("merchant_region").orderBy(desc("chiffre_affaires_total"))

        val rankedMerchants = merchantKPIs
          .withColumn("rang_categorie", rank().over(categoryWindow))
          .withColumn("rang_region", rank().over(regionWindow))

        // Répartition des ventes par tranche d'âge
        val ageDistribution = txWithMerchants
          .withColumn("tranche_age",
            when(col("age") < 25, "Jeune")
              .when(col("age").between(25, 44), "Adulte")
              .when(col("age").between(45, 64), "Age Moyen")
              .otherwise("Senior"))
          .groupBy(col("merchant_id"), col("tranche_age"))
          .agg(sum("amount").as("ca_tranche_age"))
          .groupBy("merchant_id")
          .pivot("tranche_age")
          .agg(first("ca_tranche_age"))
          .na.fill(0.0)

        // Jointure finale de tous les KPIs
        val finalReport = rankedMerchants
          .join(ageDistribution, Seq("merchant_id"), "left")
          .select(
            col("merchant_id"),
            col("merchant_name").as("nom_marchand"),
            col("merchant_category").as("categorie"),
            col("merchant_region").as("region"),
            col("chiffre_affaires_total"),
            col("nombre_transactions"),
            col("clients_uniques"),
            col("montant_moyen_transaction"),
            col("commission_totale"),
            col("rang_categorie"),
            col("rang_region"),
            coalesce(col("Jeune"), lit(0.0)).as("ca_jeune"),
            coalesce(col("Adulte"), lit(0.0)).as("ca_adulte"),
            coalesce(col("Age Moyen"), lit(0.0)).as("ca_age_moyen"),
            coalesce(col("Senior"), lit(0.0)).as("ca_senior")
          )
          .orderBy(desc("chiffre_affaires_total"))

        println("Rapport par marchand généré avec succès avec optimisations!")
        finalReport

      } finally {
        merchantKPIs.unpersist()
      }

    } finally {
      txWithMerchants.unpersist()
      broadcastMerchants.unpersist()
      broadcastUsers.unpersist()
    }
  }

  /**
   * Question 4.2 - Analyse de cohortes avec optimisations
   */
  def analyzeUserCohorts(transactions: Dataset[Transaction],
                         users: Dataset[User]): DataFrame = {

    println("Analyse des cohortes utilisateurs avec optimisations...")

    // Préparation des données avec colonnes explicites
    val transactionsDF = transactions.toDF().select(
      col("user_id"),
      col("timestamp")
    )

    val firstTransactionDate = transactionsDF
      .withColumn("transaction_date", to_date(col("timestamp"), "yyyyMMddHHmmss"))
      .groupBy("user_id")
      .agg(min("transaction_date").as("first_transaction_date"))
      .withColumn("cohort_month", date_format(col("first_transaction_date"), "yyyy-MM"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    try {
      val cohortSize = firstTransactionDate
        .groupBy("cohort_month")
        .agg(count("user_id").as("cohort_size"))
        .cache()

      try {
        val userMonthlyActivity = transactionsDF
          .withColumn("transaction_date", to_date(col("timestamp"), "yyyyMMddHHmmss"))
          .withColumn("transaction_month", date_format(col("transaction_date"), "yyyy-MM"))
          .join(firstTransactionDate, Seq("user_id"))
          .filter(col("transaction_month") >= col("cohort_month"))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        try {
          val cohortAnalysis = userMonthlyActivity
            .groupBy("cohort_month", "transaction_month")
            .agg(countDistinct("user_id").as("utilisateurs_actifs"))
            .withColumn("mois_apres_cohorte",
              months_between(to_date(col("transaction_month"), "yyyy-MM"),
                to_date(col("cohort_month"), "yyyy-MM")))

          val retentionWithRate = cohortAnalysis
            .join(cohortSize, Seq("cohort_month"))
            .withColumn("taux_retention", round(col("utilisateurs_actifs") / col("cohort_size") * 100, 2))

          val retentionMatrix = retentionWithRate
            .groupBy("cohort_month", "cohort_size")
            .pivot("mois_apres_cohorte")
            .agg(first("taux_retention"))
            .na.fill(0.0)
            .orderBy("cohort_month")

          println("Analyse de cohortes terminée avec optimisations!")
          retentionMatrix

        } finally {
          userMonthlyActivity.unpersist()
        }

      } finally {
        cohortSize.unpersist()
      }

    } finally {
      firstTransactionDate.unpersist()
    }
  }

  /**
   * Top produits avec optimisations
   */
  def topProductsByCategory(products: Dataset[Product],
                            transactions: Dataset[Transaction]): DataFrame = {

    println("Calcul des top produits par catégorie avec optimisations...")

    // Préparation explicite des DataFrames
    val productsDF = products.toDF().select(
      col("product_id").as("product_id_p"),
      col("name").as("product_name"),
      col("category").as("product_category"), // Renommage explicite
      col("price"),
      col("rating")
    )

    val transactionsDF = transactions.toDF().select(
      col("product_id"),
      col("transaction_id"),
      col("amount")
    )

    // OPTIMISATION 5.2 - Broadcast pour les petites tables
    val broadcastProducts = broadcast(productsDF.cache())

    try {
      val productSales = transactionsDF
        .groupBy("product_id")
        .agg(
          sum("amount").as("chiffre_affaires"),
          count("transaction_id").as("nombre_ventes")
        )
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      try {
        val topProducts = broadcastProducts
          .join(productSales, col("product_id_p") === col("product_id"), "left")
          .drop("product_id") // Gardons seulement product_id_p
          .na.fill(0.0, Seq("chiffre_affaires", "nombre_ventes"))
          .withColumn("rang_categorie",
            rank().over(Window.partitionBy("product_category").orderBy(desc("chiffre_affaires"))))
          .filter(col("rang_categorie") <= 10)
          .select(
            col("product_id_p").as("product_id"),
            col("product_name").as("nom_produit"),
            col("product_category").as("categorie"),
            col("price").as("prix"),
            col("rating").as("note"),
            col("chiffre_affaires"),
            col("nombre_ventes"),
            col("rang_categorie")
          )
          .orderBy("categorie", "rang_categorie")

        println("Top produits calculés avec optimisations!")
        topProducts

      } finally {
        productSales.unpersist()
      }

    } finally {
      broadcastProducts.unpersist()
    }
  }

  /**
   *Méthode CLV corrigée avec colonnes explicites
   */
  def calculateCustomerLifetimeValue(
                                      transactions: Dataset[Transaction],
                                      users: Dataset[User]
                                    ): DataFrame = {

    import spark.implicits._

    // Préparation des DataFrames avec colonnes explicites
    val transactionsDF = transactions.toDF().select(
      col("user_id"),
      col("transaction_id"),
      col("amount"),
      col("timestamp")
    )

    val usersDF = users.toDF().select(
      col("user_id").as("user_id_u"),
      col("registration_date"),
      col("city")
    )

    val customerValue = transactionsDF
      .withColumn("transaction_date", to_date(col("timestamp"), "yyyyMMddHHmmss"))
      .groupBy(col("user_id"))
      .agg(
        sum(col("amount")).as("total_spent"),
        count(col("transaction_id")).as("transaction_count"),
        avg(col("amount")).as("avg_transaction_value"),
        max(col("transaction_date")).as("last_transaction_date")
      )
      .withColumn("days_since_last_purchase",
        datediff(current_date(), col("last_transaction_date")))

    // Jointure avec les utilisateurs
    customerValue
      .join(usersDF, col("user_id") === col("user_id_u"), "left")
      .drop("user_id_u")
      .select(
        col("user_id"),
        col("total_spent"),
        col("transaction_count"),
        col("avg_transaction_value"),
        col("days_since_last_purchase"),
        col("registration_date"),
        col("city")
      )
  }
}