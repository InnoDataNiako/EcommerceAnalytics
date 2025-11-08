// src/main/scala/com/ecommerce/analytics/DataTransformation.scala
package com.ecommerce.analytics

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import com.ecommerce.models.{Transaction, User, Product, Merchant}

// Case class pour représenter la structure de retour de la UDF
case class TimeFeatures(
                         hour: Int,
                         day_of_week: String,
                         month: String,
                         is_weekend: Int,
                         day_period: String,
                         is_working_hours: Int
                       )

class DataTransformation(spark: SparkSession) {

  private val extractTimeFeaturesUdf: UserDefinedFunction = udf((timestampStr: String) => {
    if (timestampStr == null || timestampStr.length != 14) {
      TimeFeatures(
        hour = -1,
        day_of_week = "Invalid",
        month = "Invalid",
        is_weekend = 0,
        day_period = "Invalid",
        is_working_hours = 0
      )
    } else {
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      val dt = LocalDateTime.parse(timestampStr, formatter)

      val hour = dt.getHour
      val dayOfWeek = dt.getDayOfWeek.toString.toLowerCase.capitalize
      val month = dt.getMonth.toString.toLowerCase.capitalize

      val isWeekend = if (dt.getDayOfWeek.getValue >= 6) 1 else 0 // 6=Saturday, 7=Sunday

      val dayPeriod = hour match {
        case h if h >= 6 && h < 12 => "Morning"
        case h if h >= 12 && h < 18 => "Afternoon"
        case h if h >= 18 && h < 22 => "Evening"
        case _ => "Night"
      }

      val isWorkingHours = if (hour >= 9 && hour < 17) 1 else 0

      TimeFeatures(
        hour = hour,
        day_of_week = dayOfWeek,
        month = month,
        is_weekend = isWeekend,
        day_period = dayPeriod,
        is_working_hours = isWorkingHours
      )
    }
  })

  /**
   * Enrichit les données de transaction en joignant les autres tables et en ajoutant de nouvelles features.
   */
  def enrichTransactionData(
                             transactions: Dataset[Transaction],
                             users: Dataset[User],
                             products: Dataset[Product],
                             merchants: Dataset[Merchant]): DataFrame = {
    import spark.implicits._

    // 1. Préparation des DataFrames avec sélection explicite de toutes les colonnes nécessaires
    // Transactions - on garde category de transactions comme tx_category
    val transactionsDF = transactions.toDF().select(
      col("transaction_id"),
      col("user_id").as("main_user_id"),
      col("product_id").as("main_product_id"),
      col("merchant_id").as("main_merchant_id"),
      col("amount"),
      col("timestamp"),
      col("location"),
      col("payment_method"),
      col("category").as("tx_category") // Renommer pour éviter l'ambiguïté
    )

    // Users - sélection explicite
    val usersDF = users.toDF().select(
      col("user_id").as("user_id_users"),
      col("age"),
      col("annual_income"),
      col("city"),
      col("customer_segment"),
      col("preferred_categories"),
      col("registration_date")
    )

    // Products - sélection explicite avec renommage de category
    val productsDF = products.toDF().select(
      col("product_id").as("product_id_products"),
      col("name").as("product_name"),
      col("category").as("product_category"), // Renommer explicitement
      col("price"),
      col("merchant_id").as("product_merchant_id"),
      col("rating"),
      col("stock")
    )

    // Merchants - sélection explicite avec renommage de category et name
    val merchantsDF = merchants.toDF().select(
      col("merchant_id").as("merchant_id_merchants"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"), // Renommer explicitement
      col("region"),
      col("commission_rate"),
      col("establishment_date")
    )

    // 2. Jointures séquentielles avec suppression immédiate des colonnes de jointure
    val withUsers = transactionsDF
      .join(usersDF, col("main_user_id") === col("user_id_users"), "left")
      .drop("user_id_users") // Supprimer immédiatement

    val withProducts = withUsers
      .join(productsDF, col("main_product_id") === col("product_id_products"), "left")
      .drop("product_id_products", "product_merchant_id") // Supprimer les colonnes non nécessaires

    val enrichedDF = withProducts
      .join(merchantsDF, col("main_merchant_id") === col("merchant_id_merchants"), "left")
      .drop("merchant_id_merchants") // Supprimer la colonne de jointure

    // 3. Application de l'UDF pour extraire les features temporelles
    val withTimeFeaturesDF = enrichedDF
      .withColumn("time_features", extractTimeFeaturesUdf(col("timestamp")))
      .select(
        col("*"),
        col("time_features.hour").as("hour"),
        col("time_features.day_of_week").as("day_of_week"),
        col("time_features.month").as("month"),
        col("time_features.is_weekend").as("is_weekend"),
        col("time_features.day_period").as("day_period"),
        col("time_features.is_working_hours").as("is_working_hours")
      )
      .drop("time_features")

    // 4. Ajout de la tranche d'âge
    val withAgeGroupDF = withTimeFeaturesDF
      .withColumn("age_group",
        when(col("age") < 25, "Jeune")
          .when(col("age").between(25, 44), "Adulte")
          .when(col("age").between(45, 64), "Age Moyen")
          .otherwise("Senior")
      )

    // 5. Window Functions avec noms de colonnes explicites
    import org.apache.spark.sql.expressions.Window

    val userWindow = Window.partitionBy("main_user_id").orderBy("timestamp")
    val userCountWindow = Window.partitionBy("main_user_id")

    val finalEnrichedDF = withAgeGroupDF
      .withColumn("transaction_rank", row_number().over(userWindow))
      .withColumn("total_transactions_per_user", count("transaction_id").over(userCountWindow))

    finalEnrichedDF.select(
      // Colonnes de base des transactions
      col("transaction_id"),
      col("main_user_id").as("user_id"),        // Renommer pour l'output final
      col("main_product_id").as("product_id"),  // Renommer pour l'output final
      col("main_merchant_id").as("merchant_id"), // Renommer pour l'output final
      col("timestamp"),
      col("amount"),

      // Colonnes des utilisateurs
      col("age"),
      col("annual_income"),
      col("city"),
      col("customer_segment"),
      col("preferred_categories"),
      col("registration_date"),

      col("product_category").as("category"), // Utiliser la catégorie produit comme catégorie principale
      col("price"),
      col("product_name").as("name"),

      // Colonnes des marchands
      col("merchant_name"),

      // Features temporelles
      col("hour"),
      col("day_of_week"),
      col("month"),
      col("is_weekend"),
      col("day_period"),
      col("is_working_hours"),

      // Autres features
      col("age_group"),
      col("transaction_rank"),
      col("total_transactions_per_user")
    )
  }

  /**
   * Ajoute des analyses de fenêtrage glissant au DataFrame enrichi.
   */
  def addWindowAnalytics(enrichedDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window

    // Conversion timestamp en unix timestamp
    val dfWithTs = enrichedDF.withColumn(
      "ts_long",
      unix_timestamp(to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
    )

    // Fenêtre de 7 jours en secondes (7 * 24 * 3600 = 604800)
    val window7Days = Window
      .partitionBy("user_id")  // user_id après enrichTransactionData
      .orderBy("ts_long")
      .rangeBetween(-604800, 0)

    // Calcul du montant cumulé sur 7 jours
    val withCumulative = dfWithTs
      .withColumn("cumulative_amount_7d", sum("amount").over(window7Days))

    // Pour calculer l'utilisateur actif (5 jours sur 7)
    val dailyWindow = Window
      .partitionBy("user_id")
      .orderBy("ts_long")
      .rangeBetween(-604800, 0)

    val result = withCumulative
      .withColumn("transaction_date", to_date(to_timestamp(col("timestamp"), "yyyyMMddHHmmss")))
      .withColumn("distinct_days_7d",
        size(collect_set("transaction_date").over(dailyWindow)))
      .withColumn("is_active_user",
        when(col("distinct_days_7d") >= 5, 1).otherwise(0))
      .drop("ts_long", "transaction_date", "distinct_days_7d")

    result
  }

  /**
   * Charge les données des utilisateurs depuis un fichier JSON.
   * La fonction gère l'inférence de schéma et le renommage de colonnes si nécessaire.
   */
  def loadUsers(spark: SparkSession, usersPath: String): DataFrame = {
    // Schéma personnalisé pour le fichier users.json
    val usersSchema = new StructType()
      .add("age", IntegerType, nullable = true)
      .add("annual_income", IntegerType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("customer_segment", StringType, nullable = true)
      .add("preferred_categories", ArrayType(StringType), nullable = true)
      .add("registration_date", StringType, nullable = true)
      .add("user_id", StringType, nullable = true)

    spark.read
      .option("inferSchema", "false")
      .schema(usersSchema)
      .json(usersPath)
      .withColumn("registration_date", to_date(col("registration_date"), "yyyyMMdd"))
  }

  /**
   * Calcule le nombre de ventes quotidiennes à partir des transactions.
   */
  def calculateDailySales(spark: SparkSession, transactionsPath: String): DataFrame = {
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(transactionsPath)

    val dailySalesDF = transactionsDF
      .withColumn("transaction_date", to_date(col("transaction_date"), "yyyyMMdd"))
      .groupBy("transaction_date")
      .count()
      .withColumnRenamed("count", "daily_sales")
      .sort(asc("transaction_date"))

    dailySalesDF
  }
}