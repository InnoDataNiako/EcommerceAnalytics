package com.ecommerce.analytics

import com.ecommerce.models._
import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.spark.sql.Encoder


class DataIngestion(spark: SparkSession, config: Config = ConfigFactory.load()) {
  import spark.implicits._

  private val logger = LogManager.getLogger(getClass.getName)

  /**
   * Petite fonction utilitaire "safe"
   *  Elle essaye d’exécuter un bloc (par exemple: lecture CSV),
   */
  private def safe[T: Encoder](name: String)(block: => Dataset[T]): Dataset[T] = {
    try {
      val ds = block
      logger.info(s"$name: ${ds.count()} lignes lues.")
      ds
    } catch {
      case e: Exception =>
        logger.error(s"Erreur lecture $name : ${e.getMessage}", e)
        spark.emptyDataset[T]
    }
  }

  /** ====================
   * LECTURE DES DONNÉES
   * ==================== */

  // Lecture Transactions depuis un CSV
  def readTransactions(): Dataset[Transaction] = safe("transactions") {
    val path = config.getString("app.data.input.transactions") // chemin dans application.conf
    val schema = StructType(Array(
      StructField("transaction_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("product_id", StringType, true),
      StructField("merchant_id", StringType, true),
      StructField("amount", DoubleType, true),
      StructField("timestamp", StringType, true),
      StructField("location", StringType, true),
      StructField("payment_method", StringType, true),
      StructField("category", StringType, true)
    ))

    // Lecture CSV avec un schéma défini pour éviter des erreurs de typage
    val df = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE") // Les lignes invalides sont ignorées mais loguées
      .schema(schema)
      .csv(path)

    df.as[Transaction] // Conversion en Dataset[Transaction]
  }

  // Lecture Users depuis un JSON
  def readUsers(): Dataset[User] = safe("users") {
    val path = config.getString("app.data.input.users")

    // Lecture du fichier JSON Lines (un objet par ligne)
    val df = spark.read
      .option("mode", "PERMISSIVE")
      .json(path)

    val normalized = df.select(
      col("user_id").cast(StringType).as("user_id"),
      col("age").cast(IntegerType).as("age"),
      col("annual_income").cast(DoubleType).as("annual_income"),
      col("city").cast(StringType).as("city"),
      col("customer_segment").cast(StringType).as("customer_segment"),
      when(col("preferred_categories").isNull, array()) // Si null -> tableau vide
        .otherwise(col("preferred_categories")).cast("array<string>")
        .as("preferred_categories"),
      col("registration_date").cast(StringType).as("registration_date")
    )

    normalized.as[User]
  }

  // Lecture Products depuis un Parquet
  def readProducts(): Dataset[Product] = safe("products") {
    val path = config.getString("app.data.input.products")
    val df = spark.read.parquet(path)

    df.select(
      col("product_id").cast(StringType),
      col("name").cast(StringType),
      col("category").cast(StringType),
      col("price").cast(DoubleType),
      col("merchant_id").cast(StringType),
      col("rating").cast(DoubleType),
      col("stock").cast(IntegerType)
    ).as[Product]
  }

  // Lecture Merchants depuis un CSV
  def readMerchants(): Dataset[Merchant] = safe("merchants") {
    val path = config.getString("app.data.input.merchants")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    df.select(
      col("merchant_id").cast(StringType),
      col("name").cast(StringType),
      col("category").cast(StringType),
      col("region").cast(StringType),
      col("commission_rate").cast(DoubleType),
      col("establishment_date").cast(StringType)
    ).as[Merchant]
  }

  /** ====================
   * VALIDATION DES DONNÉES
   * ==================== */

  // Validation Transactions : montant > 0 et timestamp de 14 caractères (yyyyMMddHHmmss)
  def validateTransactions(ds: Dataset[Transaction]): Dataset[Transaction] = {
    val total = ds.count()
    val validDF = ds.toDF()
      .filter(col("amount") > 0 && length(col("timestamp")) === 14)
    val valid = validDF.as[Transaction]
    logger.info(s"[Transactions] lues=$total, valides=${valid.count()}, invalides=${total - valid.count()}")
    valid
  }

  // Validation Users : âge entre 16 et 100, revenu annuel positif
  def validateUsers(ds: Dataset[User]): Dataset[User] = {
    val total = ds.count()
    val validDF = ds.toDF()
      .filter(col("age").between(16, 100) && col("annual_income") > 0)
    val valid = validDF.as[User]
    logger.info(s"[Users] lues=$total, valides=${valid.count()}, invalides=${total - valid.count()}")
    valid
  }

  // Validation Products : prix positif et note (rating) entre 1 et 5
  def validateProducts(ds: Dataset[Product]): Dataset[Product] = {
    val total = ds.count()
    val validDF = ds.toDF()
      .filter(col("price") > 0 && col("rating").between(1.0, 5.0))
    val valid = validDF.as[Product]
    logger.info(s"[Products] lues=$total, valides=${valid.count()}, invalides=${total - valid.count()}")
    valid
  }

  // Validation Merchants : taux de commission entre 0 et 1
  def validateMerchants(ds: Dataset[Merchant]): Dataset[Merchant] = {
    val total = ds.count()
    val validDF = ds.toDF()
      .filter(col("commission_rate").between(0.0, 1.0))
    val valid = validDF.as[Merchant]
    logger.info(s"[Merchants] lues=$total, valides=${valid.count()}, invalides=${total - valid.count()}")
    valid
  }
}
