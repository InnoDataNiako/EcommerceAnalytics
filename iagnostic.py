# diagnostic.py - Sauvegardez ce fichier dans EcommerceAnalytics-Portfolio/
import pandas as pd
import os

print("🔍 DIAGNOSTIC DES DONNÉES SPARK")
print("=" * 50)

# Vérifier merchant_report
try:
    merchant_path = "demo_results/merchant_report"
    if os.path.exists(merchant_path):
        csv_files = [f for f in os.listdir(merchant_path) if f.endswith('.csv')]
        if csv_files:
            file_path = os.path.join(merchant_path, csv_files[0])
            df_merchant = pd.read_csv(file_path)
            print("📊 MERCHANT REPORT - Colonnes:", df_merchant.columns.tolist())
            print("    Premières lignes:")
            print(df_merchant.head(2))
        else:
            print("❌ Aucun CSV dans merchant_report/")
    else:
        print("❌ Dossier merchant_report/ introuvable")
except Exception as e:
    print(f"❌ Erreur merchant_report: {e}")

print("\n" + "=" * 50)

# Vérifier cohort_analysis
try:
    cohort_path = "demo_results/cohort_analysis"
    if os.path.exists(cohort_path):
        csv_files = [f for f in os.listdir(cohort_path) if f.endswith('.csv')]
        if csv_files:
            file_path = os.path.join(cohort_path, csv_files[0])
            df_cohort = pd.read_csv(file_path)
            print("👥 COHORT ANALYSIS - Colonnes:", df_cohort.columns.tolist())
            print("    Premières lignes:")
            print(df_cohort.head(2))
        else:
            print("❌ Aucun CSV dans cohort_analysis/")
    else:
        print("❌ Dossier cohort_analysis/ introuvable")
except Exception as e:
    print(f"❌ Erreur cohort_analysis: {e}")

print("\n" + "=" * 50)

# Vérifier top_products
try:
    products_path = "demo_results/top_products"
    if os.path.exists(products_path):
        csv_files = [f for f in os.listdir(products_path) if f.endswith('.csv')]
        if csv_files:
            file_path = os.path.join(products_path, csv_files[0])
            df_products = pd.read_csv(file_path)
            print("📦 TOP PRODUCTS - Colonnes:", df_products.columns.tolist())
            print("    Premières lignes:")
            print(df_products.head(2))
        else:
            print("❌ Aucun CSV dans top_products/")
    else:
        print("❌ Dossier top_products/ introuvable")
except Exception as e:
    print(f"❌ Erreur top_products: {e}")