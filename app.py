import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json

st.set_page_config(
    page_title="Ecommerce Analytics - Spark/Scala",
    page_icon="📊",
    layout="wide"
)

# --- CSS Personnalisé ---
st.markdown("""
<style>
    .spark-header {
        background: linear-gradient(135deg, #E65C19 0%, #F39C12 100%);
        padding: 3rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    .tech-badge {
        background: #2c3e50;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
    }
</style>
""", unsafe_allow_html=True)

# --- En-tête ---
st.markdown("""
<div class="spark-header">
    <h1 style='margin:0; font-size: 2.8rem;'>🔥 Ecommerce Analytics</h1>
    <p style='font-size: 1.3rem; opacity: 0.9;'>Pipeline Data Engineering Spark/Scala - Résultats en Temps Réel</p>
    <div style='margin-top: 1rem;'>
        <span class="tech-badge">Apache Spark 3.4</span>
        <span class="tech-badge">Scala 2.13</span>
        <span class="tech-badge">SBT</span>
        <span class="tech-badge">Parquet</span>
        <span class="tech-badge">Window Functions</span>
    </div>
</div>
""", unsafe_allow_html=True)

# --- Fonction de chargement des données AVEC VRAIS NOMS DE COLONNES ---
@st.cache_data
def load_merchant_report():
    try:
        # Essayer de charger depuis les résultats Spark
        base_path = os.path.join('demo_results', 'merchant_report')
        if os.path.exists(base_path):
            csv_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]
            if csv_files:
                file_path = os.path.join(base_path, csv_files[0])
                df = pd.read_csv(file_path)
                st.success(f"✅ Données marchands chargées: {len(df)} lignes")
                return df
    except Exception as e:
        st.error(f"❌ Erreur chargement marchands: {e}")
    
    # Données exemple si les résultats ne sont pas disponibles
    st.info("ℹ️ Utilisation des données exemple pour la démo")
    return pd.DataFrame({
        'merchant_id': [f'M{10000+i}' for i in range(10)],
        'nom_marchand': ['TechStore', 'BookWorld', 'FashionHub', 'ElectroMax', 'HomeEssentials', 
                         'SportsGear', 'BeautySpot', 'KidsCorner', 'FoodMarket', 'JewelBox'],
        'chiffre_affaires_total': [125000, 89000, 156000, 210000, 78000, 92000, 67000, 54000, 112000, 98000],
        'nombre_transactions': [1250, 980, 1650, 2100, 850, 920, 720, 600, 1100, 950],
        'clients_uniques': [450, 320, 580, 720, 290, 380, 250, 180, 420, 350],
        'montant_moyen_transaction': [100.0, 90.8, 94.5, 100.0, 91.8, 100.0, 93.1, 90.0, 101.8, 103.2],
        'rang_categorie': [1, 2, 1, 1, 3, 2, 1, 2, 1, 1],
        'rang_region': [1, 1, 2, 1, 3, 2, 1, 2, 1, 1],
        'commission_totale': [12500, 8900, 15600, 21000, 7800, 9200, 6700, 5400, 11200, 9800]
    })

@st.cache_data
def load_cohort_analysis():
    try:
        base_path = os.path.join('demo_results', 'cohort_analysis')
        if os.path.exists(base_path):
            csv_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]
            if csv_files:
                file_path = os.path.join(base_path, csv_files[0])
                df = pd.read_csv(file_path)
                
                # Renommer les colonnes de rétention pour plus de clarté
                rename_cols = {}
                for col in df.columns:
                    if col.replace('.', '').isdigit():
                        rename_cols[col] = f'mois_{int(float(col))}'
                df = df.rename(columns=rename_cols)
                
                return df
    except Exception as e:
        st.error(f"Erreur chargement cohortes: {e}")
    
    # Données exemple
    return pd.DataFrame({
        'cohort_month': ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'],
        'cohort_size': [150, 180, 200, 220, 240],
        'mois_0': [1.0, 1.0, 1.0, 1.0, 1.0],
        'mois_1': [0.65, 0.72, 0.68, 0.75, 0.70],
        'mois_2': [0.45, 0.52, 0.48, 0.55, 0.50],
        'mois_3': [0.35, 0.42, 0.38, 0.45, 0.40]
    })

@st.cache_data
def load_top_products():
    try:
        base_path = os.path.join('demo_results', 'top_products')
        if os.path.exists(base_path):
            csv_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]
            if csv_files:
                file_path = os.path.join(base_path, csv_files[0])
                return pd.read_csv(file_path)
    except Exception as e:
        st.error(f"Erreur chargement produits: {e}")
    
    # Données exemple
    return pd.DataFrame({
        'product_id': [f'P{1000+i}' for i in range(15)],
        'nom_produit': ['iPhone 15', 'MacBook Pro', 'Samsung TV', 'Nike Shoes', 'Adidas Jacket',
                        'Python Book', 'Coffee Maker', 'Gaming Mouse', 'Wireless Headphones',
                        'Smart Watch', 'Camera', 'Tablet', 'Desk Lamp', 'Water Bottle', 'Backpack'],
        'categorie': ['Electronics']*5 + ['Books']*2 + ['Home']*2 + ['Electronics']*3 + ['Home']*2 + ['Fashion'],
        'chiffre_affaires': [500000, 450000, 380000, 320000, 280000, 
                        250000, 220000, 200000, 180000, 160000,
                        150000, 140000, 120000, 100000, 90000],
        'nombre_ventes': [5000, 4500, 3800, 3200, 2800, 2500, 2200, 2000, 1800, 1600, 1500, 1400, 1200, 1000, 900],
        'note': [4.8, 4.7, 4.6, 4.5, 4.4, 4.3, 4.2, 4.1, 4.0, 3.9, 3.8, 3.7, 3.6, 3.5, 3.4]
    })

# --- Chargement des données ---
merchant_data = load_merchant_report()
cohort_data = load_cohort_analysis()
products_data = load_top_products()

# --- Sidebar ---
with st.sidebar:
    st.header(" À Propos du Projet")
    
    st.markdown("""
    **🎯 Objectifs:**
    - Pipeline Data Engineering complet
    - Analyse e-commerce multi-sources
    - Optimisations Spark avancées
    
    **⚡ Technologies:**
    - Apache Spark 3.4.0
    - Scala 2.13.12
    - SBT 1.9.7
    - Hadoop/Parquet
    
    **📊 Données Traitées:**
    - 7.5M+ transactions
    - Données utilisateurs
    - Catalogue produits
    - Informations marchands
    """)
    
    # Debug info
    with st.expander("🔍 Debug Info"):
        st.write("**Colonnes Marchands:**", list(merchant_data.columns))
        st.write("**Colonnes Cohortes:**", list(cohort_data.columns))
        st.write("**Colonnes Produits:**", list(products_data.columns))

# --- Onglets Principaux ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "🏪 Marchands", "👥 Clients", "📦 Produits", "⚡ Spark"])

with tab1:
    st.subheader("📈 Vue d'Ensemble des Performances")
    
    # KPI Principaux - AVEC VRAIS NOMS DE COLONNES
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = merchant_data['chiffre_affaires_total'].sum()
        st.metric("💰 CA Total", f"${total_revenue:,.0f}")
    
    with col2:
        total_transactions = merchant_data['nombre_transactions'].sum()
        st.metric("🛒 Transactions", f"{total_transactions:,}")
    
    with col3:
        total_customers = merchant_data['clients_uniques'].sum()
        st.metric("👥 Clients Uniques", f"{total_customers:,}")
    
    with col4:
        avg_transaction = merchant_data['montant_moyen_transaction'].mean()
        st.metric("📈 Panier Moyen", f"${avg_transaction:.2f}")
    
    # Graphiques principaux
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top 10 Marchands par CA")
        top_merchants = merchant_data.nlargest(10, 'chiffre_affaires_total')
        fig = px.bar(top_merchants, x='nom_marchand', y='chiffre_affaires_total',
                    color='chiffre_affaires_total', 
                    title="Top 10 Marchands par Chiffre d'Affaires",
                    color_continuous_scale='Viridis')
        fig.update_layout(height=400, xaxis_title="Marchand", yaxis_title="CA Total")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Répartition par Catégorie")
        category_revenue = merchant_data.groupby('categorie')['chiffre_affaires_total'].sum().reset_index()
        fig = px.pie(category_revenue, values='chiffre_affaires_total', names='categorie',
                    hole=0.4, 
                    title="Répartition du CA par Catégorie",
                    color_discrete_sequence=px.colors.sequential.Plasma)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("📋 Analyse Détaillée des Marchands")
    
    # Tableau interactif
    st.dataframe(merchant_data, use_container_width=True, height=400)
    
    # Graphiques supplémentaires
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.scatter(merchant_data, x='nombre_transactions', y='chiffre_affaires_total',
                        size='clients_uniques', color='montant_moyen_transaction',
                        hover_name='nom_marchand', 
                        title="CA vs Volume Transactions",
                        labels={
                            'nombre_transactions': 'Nombre de Transactions',
                            'chiffre_affaires_total': 'CA Total',
                            'clients_uniques': 'Clients Uniques',
                            'montant_moyen_transaction': 'Panier Moyen'
                        })
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.box(merchant_data, y='montant_moyen_transaction', 
                    title="Distribution du Panier Moyen",
                    labels={'montant_moyen_transaction': 'Panier Moyen (€)'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Analyse par région
        region_sales = merchant_data.groupby('region')['chiffre_affaires_total'].sum().reset_index()
        fig_region = px.bar(region_sales, x='region', y='chiffre_affaires_total',
                           title="CA par Région",
                           color='chiffre_affaires_total')
        st.plotly_chart(fig_region, use_container_width=True)

with tab3:
    st.subheader("👥 Analyse de Cohortes Clients")
    
    # Heatmap de rétention
    retention_cols = [col for col in cohort_data.columns if col.startswith('mois_')]
    if retention_cols:
        retention_data = cohort_data[['cohort_month'] + retention_cols]
        
        # Préparer les données pour la heatmap
        heatmap_data = retention_data.set_index('cohort_month')
        
        fig = px.imshow(heatmap_data,
                       title="Taux de Rétention par Cohortes (%)",
                       color_continuous_scale='Viridis',
                       aspect="auto",
                       labels=dict(x="Mois", y="Cohorte", color="Rétention %"))
        st.plotly_chart(fig, use_container_width=True)
    
    # Graphique linéaire de rétention
    if retention_cols:
        fig = px.line(cohort_data, x='cohort_month', y=retention_cols,
                     title="Évolution de la Rétention par Cohortes",
                     markers=True,
                     labels={'value': 'Taux de Rétention (%)', 'variable': 'Mois'})
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.subheader("📦 Performance des Produits")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏅 Top 10 Produits")
        top_10 = products_data.nlargest(10, 'chiffre_affaires')
        fig = px.bar(top_10, x='nom_produit', y='chiffre_affaires',
                    color='chiffre_affaires', 
                    title="Top 10 Produits par Chiffre d'Affaires",
                    color_continuous_scale='Plasma')
        fig.update_layout(height=400, xaxis_title="Produit", yaxis_title="CA Total")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("⭐ Produits par Catégorie")
        category_sales = products_data.groupby('categorie')['chiffre_affaires'].sum().reset_index()
        fig = px.pie(category_sales, values='chiffre_affaires', names='categorie',
                    title="Répartition du CA par Catégorie de Produits")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Notes moyennes par catégorie
        category_rating = products_data.groupby('categorie')['note'].mean().reset_index()
        fig_rating = px.bar(category_rating, x='categorie', y='note',
                           title="Note Moyenne par Catégorie",
                           color='note')
        st.plotly_chart(fig_rating, use_container_width=True)

with tab5:
    st.subheader("⚡ Détails Techniques Spark")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🏗️ Architecture du Pipeline
        
        ```scala
        // 1. Data Ingestion Multi-format
        val transactions = DataIngestion.readTransactions()
        val users = DataIngestion.readUsers() 
        val products = DataIngestion.readProducts()
        val merchants = DataIngestion.readMerchants()
        
        // 2. Data Validation & Cleaning
        val validTx = ingestion.validateTransactions(txRaw)
        val validUsers = ingestion.validateUsers(usersRaw)
        
        // 3. Advanced Transformations
        val enriched = transformation.enrichTransactionData(
          tx, users, products, merchants
        )
        
        // 4. Window Functions Analytics
        val finalDF = transformation.addWindowAnalytics(enriched)
        
        // 5. Business Intelligence
        val merchantReport = analytics.generateMerchantReport(...)
        val cohortAnalysis = analytics.analyzeUserCohorts(...)
        ```
        """)
    
    with col2:
        st.markdown("""
        ### 🚀 Optimisations Implementées
        
        **🎯 Performance:**
        - `cache()` pour données réutilisées
        - `persist(StorageLevel.MEMORY_AND_DISK_SER)`
        - Broadcast joins pour petites tables
        - Partitionnement adaptatif
        
        **📊 Analytics Avancés:**
        - UDF: extraction features temporelles
        - Window Functions: calculs cumulatifs
        - Cohort Analysis: rétention clients
        - KPI marchands: classements, commissions
        
        **💾 Formats Supportés:**
        - CSV (transactions, marchands)
        - JSON (utilisateurs) 
        - Parquet (produits)
        """)
    
    # Statistiques d'exécution avec vraies données
    st.subheader("📈 Métriques d'Exécution Réelles")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Marchands Analysés", f"{len(merchant_data)}")
    
    with col2:
        st.metric("Cohortes Utilisateurs", f"{len(cohort_data)}")
    
    with col3:
        st.metric("Produits Top", f"{len(products_data)}")
    
    with col4:
        total_ca = merchant_data['chiffre_affaires_total'].sum()
        st.metric("CA Total Analysé", f"${total_ca:,.0f}")

# --- Footer avec liens ---

# --- Footer avec liens POUR STREAMLIT ---
st.markdown("---")

# Titre et description
st.markdown("### 🔗 Accès au Code Source Complet")
st.markdown("Le projet Spark/Scala complet avec tous les fichiers source est disponible sur GitHub")

# Bouton principal GitHub
st.markdown(
    """
    <div style="text-align: center; margin: 1.5rem 0;">
        <a href="https://github.com/InnoDataNiako/EcommerceAnalytics" target="_blank" style="text-decoration: none;">
            <button style="background: linear-gradient(135deg, #E65C19 0%, #F39C12 100%); 
                          color: white; padding: 14px 28px; border: none; border-radius: 10px; 
                          font-size: 1.1rem; font-weight: 600; cursor: pointer; 
                          box-shadow: 0 4px 15px rgba(230, 92, 25, 0.3);
                          transition: all 0.3s ease;">
                🚀 Voir le Code Source Spark/Scala
            </button>
        </a>
    </div>
    """, 
    unsafe_allow_html=True
)

# Liens supplémentaires
st.markdown(
    """
    <div style="text-align: center; margin-top: 1rem;">
        <a href="https://github.com/InnoDataNiako" target="_blank" style="color: #6c757d; text-decoration: none; margin: 0 1rem;">
            👨‍💻 Mon GitHub
        </a>
        <span style="color: #dee2e6">|</span>
        <a href="https://www.linkedin.com/in/niako-kebe-60819a284/" target="_blank" style="color: #6c757d; text-decoration: none; margin: 0 1rem;">
            💼 Mon LinkedIn
        </a>
    </div>
    """, 
    unsafe_allow_html=True
)

# Footer final
st.markdown(
    """
    <div style="text-align: center;">
        <p style="color: #adb5bd; margin-top: 1.5rem; font-style: italic;">
            Projet Data Engineering - Spark 3.4 | Scala 2.13 | Optimisations Avancées
        </p>
    </div>
    """, 
    unsafe_allow_html=True
)