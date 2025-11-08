"""ier est : output/merchant_report/part-00000-fcbef5a1-ae37-40f3-905a-f0da892173db-c000.csv

Veuillez copier le contenu de ce fichier (en particulier la ligne d'en-tête et quelques lignes de données) ici. Dès que je l'aurai, je pourrai construire les graphiques suivants :

Top 10 des marchands (par CA total).

Répartition du CA par catégorie et segment d'âge (empilé).

voici lentete : on\Python313\python.exe c:/Master1-Document/Cour_DataEngenieur/projects/EcommerceAnalytics/output/Visualisation/visualisationMerchant.py
  merchant_id   nom_marchand    categorie              region  ...  ca_jeune  ca_adulte  ca_age_moyen  ca_senior
0      M00262    CarParts 63   Automotive           Grand Est  ...  39413.35  168808.03     199064.05  142222.04
1      M00229  AutoCenter 30   Automotive  Nouvelle-Aquitaine  ...  48617.09  136233.01     120698.40  102903.77
2      M00305     AutoShop 6   Automotive           Occitanie  ...  52431.34  128222.04      94957.45  130747.58
3      M00115  DigitalHub 16  Electronics           Grand Est  ...  26872.37   97892.04     135853.22  124327.43
4      M00283    AutoShop 84   Automotive           Grand Est  ...  45911.63  107980.30     106434.05  117329.67"""


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
# Le chemin que vous avez dans votre terminal :
file_path = "merchant_report/part-00000-fcbef5a1-ae37-40f3-905a-f0da892173db-c000.csv"
df = pd.read_csv(file_path)
# Afficher les premières lignes du DataFrame pour vérifier le chargement
print(df.head())
# Afficher les informations sur le DataFrame pour comprendre sa structure
print(df.info())
# Afficher les statistiques descriptives pour les colonnes numériques
print(df.describe())
# Afficher les noms des colonnes pour référence
print(df.columns)
# ==============================================================================
# 2. PRÉPARATION DES DONNÉES

# ==============================================================================
# Top 10 des marchands par CA total
top_10_merchants = df.nlargest(10, 'ca_total')
plt.figure(figsize=(12, 6))
sns.barplot(data=top_10_merchants, x='nom_marchand', y='ca_total', palette='viridis')
plt.title('Top 10 des Marchands par CA Total')
plt.xlabel('Nom du Marchand')
plt.ylabel('CA Total')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
# Répartition du CA par catégorie et segment d'âge (empilé)
age_segments = ['ca_jeune', 'ca_adulte', 'ca_age_moyen', 'ca_senior']
ca_by_category = df.groupby('categorie')[age_segments].sum().reset_index()
ca_by_category.set_index('categorie', inplace=True)
ca_by_category.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='viridis')
plt.title('Répartition du CA par Catégorie et Segment d\'Âge')
plt.xlabel('Catégorie')
plt.ylabel('CA Total')
plt.xticks(rotation=45)
plt.legend(title='Segment d\'Âge')
plt.tight_layout()
plt.show()
# ==============================================================================
# 3. CRÉATION DE LA HEATMAP
# ==============================================================================
plt.figure(figsize=(18, 10))
sns.heatmap(
    df_retention,
    annot=True,          # Afficher les valeurs de rétention dans les cellules
    fmt=".1f",           # Formater les pourcentages avec une décimale
    cmap="YlGnBu",       # Palette de couleurs (plus foncé = meilleure rétention)
    linewidths=.5,       # Ajouter des séparateurs entre les cellules
    linecolor='white',
    cbar_kws={'label': 'Taux de Rétention (%)'} # Légende de la barre de couleur
)
plt.title('Heatmap de Rétention des Cohortes (en %)', fontsize=20, pad=20)
plt.ylabel('Cohorte (Mois d\'Acquisition)', fontsize=16)
plt.xlabel('Mois depuis l\'Acquisition', fontsize=16)
plt.yticks(rotation=0)
plt.show()
