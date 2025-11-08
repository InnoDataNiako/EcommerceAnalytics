
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Le chemin que vous avez dans votre terminal :
file_path = "cohort_analysis/part-00000-56329b1d-cdf4-4bd3-a9b6-6542a8908094-c000.csv" 
df = pd.read_csv(file_path)


# ==============================================================================
# 2. PRÉPARATION DES DONNÉES
# ==============================================================================

# Définir 'cohort_month' comme index
df = df.set_index('cohort_month')

# Retirer la taille de la cohorte et le mois 0 (toujours 100%)
df_retention = df.drop(columns=['cohort_size', '0.0'])

# Renommer les colonnes pour une meilleure lisibilité dans le graphique (Mois 1, 2, 3...)
df_retention.columns = [f"Mois {int(float(c))}" for c in df_retention.columns]


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