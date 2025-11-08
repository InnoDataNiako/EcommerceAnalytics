# Ecommerce Analytics - Spark/Scala + Streamlit Demo

Ce projet démontre la mise en œuvre d'un pipeline de données complet pour l'analyse de données e-commerce. Il inclut l'ingestion de données multi-formats, la validation, les transformations avancées avec Spark SQL, et la génération de rapports analytiques business.

## Démonstration en Ligne
[Application Streamlit](lien_streamlit)

## Architecture
- **Backend** : Spark/Scala (Data Engineering)
- **Frontend** : Streamlit (Visualisation)
- **Données** : Multi-format (CSV, JSON, Parquet)

## Structure du Projet
```
EcommerceAnalytics/
├── spark_code/          # Code source Spark/Scala
├── demo_results/        # Résultats d'analyse
├── app.py              # Application Streamlit
└── requirements.txt    # Dépendances Python
```

## Guide d'Utilisation

### Code Spark/Scala
```bash
cd spark_code
sbt run
```

### Application Streamlit
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Fonctionnalités Principales
- Analyse de données e-commerce
- Segmentation client
- Analyse de cohorte
- KPI marchands
- Visualisations interactives

## Auteur
**NIAKO KEBE**

- GitHub: [InnoDataNiako](https://github.com/InnoDataNiako)
- LinkedIn: [niako kebe](https://linkedin.com/in/niako-kebe)

## Licence
Ce projet est développé dans le cadre d'une formation Data Engineering.