# MedicareML: Predictive Modeling and Fraud Detection for Medicare Providers

This project leverages big data analytics and machine learning to predict future Medicare provider expenses (HCC scores) and identify potential fraudulent activity. 
It integrates multiple large-scale datasets: Medicare annual provider data, IRS aggregated tax data by ZIP Code, EPA carbon monoxide measurements, and U.S. ZIP Code demographic information.

## Infrastructure & Data Engineering
- Data ingestion and management using Azure Data Factory, Blob Storage, and Data Lake zoning.
- Processing and analysis on Azure Databricks using PySpark, handling both structured and unstructured data.
- Feature engineering pipelines implemented with `VectorAssembler`, `StringIndexer`, `OneHotEncoderEstimator`, and `Pipeline`.

## Machine Learning Models
- **Supervised Learning:** Logistic Regression, Random Forest (PySpark ML)
- **Unsupervised Learning / Anomaly Detection:** K-Means Clustering, with potential extensions to hierarchical clustering, XGBoost, neural networks, and KNN.
- Visualizations generated with `matplotlib.pyplot`.

## Key Highlights
- Integrated heterogeneous datasets across healthcare, tax, environmental, and demographic sources.
  https://www.kaggle.com/cms/medicare-physician-other-supplier-npi-aggregates
  https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi
  https://www.kaggle.com/epa/hazardous-air-pollutants
  https://www.kaggle.com/epa/carbon-monoxide
  https://simplemaps.com/data/us-zips
<img width="652" height="123" alt="image" src="https://github.com/user-attachments/assets/6240dd7f-6afa-447a-9bbf-601d9e72c212" />

- Built scalable ML pipelines for end-to-end predictive modeling and clustering.
- Designed a foundation for anomaly detection engines to detect potential Medicare fraud.

