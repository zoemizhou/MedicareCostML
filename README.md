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
- Built scalable ML pipelines for end-to-end predictive modeling and clustering.
- Designed a foundation for anomaly detection engines to detect potential Medicare fraud.

