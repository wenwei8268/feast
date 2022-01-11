# Dataset

Feast Datasets allow for conveniently saving dataframes that include both features and entities to be subsequently used for data analysis and model training.
[Data Quality Monitoring]() was the primary motivation for creating Dataset concept.

Dataset's metadata is stored in the Feast registry and dataframe with features, entities, additional input keys and timestamp is kept in the [offline store](../architecture-and-components/offline-store.md).

Dataset can be created from:
1. Results of historical retrieval
2. Logging request (including input for [on demand transformation](../../reference/alpha-on-demand-feature-view.md)) and response during feature serving
3. Logging features during writing to online store (from batch source or stream)


### Creating Saved Dataset from Historical Retrieval

To create a saved dataset from historical features for later retrieval or analysis, use the `save_as` parameter of `get_historical_features` method.
If this parameter is supplied, Feast eagerly computes the DataFrame and stores the data using specified `storage`.
Any offline store can be used as such storage. Out of the box Feast supports BigQuery, Redshift and File storage (local, Google Storage or S3).

```python

from feast import FeatureStore
from feast.saved_dataset import SavedDatasetOptions
from feast.infra.offline_stores.bigquery_source import SavedDatasetBigQueryStorage

store = FeatureStore()

store.get_historical_features(
    features=["driver:avg_trip"],
    entity_df=...,
    save_as=SavedDatasetOptions(
        name='my_training_dataset',
        storage=SavedDatasetBigQueryStorage(table_ref='<gcp-project>.<gcp-dataset>.my_training_dataset')
    )
)
```

Saved dataset can be later retrieved using `get_saved_dataset` method:
```python
retrieval_job = store.get_saved_dataset('my_training_dataset')
retrieval_job.to_df()
```