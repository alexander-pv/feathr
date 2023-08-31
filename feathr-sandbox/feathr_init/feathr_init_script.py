"""
Goal of this file is to run a basic Feathr script within spark
so that Maven packages can be downloaded into the docker container to save time during actual run.
"""
import os
from dataclasses import dataclass, asdict
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from feathr import BOOLEAN, FLOAT, INT32, ValueType
from feathr import FeathrClient
from feathr import Feature, DerivedFeature, FeatureAnchor
from feathr import FeatureQuery, ObservationSettings
from feathr import INPUT_CONTEXT, HdfsSource
from feathr import TypedKey
from feathr import WindowAggTransformation
from feathr.datasets.constants import NYC_TAXI_SMALL_URL
from feathr.datasets.utils import maybe_download
from feathr.utils.job_utils import get_result_df
from loguru import logger


@dataclass
class SandboxSettings:
    # Common settings
    feathr_runtime_path: str = os.getenv("FEATHR_RUNTIME_PATH")
    feathr_config_path: str = os.getenv("FEATHR_CONFIG_PATH")
    spark_local_ip: str = os.getenv("SPARK_LOCAL_IP")
    test_init_timeout: int = 5000

    # Test data settings
    data_file_path: str = os.getenv("DATA_FILE_PATH")
    timestamp_col: str = "lpep_dropoff_datetime"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"

    def as_dict(self) -> dict:
        return asdict(self)


sandbox_settings = SandboxSettings()
logger.info(f"Feathr sandbox settings: {sandbox_settings.as_dict()}")


def preprocessing(df: DataFrame) -> DataFrame:
    df = df.withColumn("fare_amount_cents",
                       (F.col("fare_amount") * 100.0).cast("float"))
    return df


def main() -> None:
    maybe_download(
        src_url=NYC_TAXI_SMALL_URL,
        dst_filepath=sandbox_settings.data_file_path
    )
    client = FeathrClient(config_path=sandbox_settings.feathr_config_path)
    logger.success(f"Initialized FeathrClient. Project: {client.project_name}")

    batch_source = HdfsSource(
        name="nycTaxiBatchSource",
        path=sandbox_settings.data_file_path,
        event_timestamp_column=sandbox_settings.timestamp_col,
        preprocessing=preprocessing,
        timestamp_format=sandbox_settings.timestamp_format,
    )

    # We define f_trip_distance and f_trip_time_duration features separately
    # so that we can reuse them later for the derived features.
    f_trip_distance = Feature(
        name="f_trip_distance",
        feature_type=FLOAT,
        transform="trip_distance",
    )
    f_trip_time_duration = Feature(
        name="f_trip_time_duration",
        feature_type=FLOAT,
        transform="cast_float((to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime)) / 60)",
    )

    features = [
        f_trip_distance,
        f_trip_time_duration,
        Feature(
            name="f_is_long_trip_distance",
            feature_type=BOOLEAN,
            transform="trip_distance > 30.0",
        ),
        Feature(
            name="f_day_of_week",
            feature_type=INT32,
            transform="dayofweek(lpep_dropoff_datetime)",
        ),
        Feature(
            name="f_day_of_month",
            feature_type=INT32,
            transform="dayofmonth(lpep_dropoff_datetime)",
        ),
        Feature(
            name="f_hour_of_day",
            feature_type=INT32,
            transform="hour(lpep_dropoff_datetime)",
        ),
    ]

    # After you have defined features, bring them together to build the anchor to the source.
    feature_anchor = FeatureAnchor(
        name="feature_anchor",
        source=INPUT_CONTEXT,  # Pass through source, i.e. observation data.
        features=features,
    )

    agg_key = TypedKey(
        key_column="DOLocationID",
        key_column_type=ValueType.INT32,
        description="location id in NYC",
        full_name="nyc_taxi.location_id",
    )

    agg_window = "90d"

    # Anchored features with aggregations
    agg_features = [
        Feature(
            name="f_location_avg_fare",
            key=agg_key,
            feature_type=FLOAT,
            transform=WindowAggTransformation(
                agg_expr="fare_amount_cents",
                agg_func="AVG",
                window=agg_window,
            ),
        ),
        Feature(
            name="f_location_max_fare",
            key=agg_key,
            feature_type=FLOAT,
            transform=WindowAggTransformation(
                agg_expr="fare_amount_cents",
                agg_func="MAX",
                window=agg_window,
            ),
        ),
    ]

    agg_feature_anchor = FeatureAnchor(
        name="agg_feature_anchor",
        # External data source for feature. Typically, a data table.
        source=batch_source,
        features=agg_features,
    )

    f_trip_time_distance = DerivedFeature(name="f_trip_time_distance",
                                          feature_type=FLOAT,
                                          input_features=[
                                              f_trip_distance, f_trip_time_duration],
                                          transform="f_trip_distance * f_trip_time_duration")

    f_trip_time_rounded = DerivedFeature(name="f_trip_time_rounded",
                                         feature_type=INT32,
                                         input_features=[f_trip_time_duration],
                                         transform="f_trip_time_duration % 10")

    derived_feature = [f_trip_time_distance, f_trip_time_rounded]

    client.build_features(
        anchor_list=[feature_anchor, agg_feature_anchor],
        derived_feature_list=derived_feature,
    )

    feature_names = [feature.name for feature in features + agg_features]
    logger.info("Trying to register features in Feathr...")
    # Try to register the service after the spark run (so that the Feathr API can start with sufficient time)
    try:
        client.register_features()
    except Exception as e:
        logger.error("Error occurred during feature registration")
        logger.error(e)
    reg_features = client.list_registered_features(project_name=client.project_name)
    logger.success(f"Registered features:\n{reg_features}")

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    offline_features_path = os.path.join("debug", f"test_output_{now}")

    # Features that we want to request. Can use a subset of features
    query = FeatureQuery(
        feature_list=feature_names,
        key=agg_key,
    )
    settings = ObservationSettings(
        observation_path=sandbox_settings.data_file_path,
        event_timestamp_column=sandbox_settings.timestamp_col,
        timestamp_format=sandbox_settings.timestamp_format,
    )
    client.get_offline_features(
        observation_settings=settings,
        feature_query=query,
        output_path=offline_features_path,
    )
    logger.info("Waiting for job to finish...")
    client.wait_job_to_finish(timeout_sec=sandbox_settings.test_init_timeout)
    res_df = get_result_df(client)
    logger.info(f"Resulting dataframe:\n{res_df.head()}")


if __name__ == "__main__":
    main()
