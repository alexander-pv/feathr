

#### Local Feathr with RBAC support:

* Choose your database for RBAC and Registry
* Go to `./envs`, choose env file and set email to `RBAC_DEFAULT_ADMIN` (Microsoft account email for now)
* Choose one of the docker-compose files and run:

#### Feathr SQL Registry and UI: 
```bash
# SQLite test
$ docker compose --env-file ./feathr-sandbox/envs/sqlite-registry.env -f ./feathr-sandbox/docker/docker-compose.registry.sqlite.yml up
# PostgreSQL test
$ docker compose --env-file ./feathr-sandbox/envs/pgsql-registry.env -f ./feathr-sandbox/docker/docker-compose.registry.pgsql.yml up
# MariaDB test
$ docker compose --env-file ./feathr-sandbox/envs/mariadb-registry.env -f ./feathr-sandbox/docker/docker-compose.registry.mariadb.yml up
# MySQL test
$ docker compose --env-file ./feathr-sandbox/envs/mysql-registry.env -f ./feathr-sandbox/docker/docker-compose.registry.mysql.yml up
# MSSQL. Supported by only experimental mssqlv2 connector!
$ docker compose --env-file ./feathr-sandbox/envs/mssql-registry.env -f ./feathr-sandbox/docker/docker-compose.registry.mssql.yml up
```
Databases need some time to be ready, so you may see connection errors first. 

#### Feathr Sandbox

Example includes:
* Servers: RBAC, Registry
* PostgreSQL RDBMS for RBAC and Registry
* Feathr UI
* Online storage - Redis
* Offline storage - MinIO(S3)
* Spark master and worker
* Sandbox with Feathr and JupyterLab
```bash
$ docker compose --env-file ./feathr-sandbox/envs/sandbox.env  -f ./feathr-sandbox/docker/docker-compose.sandbox.yml up -d
```

Go to localhost:8888, open in Jupyter Lab, and authorize:

Current identity provider is only MS Azure:
```bash
$ az login --allow-no-subscriptions
```
Run init script:
```bash
$ cd feathr_init && python feathr_init_script.py
```

Example output:
```postgresql
(base) jovyan@71268fe0bd96:~/work$ cd feathr_init && python feathr_init_script.py
2023-08-30 23:21:21.212 | INFO     | __main__:<module>:42 - Feathr sandbox settings: {
'feathr_runtime_path': '/opt/feathr-runtime/feathr-runtime.jar',
'feathr_config_path': '/home/jovyan/work/feathr_init/feathr_config.yaml', 
'spark_local_ip': '127.0.0.1', 'test_init_timeout': 5000, 
'data_file_path': '/tmp/green_tripdata_2020-04_with_index.csv', 
'timestamp_col': 'lpep_dropoff_datetime', 
'timestamp_format': 'yyyy-MM-dd HH:mm:ss'
}
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 3.83k/3.83k [00:03<00:00, 1.17kKB/s]
2023-08-30 23:21:25.793 | INFO     | feathr.utils._env_config_reader:get:62 - Config secrets__azure_key_vault__name is not found in the environment variable, configuration file, or the remote key value store. Returning the default value: None.
2023-08-30 23:21:25.794 | INFO     | feathr.utils._env_config_reader:get:62 - Config offline_store__adls__adls_enabled is not found in the environment variable, configuration file, or the remote key value store. Returning the default value: None.
2023-08-30 23:21:25.794 | INFO     | feathr.utils._env_config_reader:get:62 - Config offline_store__wasb__wasb_enabled is not found in the environment variable, configuration file, or the remote key value store. Returning the default value: None.
2023-08-30 23:21:25.794 | INFO     | feathr.utils._env_config_reader:get:62 - Config offline_store__jdbc__jdbc_enabled is not found in the environment variable, configuration file, or the remote key value store. Returning the default value: None.
2023-08-30 23:21:25.794 | INFO     | feathr.utils._env_config_reader:get:62 - Config offline_store__snowflake__snowflake_enabled is not found in the environment variable, configuration file, or the remote key value store. Returning the default value: None.
2023-08-30 23:21:25.795 | INFO     | feathr.utils._env_config_reader:get:62 - Config feature_registry__purview__purview_name is not found in the environment variable, configuration file, or the remote key value store. Returning the default value: None.
2023-08-30 23:21:25.797 | INFO     | feathr.client:__init__:213 - Feathr client 1.0.0 initialized successfully.
2023-08-30 23:21:25.797 | SUCCESS  | __main__:main:57 - Initialized FeathrClient. Project: local_spark
2023-08-30 23:21:25.866 | INFO     | __main__:main:171 - Trying to register features in Feathr...
2023-08-30 23:21:35.024 | SUCCESS  | __main__:main:179 - Registered features:
[{'name': 'f_trip_distance','id': 'a2c4de27-937e-43f3-a672-343eb2b8387e','qualifiedName': 'local_spark__feature_anchor__f_trip_distance'},
{'name': 'f_trip_time_duration', 'id': 'e62a608b-702b-4b1c-b7ed-652e9d184588','qualifiedName': 'local_spark__feature_anchor__f_trip_time_duration'}, 
{'name': 'f_is_long_trip_distance', 'id': '66c05044-c083-4e05-a19d-79af431628a8','qualifiedName': 'local_spark__feature_anchor__f_is_long_trip_distance'},
{'name': 'f_day_of_week', 'id': 'dc23306c-e129-4b8c-93e9-d4086bb2a921', 'qualifiedName': 'local_spark__feature_anchor__f_day_of_week'},
{'name': 'f_day_of_month', 'id': 'dc7b0632-1486-4c9c-a6d8-f6b0b182f964', 'qualifiedName': 'local_spark__feature_anchor__f_day_of_month'}, 
{'name': 'f_hour_of_day', 'id': 'f4aa71f2-d7f1-4131-89e2-ad73cbc454ce', 'qualifiedName': 'local_spark__feature_anchor__f_hour_of_day'}, 
{'name': 'f_location_avg_fare', 'id': 'a4c7b7f3-c0d7-4fd0-b8ed-51aba8c0671a', 'qualifiedName': 'local_spark__agg_feature_anchor__f_location_avg_fare'},
{'name': 'f_location_max_fare', 'id': 'a04e32c8-ddf7-44e9-9bc3-69eec3018496', 'qualifiedName': 'local_spark__agg_feature_anchor__f_location_max_fare'}, 
{'name': 'f_trip_time_rounded', 'id': '8a4dca0e-64eb-49c5-8030-e918faf289ab', 'qualifiedName': 'local_spark__f_trip_time_rounded'},
{'name': 'f_trip_time_distance', 'id': '36267464-a04c-445d-bdcd-993ab0dfe93f', 'qualifiedName': 'local_spark__f_trip_time_distance'}]
/opt/conda/lib/python3.9/site-packages/feathr/utils/job_utils.py:215: DtypeWarning: Columns (4) have mixed types. Specify dtype option on import or set low_memory=False.
  return pd.read_csv(dir_path)
2023-08-30 23:21:35.199 | WARNING  | feathr.spark_provider._localspark_submission:submit_feathr_job:78 - Local Spark Mode only support basic params right now and should be used only for testing purpose.
2023-08-30 23:21:35.199 | INFO     | feathr.spark_provider._localspark_submission:_get_debug_file_name:287 - Spark log path is debug/local_spark_feathr_feature_join_job20230830232135
2023-08-30 23:21:35.199 | INFO     | feathr.spark_provider._localspark_submission:_init_args:262 - Spark job: local_spark_feathr_feature_join_job is running on local spark with master: spark://spark-master:7077.
2023-08-30 23:21:35.209 | INFO     | feathr.spark_provider._localspark_submission:submit_feathr_job:142 - Detail job stdout and stderr are in debug/local_spark_feathr_feature_join_job20230830232135/log.
2023-08-30 23:21:35.209 | INFO     | feathr.spark_provider._localspark_submission:submit_feathr_job:152 - Local Spark job submit with pid: 307.
2023-08-30 23:21:35.209 | INFO     | __main__:main:199 - Waiting for job to finish...
2023-08-30 23:21:35.210 | INFO     | feathr.spark_provider._localspark_submission:wait_for_completion:162 - 1 local spark job(s) in this Launcher, only the latest will be monitored.
2023-08-30 23:21:35.210 | INFO     | feathr.spark_provider._localspark_submission:wait_for_completion:163 - Please check auto generated spark command in debug/local_spark_feathr_feature_join_job20230830232135/command.sh and detail logs in debug/local_spark_feathr_feature_join_job20230830232135/log.

```