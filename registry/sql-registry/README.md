# SQL-Based Registry for Feathr

This is the reference implementation of [the Feathr API spec](./api-spec.md), base on SQL databases.

### Environment Settings

| Variable                         | Description | Default     |
|----------------------------------|-------------|-------------|
| FEATHR_REGISTRY_DATABASE         |             | sqlite      |
| FEATHR_REGISTRY_CONNECTION_STR   |             | -           |
| FEATHER_REGISTRY_LISTENING_PORT  |             | 8000        |
| FEATHR_API_BASE                  |             | /api/v1     |
| REGISTRY_DEBUGGING               |             | 0           |
| LOGGING_LEVEL                    |             | DEBUG       |
| UVICORN_LOG_LEVEL                |             | info        | 