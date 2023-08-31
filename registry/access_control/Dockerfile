FROM python:3.9


COPY . /usr/src/registry
## Install dependencies
RUN apt-get update -y && apt-get install -y freetds-dev unixodbc
# RBAC package
WORKDIR /usr/src/registry/access_control
RUN pip install .
# Common handlers package
WORKDIR /usr/src/registry/handlers
RUN pip install .

## Start server
WORKDIR /usr/src/registry
CMD python -m feathr_rbac
