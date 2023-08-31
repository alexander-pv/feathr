FROM gradle:7.6.0-jdk8 as gradle-build
WORKDIR /usr/src/feathr
COPY . .
RUN ./gradlew build


FROM jupyter/pyspark-notebook:python-3.9.13
ARG FEATHR_RUNTIME_PATH
USER root

## Install dependencies
RUN apt-get update -y \
    && apt-get install -y \
    software-properties-common gnupg freetds-dev sqlite3 libsqlite3-dev  \
    lsb-release lsof cmake curl iputils-ping vim


# librdkafka-dev has scarce amount of versions in Ubuntu apt repo
# Related issue: https://github.com/confluentinc/confluent-kafka-python/issues/1466
WORKDIR /usr/src/librdkafka
RUN wget -q https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.1.0.zip && \
    unzip v2.1.0.zip && \
    cd librdkafka-2.1.0 && \
    ./configure && \
    make && \
    make install && \
    ldconfig && \
    cd .. && \
    rm -rf librdkafka-2.1.0


# CLI for identity providers
# Azure: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash


# Run the script so that maven cache can be added for better experience.
# Otherwise users might have to wait for some time for the maven cache to be ready.
RUN python -m pip install interpret==0.4.4
## Feathr
WORKDIR /home/jovyan/work
# Runtime
COPY --chown=1000:100 --from=gradle-build /usr/src/feathr/build/libs $FEATHR_RUNTIME_PATH
RUN mv $FEATHR_RUNTIME_PATH/feathr_*.jar $FEATHR_RUNTIME_PATH/feathr-runtime.jar
# Client
COPY ./feathr_project ./feathr_project
RUN cd feathr_project &&  \
    python -m pip install . && \
    cd .. && rm -rf feathr_project
RUN chown -R 1000:100 /opt/conda/lib/python3.9


EXPOSE 8888 7080
WORKDIR /home/jovyan/work
USER jovyan
CMD start-notebook.sh --ip='*' --NotebookApp.password='' --NotebookApp.token='' --no-browser