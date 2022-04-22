# based on
# 1) https://github.com/dehume/big-data-madison-dagster/blob/main/Dockerfile
# 2) https://hub.docker.com/r/mambaorg/micromamba

FROM mambaorg/micromamba:0.22.0 AS builder

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
USER root
RUN mkdir -p $DAGSTER_HOME
RUN groupadd -r dagster && useradd -m -r -g dagster dagster && \
    chown -R dagster:dagster $DAGSTER_HOME
USER dagster

RUN mkdir -p $DAGSTER_HOME
WORKDIR $DAGSTER_HOME

COPY --chown=dagster:dagster environment.yml /tmp/env.yaml
RUN micromamba install -y --name base -f /tmp/env.yaml && \
     micromamba clean --all --yes

# https://hub.docker.com/r/mambaorg/micromamba
ARG MAMBA_DOCKERFILE_ACTIVATE=1  # (otherwise python will not be found)
# RUN python -c "import uuid; print(uuid.uuid4())"

# dagit and daemon
FROM builder AS dagit
EXPOSE 3000
RUN chown -R dagster:dagster $DAGSTER_HOME
USER dagster:dagster
CMD ["dagit", "-h", "0.0.0.0", "--port", "3000", "-w", "workspace.yaml"]

FROM builder AS daemon
USER dagster:dagster
CMD ["dagster-daemon", "run"]

# Formatting
FROM python:3.8-slim AS test
COPY requirements/ $DAGSTER_HOME/requirements
RUN pip install --no-cache-dir -r $DAGSTER_HOME/requirements/dev-requirements.txt
COPY . /src

# workspaces
FROM builder AS ssh-demo
COPY . ./src
WORKDIR $DAGSTER_HOME/src/
USER dagster:dagster
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "SSH_DEMO/repository.py"]

FROM builder AS ssh-demo-test
COPY . ./src
WORKDIR $DAGSTER_HOME/src/
USER dagster:dagster
CMD ["python", "-m", "pytest", ".", "-v"]

FROM builder AS other
COPY workspaces/other/ ./src
WORKDIR $DAGSTER_HOME/src/
USER dagster:dagster
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4001", "-f", "repo.py"]
