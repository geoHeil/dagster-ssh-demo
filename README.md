# SSH_DEMO

## usage

```bash
git clone https://github.com/geoHeil/dagster-ssh-demo.git
cd dagster-ssh-demo

conda activate base
conda install -y -c conda-forge mamba
conda deactivate

make create_environment

# follow the instructions below to set the DAGSTER_HOME
# and perform an editable installation (if you want to toy around with this dummy pipeline)
conda activate dagster-ssh-demo
pip install --editable .

dagit
# explore: Go to http://localhost:3000

# optionally enable:
dagster-daemon run
# to use schedules and backfills

docker-compose up
# to start the SFTP server
```

## questions

- partition aware downstream sensors https://dagster.slack.com/archives/C01U954MEER/p1649909251959589
  - is it needed at all?
  - can it be more direct i.e. passed down instead of listened?
  - can the listening be improved (pushed down)?


Further topics:
- general code review
- later docker-compose the example for deployment
  - Volume mounts problems for docker executor: https://dagster.slack.com/archives/C01U954MEER/p1650643391313319

open MRs/issues:
 - direct asset_key access https://github.com/dagster-io/dagster/pull/7395 (TODO fix workaround)
 - UI inconsistencies for Job an Asset graph view
  - compute_kind metadata missing https://github.com/dagster-io/dagster/issues/7503 and https://github.com/dagster-io/dagster/issues/6956
  - stale UI notification in asset https://github.com/dagster-io/dagster/issues/7434 why is this also a problem in the jobs view https://dagster.slack.com/archives/C01U954MEER/p1650625527293049?

  - lazy resource initialization https://github.com/dagster-io/dagster/issues/7585 and https://github.com/dagster-io/dagster/issues/7586
  

 Extended topics:
 - dagster YARN awareness like Oozie K8s on YARN?
 - multi repository multi workspace multi git repo/python package example deployment
  - https://dagster.slack.com/archives/C01U954MEER/p1650643391313319 is perhaps a first step towards this
  - there are still open questions with regards to volume mounts


## installing a custom dagster version:

```bash
pip install -e python_modules/dagster
```

## pipeline overview

![pipeline overview](img/pipeline.png)


## deployment 

> inspired by https://github.com/dehume/big-data-madison-dagster/blob/main/README.md


### Quick Start
Assumes you have [Docker](https://www.docker.com/) running on your machine. To start up the project do the following:

0. modify the dagster.yaml (container_kwargs/volumes) configuration to point to the local directory (must be an absolute path) you want to use for the volume mapping
1. `make start-detached`
2. Access dagit: [http://localhost:3000/](http://localhost:3000/)

The `Makefile` also has some handy commands for running formatting (`make fmt`) and running tests for the two workspaces (`make test-ssh-demo`).



### cleanup

```
docker-compose down

rm -r warehouse_docker postgres-dagster
´``
