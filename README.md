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
- optional resources / lazy instantiation https://dagster.slack.com/archives/C01U954MEER/p1650448663629669
  - same goes for DBT using duckDB
- specify output type of asset https://dagster.slack.com/archives/C01U954MEER/p1650457944030399 (especially when there is no downstream consumer yet


Further topics:
- dagster YARN awareness like Oozie
- general code review
- later docker-compose the example for deployment
- UI inconsistencies for Job an Asset graph view
  - tags for compute_kind (missing in asset view)
  - upstream asset changed stale (in asset view)

open MRs:
 - direct asset_key access https://github.com/dagster-io/dagster/pull/7395 (TODO fix workaround)


## installing a custom dagster version:

```bash
pip install -e python_modules/dagster
```

## pipeline overview

![pipeline overview](img/pipeline.png)