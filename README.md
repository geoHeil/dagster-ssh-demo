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

## problems

### case 1 naive python job (sftp_sensor_dummy.py):

Status: works
Result: remote file paths are logged

![](img/Sensor__my_directory_sensor_SFTP.png)

### case 2: naive python job with ssh-resoure (sftp_sensor_dummy_provided_resource.py)

> To reproduce: https://github.com/geoHeil/dagster-ssh-demo/blob/master/SSH_DEMO/repository.py re-enable the sensor and job for `provided_resources` (the 2nd example).

Adapting case 1 to use the provided [dagster-ssh](https://docs.dagster.io/_apidocs/libraries/dagster-ssh#) resource:

```
# :rtype: paramiko.client.SSHClient
# this internally is calling paramiko.client.SSHClient.connect (but failing)
# before I manually did the same thing but succeeded. Where is the error? I cannot spot it right now.
ssh.get_connection()
```

Stacktrace:

```
The above exception was caused by the following exception:
AttributeError: 'NoneType' object has no attribute '_fields'

Sensor daemon caught an error for sensor my_directory_sensor_SFTP_provided_resource : dagster.core.errors.SensorExecutionError: Error occurred during the execution of evaluation_fn for sensor my_directory_sensor_SFTP_provided_resource

Stack Trace:
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster/grpc/impl.py", line 284, in get_external_sensor_execution
    return sensor_def.evaluate_tick(sensor_context)
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/contextlib.py", line 137, in __exit__
    self.gen.throw(typ, value, traceback)
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster/core/errors.py", line 191, in user_code_error_boundary
    raise error_cls(

The above exception was caused by the following exception:
AttributeError: 'NoneType' object has no attribute '_fields'

Stack Trace:
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster/core/errors.py", line 184, in user_code_error_boundary
    yield
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster/grpc/impl.py", line 284, in get_external_sensor_execution
    return sensor_def.evaluate_tick(sensor_context)
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster/core/definitions/sensor_definition.py", line 334, in evaluate_tick
    result = list(ensure_gen(self._evaluation_fn(context)))
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster/core/definitions/sensor_definition.py", line 494, in _wrapped_fn
    for item in result:
  File "/path/to/SSH_DEMO/SSH_DEMO/sensors/sftp_sensor_dummy_provided_resource.py", line 74, in my_directory_sensor_SFTP_provided_resource
    ssh.get_connection()
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/dagster_ssh/resources.py", line 105, in get_connection
    client.connect(
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/paramiko/client.py", line 420, in connect
    if our_key != server_key:
  File "/path/to/miniconda/base/envs/dagster-ssh-demo/lib/python3.9/site-packages/paramiko/pkey.py", line 143, in __eq__
    return self._fields == other._fields
```


### case 3: sensor + asset: how to transfer the configuration? 
#### Step 1: Workaround (sftp_sensor_asset_workaround.py)

Using an OP I can pass the metadata (key/file path) form the sensor to the job for later materialization of the remote file in a central analytics location (DWH/lake/whatever).

Status: running, but: not SDA native, requires several sensors to be created to link up further assets & update these in case of changes

The files in the SFTP directory https://github.com/geoHeil/dagster-ssh-demo/tree/master/sft_sample/upload/2022-01-01 (for each day 2022-01-01 ... 02/03) the tables/CSVs foo,bar,baz are present. For each of these three tables an output asset should be created - and it is:

```
dummy_asset__bar
dummy_asset__baz
dummy_asset__foo
```

They are all created successfully.
However, it seems to be cumbersome to add for each of these a sensor that would fire off any additonal compute step (4,5) whereas in the world of software defined assets this should be automatic (by the lineage of the assets). Also the lineage is missiong.
Would a source asset perhaps work here? So far, I did not manage to get this to work as desired.

#### Step 2: direct but failing (sftp_sensor_asset.py)

Status failing: I cannot pass the configuration to the asset. How can I accomplish this task?


### case 4 multi asset updates + trigger comptuation step

After materialization of a partition trigger a computation step (for each of the tables/assets/CSV files) which outputs (for each of them) a new asset.

When trying to work with the workaround presented before and reading the assets directly with an `asset_sensor` the job fails with:
```
DagsterInvalidDefinitionError: Input asset '["dummy_asset__foo"]' for asset 'my_asset_computation' is not produced by any of the provided asset ops and is not one of the provided sources
```
As the dynamically generated asset somehow cannot be referenced. Why is this the case? How can I obtain a valid reference?


When Step 2 works: How can the whole process be simplfied?

### case 5

After case 4 combine the outputs of 4 (SQL: JOIN) and then perform a big computation / update. Via DBT & DuckDB

TODO - first finish the open topics above