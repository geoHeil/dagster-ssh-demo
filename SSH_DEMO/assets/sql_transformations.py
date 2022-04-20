import json
import os

from dagster_dbt import dbt_cli_resource
from dagster_dbt.asset_defs import load_assets_from_dbt_manifest, load_assets_from_dbt_project

from SSH_DEMO.resources import DBT_PROFILES_DIR, DBT_PROJECT_DIR

#dbt_prod_resource = dbt_cli_resource.configured(
 #   {"profiles_dir": DBT_PROFILES_DIR, "project_dir": DBT_PROJECT_DIR, "target": "prod"}
#)

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR, io_manager_key="io_manager"
)

#assets = load_assets_from_dbt_manifest(
#    json.load(open(os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"))),
#    io_manager_key="warehouse_io_manager",
#)
