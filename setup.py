import setuptools

dagster_version = "0.14.7"
setuptools.setup(
    name="SSH_DEMO",
    packages=setuptools.find_packages(exclude=["SSH_DEMO_tests"]),
    package_data={"SSH_DEMO_dbt": ["SSH_DEMO_dbt/*"]},
    install_requires=[
        f"dagster=={dagster_version}",
        f"dagster-ssh=={dagster_version}",
        f"dagster-pandas=={dagster_version}",
        f"dagster-dbt=={dagster_version}",
        f"dagster-postgres=={dagster_version}",
        f"dagster-pyspark=={dagster_version}",
        f"dagit=={dagster_version}",
        "pytest",
    ],
)
