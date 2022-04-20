from dagster import resource


@resource()
def my_dummy_pyspark_resource():
    """Not doing anything, only serving as a mock instance for the multimodel IO manager"""
    pass
