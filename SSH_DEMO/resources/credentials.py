from dagster import resource


@resource(config_schema={"username": str, "password": str})
def the_credentials(init_context):
    user_resource = init_context.resource_config["username"]
    pass_resource = init_context.resource_config["password"]
    return user_resource, pass_resource
