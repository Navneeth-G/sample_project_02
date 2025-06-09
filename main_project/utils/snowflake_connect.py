import snowflake.connector

def create_snowflake_connection(input_dict: dict):
    """
    Establishes a Snowflake connection using credentials from input_dict.

    Parameters:
        input_dict (dict): Must include keys:
            - sf_user, sf_password, sf_account,
            - sf_warehouse, sf_database, sf_schema

    Returns:
        snowflake.connector.connection.SnowflakeConnection: A live connection object.
    """
    return snowflake.connector.connect(
        user=input_dict["sf_user"],
        password=input_dict["sf_password"],
        account=input_dict["sf_account"],
        warehouse=input_dict["sf_warehouse"],
        database=input_dict["sf_database"],
        schema=input_dict["sf_schema"]
    )
