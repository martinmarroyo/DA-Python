"""A collection of utility functions used for ETL"""

def extract_schema(conf:dict, key:str = "data", val:str = "schema") -> list:
    """Checks the given configuration file and extracts a unique list of all schemas"""
    data = conf[key]
    result = set()
    for table in data:
        result.add(data[table][val])
    return list(result)


def extract_table(conf:dict, key:str = "data") -> list:
    """Checks the given configuration file and extracts a unique list of all tables"""
    data = conf[key]
    result = set()
    for table in data:
        schema = data[table]["schema"]
        tablename = data[table]["tablename"]
        result.add(f"{schema}.{tablename}")
    return list(result)
    
