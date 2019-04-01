from urllib.parse import urlparse

def parse_engine(engine):
    result = urlparse(engine)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    portnum = result.port
    
    return f'PG:host={hostname} port={portnum} user={username} dbname={database} password={password}'