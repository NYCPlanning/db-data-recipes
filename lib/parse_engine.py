from urllib.parse import urlparse

def parse_engine(engine):
    # engine format : "postgresql://postgres:postgres@localhost/postgres")
    result = urlparse(engine)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    portnum = result.port
    return dict(
        database = database,
        user = username,
        password = password,
        host = hostname,
        port = portnum,
    )