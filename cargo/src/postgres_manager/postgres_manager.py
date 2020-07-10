from loguru import logger

from sqlalchemy.engine import create_engine


class PostgresProcessor:
    '''
    # How to create connection
    from cfs.src.db_utils import PostgresConnection
    conn = PostgresConnection().with_config(dict(config['PROJECT_DB_PG'])).establish_connection()
    '''

    def __init__(self):
        # connection params
        self.username = None
        self.password = None
        self.host = None
        self.port = None
        self.database = None
        # session objetcs
        self.connection = None

    def with_config(self, config):
        self.username = config['user']
        self.password = config['password']
        self.host = config['host']
        self.port = int(config['port'])
        self.database = config['db']
        return self

    def establish_connection(self):
        logger.debug('Open connection to postgres: {host}:{port}'.format(
            host=self.host,
            port=self.port
        ))
        conn_str = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
        )
        engine = create_engine(conn_str)
        self.connection = engine.connect()

    def execute(self, *args, **kwargs):
        # TODO не прописывать обёртки а делегировать все обращение к атриюутам
        # через __getatribute__   к объекту self.connection
        return self.connection.execute(*args, **kwargs)

    def shutdown(self):
        logger.debug('Close connection to postgres.')
        self.connection.close()
