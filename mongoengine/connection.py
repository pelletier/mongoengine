from pymongo import Connection
import threading

__all__ = ['ConnectionError', 'connect', 'disconnect']


_connection_defaults = {
    'host': 'localhost',
    'port': 27017,
}
_connection_settings = _connection_defaults.copy()

_db_name = None
_db_username = None
_db_password = None

_mongo_cache = threading.local()


class ConnectionError(Exception):
    pass


def _get_connection(reconnect=False):
    global _mongo_cache
    # Connect to the database if not already connected
    if not hasattr(_mongo_cache, 'connection') or reconnect:
        try:
            _mongo_cache.connection = Connection(**_connection_settings)
        except:
            raise ConnectionError('Cannot connect to the database')
    return _mongo_cache.connection

def _get_db(reconnect=False):
    global _mongo_cache

    if not hasattr(_mongo_cache, 'db') or reconnect:
        # Connect if not already connected
        connection = _get_connection(reconnect=reconnect)
        
        # _db_name will be None if the user hasn't called connect()
        if _db_name is None:
            raise ConnectionError('Not connected to the database')

        # Get DB from current connection and authenticate if necessary
        _mongo_cache.db = connection[_db_name]
        if _db_username and _db_password:
            _mongo_cache.db.authenticate(_db_username, _db_password)
    
    return _mongo_cache.db
    
def connect(db, username=None, password=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection 
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    global _connection_settings, _db_name, _db_username, _db_password
    _connection_settings = dict(_connection_defaults, **kwargs)
    _db_name = db
    _db_username = username
    _db_password = password
    return _get_db(reconnect=True)

def disconnect():
    global _connection_settings, _db_name, _db_username, _db_password, _mongo_cache
    _mongo_cache = threading.local()
    _db_name = None
    _db_username = None
    _db_password = None
    _connection_settings = _connection_defaults.copy()