import os

from Common import db_sql
from Common.logger import LOGGER

if os.environ['DATABASE_TYPE'] == 'mysql':
    import pymysql as sqllib
elif os.environ['DATABASE_TYPE'] == 'mssql':
    import pymssql as sqllib
else:
    LOGGER.error(f'DATABASE_TYPE 환경변수 확인. 입력값: {os.environ["DATABASE_TYPE"]}')
'''
db_option.py 파일을 통해 DB옵션을 가져옴

json형태로 Save, load

{"host": "IP",
  "user":"ID",
  "password":"PW",
  "db": "TEST_SCHEM",
  "charset": "utf8mb4"
}
'''


class DBConnect:

    __db_instance = None

    # singleton pattern
    @classmethod
    def get_instance(cls):
        if cls.__db_instance is None:
            cls.__db_instance = cls()
        return cls.__db_instance

    def __init__(self):

        self._db_options = self._options()
        self._client = None
        self._connect_check()

    @property
    def get_client(self):
        self._connect()
        return self._client

    @staticmethod
    def get_sql():
        return db_sql

    def _connect(self):
        connection = sqllib.connect(**self._db_options)
        LOGGER.debug('DB connect.')
        # Connection 으로부터 Dictoionary Cursor 생성 (참고)
        # curs = conn.cursor(pymysql.cursors.DictCursor)
        self._client = connection

    def commit(self):
        return self._client.commit()

    @staticmethod
    def _options():
        if os.environ['DATABASE_TYPE'] == 'mysql':
            return {"host": os.environ['DATABASE_HOST'],
                    "user": os.environ['DATABASE_USER'],
                    "password": os.environ['DATABASE_PASSWORD'],
                    "db": os.environ['DATABASE_NAME'],
                    "charset": "utf8mb4",
                    "autocommit": False
                    }
        elif os.environ['DATABASE_TYPE'] == 'mssql':
            return {'host': os.environ['DATABASE_HOST'],
                    'port': os.environ['DATABASE_PORT'],
                    'user': os.environ['DATABASE_USER'],
                    'password': os.environ['DATABASE_PASSWORD'],
                    'charset': 'utf8',
                    'database': os.environ['DATABASE_NAME'],
                    }

    def _connect_check(func):
        def decorated(self, *args, **kwargs):
            if not self._client:
                self._connect()
            return func(self, *args, **kwargs)
        return decorated

    @_connect_check
    def insert_data(self, sql, data, auto_commit=False):
        '''
        :param sql : str
        :param data: [(col1, col2, col2, ...), ...]  List(tuple)
        :param auto_commit:
        :return: bool is_success
        '''
        LOGGER.debug(f'Insert sql : {sql}')
        LOGGER.debug(f'data : {data}')
        for i in range(5):
            affected = -1

            try:
                cursor = self._client.cursor()
            except AttributeError:
                LOGGER.error('---------------SERVER CLOSED--------------------')
                raise

            try:
                affected = cursor.executemany(sql, data)
                if auto_commit:
                    self._client.commit()
                cursor.close()
                return affected
                # return affected
            # TODO: 나올수 있는 에러 정리 / 처리 ( Exception )
            except sqllib.Error as e:
                LOGGER.warning('[RETRY]INSERT SQL Error.. : %s' % e)
                cursor.close()
                self._connect()
            except Exception as e:
                LOGGER.warning('[RETRY]DB Error.. : %s' % e)
                cursor.close()
        LOGGER.error("Database Error...")
        LOGGER.exception('exception')
        raise sqllib.DatabaseError

    @_connect_check
    def delete_data(self, sql, data=None, auto_commit=False):
        '''
        :param sql : str
        :param data: [(col1, col2, col2, ...), ...]  List(tuple)
        :param auto_commit:
        :return: bool is_success
        '''
        LOGGER.debug(f'Delete sql : {sql}')
        LOGGER.debug(f'data : {data}')
        for i in range(5):
            try:
                cursor = self._client.cursor()
            except AttributeError:
                LOGGER.error('---------------SERVER CLOSED--------------------')
                raise

            try:
                affected = cursor.execute(sql, data)
                if auto_commit:
                    self._client.commit()
                cursor.close()
                return affected
            # TODO: 나올수 있는 에러 정리 / 처리 ( Exception )
            except sqllib.Error as e:
                LOGGER.warning('[RETRY]DELETE SQL Error.. : %s' % e)
                cursor.close()
                self._connect()
            except Exception as e:
                LOGGER.warning('[RETRY]DB Error.. : %s' % e)
                cursor.close()
        LOGGER.error("Database Error...")
        raise sqllib.DatabaseError

    @_connect_check
    def select_data(self, sql, data=None):
        '''
        :param sql : str
        :param data: [(col1, col2, col2, ...), ...]  List(tuple)
        :param auto_commit:
        :return: bool is_success
        '''
        LOGGER.debug(f'Select sql : {sql}')
        LOGGER.debug(f'data : {data}')
        for i in range(5):
            try:
                if os.environ['DATABASE_TYPE'] == 'mysql':
                    cursor = self._client.cursor(sqllib.cursors.DictCursor)
                elif os.environ['DATABASE_TYPE'] == 'mssql':
                    cursor = self._client.cursor(as_dict=True)
            except AttributeError:
                LOGGER.error('---------------SERVER CLOSED--------------------')
                raise

            try:
                cursor.execute(sql, data)
                rows = cursor.fetchall()
                cursor.close()
                return rows
            # TODO: 나올수 있는 에러 정리 / 처리 ( Exception )
            except sqllib.Error as e:
                LOGGER.warning('[RETRY]SELECT SQL Error.. : %s' % e)
                cursor.close()
                self._connect()
            except Exception as e:
                LOGGER.warning('[RETRY]DB Error.. : %s' % e)
                cursor.close()
        LOGGER.error("Database Error...")
        raise sqllib.DatabaseError


