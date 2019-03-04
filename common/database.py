import src.database_making.ActualDBMaking.dbconfig as dbconfig
import psycopg2


class Database(object):
    CONNECTION = None
    CURSOR = None

    @staticmethod
    def initialize():
        local_params = {
            'database': 'test 2',
            'user': dbconfig.USERNAME,
            'password': dbconfig.PASSWORD,
            'host': 'localhost',
            'port': 5432
        }
        hosted_params = {
            #REDACTED
        }
        try:
            Database.CONNECTION = psycopg2.connect(**local_params)
            Database.CURSOR = Database.CONNECTION.cursor()
        except:
            print("I am unable to connect to the database")
            print("If running locally, make sure to use local_params")
            print("""pg_ctl -D "{}" restart""".format(dbconfig.PATH_TO_DATA))

    @staticmethod
    def commit():
        Database.CONNECTION.commit()

    @staticmethod
    def rollback():
        Database.CONNECTION.rollback()

    @staticmethod
    def save():
        SQL = "SAVEPOINT save;"
        Database.CURSOR.execute(SQL)

    @staticmethod
    def reload():
        SQL = "ROLLBACK TO SAVEPOINT save;"
        Database.CURSOR.execute(SQL)

    @staticmethod
    def release():
        SQL = "RELEASE SAVEPOINT save;"
        Database.CURSOR.execute(SQL)

    @staticmethod
    def insert(table, params, placeholders, data):
        SQL = "INSERT INTO {} ({}) VALUES ({});".format(table, params, placeholders)
        Database.CURSOR.execute(SQL, data)

    @staticmethod
    def drop(table):
        SQL = "DROP TABLE IF EXISTS {};".format(table)
        Database.CURSOR.execute(SQL)

    @staticmethod
    def create(table, params, p_key):
        SQL = "CREATE TABLE {} ({}, PRIMARY KEY({}));".format(table, params, p_key)
        Database.CURSOR.execute(SQL)

    @staticmethod
    def find_all(table, params=[], data=[]):
        if isinstance(params, str):
            params = [params]
        if isinstance(data, str):
            data = [data]
        if len(params) == len(data):
            SQL = "SELECT * FROM {}".format(table)
            if len(params) > 0:
                SQL += " WHERE {} = %s".format(params[0])
                for index in range(1, len(params)):
                    SQL += " AND {} = %s".format(params[index])
            SQL += ";"
            Database.CURSOR.execute(SQL, data)
            return Database.CURSOR.fetchall()
        else:
            print("ERROR: LEN(PARAMS) DID NOT MATCH LEN(DATA); FIND_ALL()")

    @staticmethod
    def find_one(table, params=[], data=[]):
        if isinstance(params, str):
            params = [params]
        if isinstance(data, str):
            data = [data]
        if len(params) == len(data):
            SQL = "SELECT * FROM {}".format(table)
            if len(params) > 0:
                SQL += " WHERE {} = %s".format(params[0])
                for index in range(1, len(params)):
                    SQL += " AND {} = %s".format(params[index])
            SQL += ";"
            Database.CURSOR.execute(SQL, data)
            return Database.CURSOR.fetchone()
        else:
            print("LEN(PARAMS) DID NOT MATCH LEN(DATA); FIND_ONE()")

    @staticmethod
    def find_by_query(query, data):
        Database.CURSOR.execute(query, data)
        return Database.CURSOR.fetchall()
