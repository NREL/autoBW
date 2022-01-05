"""Syncing utility for Brightway2 databases"""

import logging

import brightway2 as bw
import psycopg2
from bw2data.backends.peewee import proxies
from peewee import DoesNotExist
from psycopg2.extras import DictCursor

from utils import validate_activity

RESET_LOCAL_DB = True

logging.basicConfig(format='%(asctime)s::%(levelname)s::%(name)s::%(lineno)s: %(message)s')

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# @TODO: remote exchanges need databases
# @TODO: need methods to handle deleted exchanges and activities


class Sync():
    """
    A synchronization class for local and remote brightway2 databases.
    """

    DEFAULT_UNCERTAINTY_TYPE = 0

    def __init__(self, conn, database: bw.Database, schema: str):
        """

        :param conn: psycopg2 connection object
        :param database: brightway2 database object
        :param schema: str remote schema name
        """

        self.conn = conn
        self.database = database
        self.schema = schema

    def get_remote_activities(self) -> list:
        """
        Collect activities from <self.schema>.activities.

        :return: list
        """

        sql = """SELECT "key", "name", "unit", "location", "type", "version",
         COALESCE("comment", '') AS "comment" FROM "{self.schema}"."activities"
         WHERE "database" = %(database)s;"""

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql, {'database': self.database.name})

            return [dict(result) for result in cur.fetchall()]

    def get_remote_activity(self, key: str) -> dict:
        """
        Collect activity for <key> from <self.schema>.activity
        :param key: str activity key
        :return: dict
        """

        sql = f"""SELECT "key", "name", "unit", "location", "type", "version",
         COALESCE("comment", '') AS "comment"
         FROM "{self.schema}"."activity" WHERE "key" = %(key)s;"""

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql, {'database': self.database.name,
                              'key': key})

            return dict(cur.fetchone())

    def get_local_activities(self) -> list:
        """
        Collect activities in self.database
        :return: list
        """

        activities = []
        for activity in self.database:
            activities.append(activity)

        return activities

    @staticmethod
    def get_local_exchanges(activity) -> list:
        """
        Collect exchanges for a local activity
        :param activity: brightway2 activity object
        :return: list
        """
        exchanges = []
        for exchange in activity.exchanges():
            exchanges.append(exchange)

        return exchanges

    def get_remote_exchanges(self, key: str) -> list:
        """
        Collect exchanges from <self.schema>.exchanges for <key>
        :param key: str activity key
        :return: list
        """

        sql = f"""SELECT "amount", "input", "uncertainty_type",
         COALESCE("comment", '') AS "comment", "version"
         FROM "{self.schema}"."exchange" WHERE "key" = %(key)s;"""

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql, {'key': key})

            results = [dict(result) for result in cur.fetchall()]

            exchanges = []
            for exchange in results:
                sql = f"""SELECT "name", "location", "unit", "type"
                 FROM "{self.schema}"."activity" WHERE "key" = %(key)s;"""

                cur.execute(sql, {'key': exchange['input']})

                exchange.update(cur.fetchone())
                exchange['input'] = (self.database.name, exchange['input'])
                exchanges.append(exchange)

        return exchanges

    def remote_database_exists(self, database: str) -> bool:
        """
        Check if <database> exists in <self.schema>.database
        :param database: str database name
        :return: bool
        """
        sql = f"""SELECT COUNT("database") FROM "{self.schema}"."database"
         WHERE "database" = %(database)s;"""

        with self.conn.cursor() as cur:
            cur.execute(sql, {'database': database})

            return bool(cur.fetchone()[0])

    def remote_activity_exists(self, db: str, key: str, version: int = None) -> bool:
        """
        Check if <key> exists for database <db> and <version) in <self.schema>.activity
        :param db: str database name
        :param key: str activity code
        :param version int activity version
        :return: bool
        """
        sql = f"""SELECT "version" FROM "{self.schema}"."activities"
         WHERE "key" = %(key)s AND "database" = %(db)s;"""

        with self.conn.cursor() as cur:
            cur.execute(sql, {'key': key, 'db': db})

            remote_version = cur.fetchone()

        if remote_version:
            _remove_version = remote_version[0] == version
        else:
            _remote_version = bool(remote_version)

        return _remote_version

    def remote_exchange_exists(self, exchange: dict) -> bool:
        """
        Check if <exchange> exists in <self.schema>.exchange
        :param exchange: dict exchange kvals
        :return: bool
        """
        sql = f"""SELECT COUNT(*) FROM "{self.schema}"."exchange"
         WHERE "key" = %(key)s AND "amount" = %(amount)s AND
         "input" = %(input)s AND "uncertainty_type" = %(uncertainty_type)s;"""

        with self.conn.cursor() as cur:
            cur.execute(sql, exchange)

            return bool(cur.fetchone()[0])

    def local_activity_exists(self, key: str) -> bool:
        """
        Check if <key> exists in self.database
        :param key: str activity key
        :return: bool
        """

        try:
            self.database.get(key)
        except DoesNotExist:
            return False
        else:
            return True

    def local_exchange_exists(self, exchange: dict, key: str) -> bool:
        """
        Check if <exchange> exists in <activity>
        :param exchange: dict exchange kvals
        :param key: str activity key
        :return:
        """

        activity = self.database.get(key)

        for _exchange in activity.exchanges():
            if _exchange.get('input') == exchange.get('input') and \
                    _exchange.get('amount') == exchange.get('amount') and \
                    _exchange.get('unit') == exchange.get('unit') and \
                    _exchange.get('type') == exchange.get('type'):
                return True

        return False

    def add_remote_database(self, database: str):
        """
        Add <database> to <self.schema>.database
        :param database: str database name
        :return:
        """
        if not self.remote_database_exists(database=self.database.name):
            sql = f"""INSERT INTO {self.schema}."database" ("database") VALUES (%(database)s);"""

            with self.conn.cursor() as cur:
                cur.execute(sql, {'database': database})

            self.conn.commit()

    def update_remote_exchange(self, key, local_exchange, remote_exchange):
        """
        Update <remote_exchange> to match <local_exchange>.
        :param key:
        :param local_exchange:
        :param remote_exchange:
        :return:
        """

        sql = f"""UPDATE "{self.schema}"."exchange" SET "amount" = %(local_amount)s,
         "type" = %(local_type)s, "uncertainty_type" = %(local_uncertainty_type)s,
         "comment" = %(local_comment)s, "version" = %(local_version)s
          WHERE "key" = %(activity_key)s AND "input" = %(exchange_key)s"""

        kvals = {'local_amount': local_exchange.get('amount'),
                 'local_type': local_exchange.get('type'),
                 'local_uncertainty_type': local_exchange.get('uncertainty_type', 0),
                 'local_comment': local_exchange.get('comment'),
                 'local_version': local_exchange.get('version'),
                 'activity_key': key,
                 'exchange_key': remote_exchange.get('input')
                 }

        with self.conn.cursor() as cur:
            cur.execute(sql, kvals)

        self.conn.commit()

    def add_remote_activity(self, activity: proxies.Activity):
        """
        Add <activity> to <self.schema>.activity and <self.schema>.activity_database
        :param activity: brightway Activity
        :return:
        """
        LOGGER.debug(activity)
        try:
            db, key = activity.key
        except AttributeError:
            db, key = activity.get('input')
            activity = bw.Database(db).get(key)

        validate_activity(activity)

        self.add_remote_database(database=db)

        # get activity version
        sql_version = f"""SELECT "version" FROM "{self.schema}"."activities" WHERE "key" = %(key)s
         AND "database" = %(database)s;"""
        with self.conn.cursor() as cur:
            cur.execute(sql_version, {'key': key, 'database': self.database.name})
            try:
                version_activity_remote = cur.fetchone()[0]
            except TypeError:
                version_activity_remote = None

        kvals_activity = {'key': key,
                          'name': activity.get('name') or key,
                          'location': activity.get('location'),
                          'type': activity.get('type'),
                          'unit': activity.get('unit'),
                          'version': activity.get('version'),
                          'comment': activity.get('comment')
                          }

        if version_activity_remote is None:
            sql_activity = f"""INSERT INTO "{self.schema}"."activity" ("key", "name", "location",
             "type", "unit", "version", "comment") VALUES (%(key)s, %(name)s, %(location)s, %(type)s,
             %(unit)s, %(version)s, %(comment)s);"""

            sql_activity_database = f"""INSERT INTO "{self.schema}"."activity_database"
            ("key", "database") VALUES (%(key)s, %(database)s);"""
            kvals_activity_database = {'key': key,
                                       'database': db
                                       }

            with self.conn.cursor() as cur:
                cur.execute(sql_activity, kvals_activity)
                cur.execute(sql_activity_database, kvals_activity_database)
        elif version_activity_remote < kvals_activity['version']:
            sql_activity = f"""UPDATE "{self.schema}"."activity" SET "name" = %(name)s,
             "location" = %(location)s, "type" = %(type)s, "unit" = %(unit)s, 
             "version" = %(version)s, "comment" = %(comment)s WHERE "key" = %(key)s;"""

            with self.conn.cursor() as cur:
                cur.execute(sql_activity, kvals_activity)
        elif version_activity_remote > kvals_activity['version']:
            kvals = {'key': kvals_activity['key'],
                     'version': kvals_activity['version'],
                     'remote_version': version_activity_remote}
            LOGGER.warning('local activity %(key)s version %(version)s is'
                           ' superseded by remote version'
                           ' %(remote_version)s' % kvals)

        for exchange in activity.exchanges():
            exchange_db, exchange_key = exchange.get('input')
            version_exchange_local = exchange.get('version')

            if not self.remote_activity_exists(key=exchange_key,
                                               db=db,
                                               version=version_exchange_local):
                # add remote activity
                self.add_remote_activity(exchange)

            kvals = {'key': key,
                     'amount': exchange.get('amount'),
                     'input': exchange_key,
                     'uncertainty_type': exchange.get('uncertainty_type')
                     }

            # get exchange version
            sql_exchange_version = f"""SELECT "version" FROM "{self.schema}"."exchange"
             WHERE "key" = %(key)s AND "input" = %(input)s;"""

            with self.conn.cursor() as cur:
                cur.execute(sql_exchange_version, kvals)

                try:
                    version_exchange_remote = cur.fetchone()[0]
                except TypeError:
                    version_exchange_remote = None

            if version_exchange_remote is None:
                self.add_remote_exchange(exchange=kvals)
            elif version_exchange_remote < version_exchange_local:
                self.update_remote_exchange(key=key, local_exchange=exchange, remote_exchange=kvals)
            elif version_exchange_remote > version_exchange_local:
                kvals_exchange = {'activity_key': kvals_activity['key'],
                                  'exchange_key': exchange_key,
                                  'local_version': version_exchange_local,
                                  'remote_version': version_exchange_remote}
                LOGGER.warning('local exchange between %(activity_key)s and %(exchange_key)s with'
                               ' version %(local_version)s is superseded by remote version '
                               '%(remote_version)s'
                               % kvals_exchange)
            else:
                continue

            # # if not self.remote_activity_exists(key=exchange_key,
            # db=db, version=version_exchange_local):
            # # get exchange version
            # sql_exchange_version = f"""SELECT "version" FROM "{self.schema}"."exchange"
            #  WHERE "key" = %(key)s AND "input" = %(input_key)s;"""
            #
            # kvals = {'key': kvals_activity['key'],
            #          'input_key': exchange_key}
            #
            # with self.conn.cursor() as cur:
            #     cur.execute(sql_exchange_version, kvals)
            #
            #     try:
            #         version_exchange_remote = cur.fetchone()[0]
            #     except TypeError:
            #         version_exchange_remote = None
            #
            # if not version_exchange_remote:
            #     self.add_remote_exchange(exchange)
            # elif version_exchange_remote < version_exchange_local:
            #     kvals = {'key': key,
            #              'amount': exchange.get('amount'),
            #              'input': exchange_key,
            #              'uncertainty_type': exchange.get('uncertainty_type')
            #              }
            #     if not self.remote_exchange_exists(exchange=kvals):
            #         self.add_remote_exchange(exchange=kvals)
            # else:
            #     kvals_exchange = {'activity_key': kvals_activity['key'],
            #                       'exchange_key': exchange_key,
            #                       'local_version': version_exchange_local,
            #                       'remote_version': version_exchange_remote}
            #     LOGGER.warning('exchange between %(activity_key)s and %(exchange_key)s with
            #     version'
            #                    ' %(local_version)s is superseded by remote version
            #                    %(remote_version)s'
            #                    % kvals_exchange)

            self.conn.commit()

    def add_remote_exchange(self, exchange: dict):
        """
        Add <exchange> to <self.schema>.exchange
        :param exchange: dict exchange kvals
        :return:
        """

        sql = f"""INSERT INTO "{self.schema}"."exchange" ("key", "amount", "input",
         "uncertainty_type") VALUES (%(key)s, %(amount)s, %(input)s, %(uncertainty_type)s);"""

        u_type = exchange.get('uncertainty_type') or Sync.DEFAULT_UNCERTAINTY_TYPE

        kvals = {'key': exchange.get('key'),
                 'amount': exchange.get('amount'),
                 'input': exchange.get('input'),
                 'uncertainty_type': u_type
                 }

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(sql, kvals)

        self.conn.commit()

    def add_local_activity(self, activity: dict):
        """
        Add <activity> to self.database
        :param activity: dict
        :return:
        """

        key = activity.pop('key')

        if self.local_activity_exists(key=key):
            _activity = self.database.get(key)

            if _activity['version'] < activity['version']:
                _activity.delete()
        if not self.local_activity_exists(key=key):
            _activity = self.database.new_activity(code=key, **activity)
            _activity.save()

        for exchange in self.get_remote_exchanges(key=key):
            exchange_key = exchange.get('input')[1]

            try:
                assert key != exchange_key
            except AssertionError:
                raise ValueError(f"activity {key} has itself as an exchange")

            # if self.local_activity_exists(key=exchange_key):
            #     old_exchange_activity = self.database.get(exchange_key)
            #     if old_exchange_activity['version'] < exchange['version']:
            #         old_exchange_activity.delete()

            if not self.local_activity_exists(key=exchange_key):
                exchange_activity = self.get_remote_activity(key=exchange_key)

                try:
                    self.add_local_activity(activity=exchange_activity)
                except RecursionError:
                    raise Exception(f"activity {key} has itself as an exchange")

            if not self.local_exchange_exists(exchange=exchange, key=key):
                new_exchange = _activity.new_exchange(**exchange)
                new_exchange.save()
            else:
                new_exchange_version = exchange['version']

                for _exchange in _activity.exchanges():
                    if _exchange.get('input')[1] == exchange.get('input') and \
                            _exchange.get('amount') == exchange.get('amount') and \
                            _exchange.get('unit') == exchange.get('unit') and \
                            _exchange.get('type') == exchange.get('type'):
                        old_exchange_version = _exchange.get('version')

                        if old_exchange_version < new_exchange_version:
                            _exchange.delete()
                            new_exchange = _activity.new_exchange(**exchange)
                            new_exchange.save()
                        elif old_exchange_version > new_exchange_version:
                            LOGGER.warning(f'remote exchange {exchange_key} for activity '
                                           f'{_activity.key} version {new_exchange_version} is'
                                           f' superseded by local exchange version'
                                           f' {old_exchange_version}')
                        else:
                            pass

                        break

        _activity.save()

    def sync(self, local_to_remote=True, remote_to_local=True):
        """
        Synchronize local and remote databases
        :param local_to_remote: bool push local differences to remote
        :param remote_to_local: bool pull remote differences into local
        :return:
        """
        if local_to_remote:
            for activity in self.get_local_activities():
                print(activity.key)
                self.add_remote_activity(activity=activity)

        if remote_to_local:
            for activity in self.get_remote_activities():
                self.add_local_activity(activity=activity)


if __name__ == '__main__':

    bw.projects.set_current('ethanol_LCA_test')

    database = 'EM_LCA_0'

    db = bw.Database(database)

    if RESET_LOCAL_DB:
        db.write({
            (database, "Electricity production"): {
                'name': 'Electricity production',
                'type': 'production',
                'location': 'GLO',
                'unit': 'kWh',
                'version': 0,
                'exchanges': [{
                    'input': (database, 'Fuel production'),
                    'amount': 2,
                    'unit': 'kg',
                    'type': 'technosphere',
                    'version': 0
                }, {
                    'input': (database, 'Carbon dioxide'),
                    'amount': 1,
                    'unit': 'kg',
                    'type': 'biosphere',
                    'version': 0
                }, {
                    'input': (database, 'Sulphur dioxide'),
                    'amount': 0.1,
                    'unit': 'kg',
                    'type': 'biosphere',
                    'version': 0
                }]
            },
            (database, 'Fuel production'): {
                'name': 'Fuel production',
                'type': 'production',
                'location': 'GLO',
                'unit': 'kg',
                'version': 0,
                'exchanges': [{
                    'input': (database, 'Carbon dioxide'),
                    'amount': 10,
                    'unit': 'kg',
                    'type': 'biosphere',
                    'version': 0
                }, {
                    'input': (database, 'Sulphur dioxide'),
                    'amount': 2,
                    'unit': 'kg',
                    'type': 'biosphere',
                    'version': 0
                }, {
                    'input': (database, 'Crude oil'),
                    'amount': -50,
                    'unit': 'kg',
                    'type': 'biosphere',
                    'version': 0
                }]
            },
            (database, 'Carbon dioxide'): {'name': 'Carbon dioxide',
                                           'type': 'biosphere',
                                           'location': 'GLO',
                                           'unit': 'kg',
                                           'version': 0
                                           },
            (database, 'Sulphur dioxide'): {'name': 'Sulphur dioxide',
                                            'location': 'GLO',
                                            'unit': 'kg',
                                            'type': 'biosphere',
                                            'version': 0},
            (database, 'Crude oil'): {'name': 'Crude oil',
                                      'location': 'GLO',
                                      'unit': 'kg',
                                      'type': 'biosphere',
                                      'version': 0}
        })

    conn = psycopg2.connect(host='walter.nrel.gov', dbname='em_lca')

    LOGGER.debug('before sync')
    for activity in db:
        LOGGER.debug(f"{activity} [version: {activity['version']}]")
        for exchange in activity.exchanges():
            LOGGER.debug(f"\t {exchange} [version: {exchange['version']}]")

    sync = Sync(conn=conn, database=db, schema='em_lca')

    sync.sync(local_to_remote=True, remote_to_local=True)

    LOGGER.debug('\nafter sync')
    for activity in db:
        LOGGER.debug(f"{activity} [version: {activity['version']}]")
        for exchange in activity.exchanges():
            LOGGER.debug(f"\t {exchange} [version: {exchange['version']}]")
