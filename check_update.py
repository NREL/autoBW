"""Script to report activity updates for testing Sync"""

import brightway2 as bw
import psycopg2
from pandas import DataFrame
from psycopg2.extras import DictCursor
from peewee import DoesNotExist


if __name__ == '__main__':
    bw.projects.set_current('ethanol_LCA_test')
    db = bw.Database('EM_LCA_0')

    with psycopg2.connect(host='walter.nrel.gov', dbname='em_lca') as conn:
        SQL = """SELECT "key", "version" FROM "em_lca"."activity"
         WHERE "key" IN ('1', '2', '7', 'Fuel production', 'Electricity production',
          'Carbon dioxide');"""

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(SQL)

            versions = [dict(result) for result in cur.fetchall()]

    kvals = {'key': [],
             'local_version': [],
             'remote_version': []
             }

    for version in versions:
        kvals['key'].append(version['key'])

        try:
            _key = db.get(version['key'])
        except DoesNotExist:
            if version['key'] == '7':
                kvals['local_version'].append(None)
            else:
                raise
        else:
            kvals['local_version'].append(_key['version'])

        kvals['remote_version'].append(version['version'])

    print(DataFrame(kvals).set_index('key'))

    for key in ('1', '2', '7', 'Fuel production', 'Electricity production', 'Carbon dioxide'):
        local_exchanges = []
        print(key)
        try:
            activity = db.get(key)
        except DoesNotExist:
            if key == '7':
                pass
            else:
                raise
        else:
            for exchange in activity.exchanges():
                local_exchanges.append({"key": exchange.input[1],
                                        "version": exchange.get("version")})

        print(f'\tlocal: {local_exchanges}')
        SQL = """SELECT "input", "version" FROM "em_lca"."exchange" WHERE "key" = %(key)s;"""
        with psycopg2.connect(host='walter.nrel.gov', dbname='em_lca') as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(SQL, {'key': key})
                remote_exchanges = [dict(res) for res in cur.fetchall()]
        print(f'\tremote: {remote_exchanges}')
