"""Script to update local and remote activities for testing Synch"""

import brightway2 as bw
import psycopg2

if __name__ == '__main__':
    bw.projects.set_current('ethanol_LCA_test')
    db = bw.Database('EM_LCA_0')

    # update local activity version
    for activity_key in ['1', '2']:
        activity = db.get(activity_key)
        activity['version'] = 1
        activity.save()

    # add new local activity

    # update local exchange version
    activity = db.get('Carbon dioxide')
    for exchange in activity.exchanges():
        if exchange.get('input') == ('other_database', '1') and\
                exchange.get('amount') == 10 and\
                exchange.get('unit') == 'gallon' and\
                exchange.get('type') == 'production' and\
                exchange.get('version') == 0:

            _exchange = exchange

            exchange.delete()

            _exchange['version'] = 1
            new_exchange = activity.new_exchange(**_exchange)
            new_exchange.save()

    # update and add remote activities
    with psycopg2.connect(host='walter.nrel.gov', dbname='em_lca') as conn:
        SQL_UPDATE = """UPDATE "em_lca"."activity" SET "version" = 1
         WHERE "key" IN ('Fuel production', 'Electricity production')"""

        SQL_INSERT_ACTIVITY = """INSERT INTO "em_lca"."activity" ("key", "name", "location", "type",
         "unit", "version", "comment") VALUES (7, 'production 2', 'GLO', 'production', 'kilogram',
          0, 'production activity 2');"""

        SQL_INSERT_ACTIVITY_DATABASE = """INSERT INTO "em_lca"."activity_database"
         ("key", "database") VALUES (7, 'EM_LCA_0');"""

        SQL_UPDATE_EXCHANGE_VERSION = """UPDATE "em_lca"."exchange" SET "version" = 1
         WHERE "key" = '1' AND "input" = '2';"""

        with conn.cursor() as cur:
            for sql in [SQL_UPDATE, SQL_INSERT_ACTIVITY, SQL_INSERT_ACTIVITY_DATABASE,
                        SQL_UPDATE_EXCHANGE_VERSION]:
                cur.execute(sql)
