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
        sql_update = """UPDATE "em_lca"."activity" SET "version" = 1
         WHERE "key" IN ('Fuel production', 'Electricity production')"""

        sql_insert_activity = """INSERT INTO "em_lca"."activity" ("key", "name", "location", "type",
         "unit", "version", "comment") VALUES (7, 'production 2', 'GLO', 'production', 'kilogram',
          0, 'production activity 2');"""

        sql_insert_activity_database = """INSERT INTO "em_lca"."activity_database"
         ("key", "database") VALUES (7, 'EM_LCA_0');"""

        sql_update_exchange_version = """UPDATE "em_lca"."exchange" SET "version" = 1
         WHERE "key" = '1' AND "input" = '2';"""

        with conn.cursor() as cur:
            for sql in [sql_update, sql_insert_activity, sql_insert_activity_database,
                        sql_update_exchange_version]:
                cur.execute(sql)
