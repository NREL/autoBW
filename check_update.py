"""Script to report activity updates for testing Sync."""

import brightway2 as bw
from pandas import DataFrame
from peewee import DoesNotExist

import psycopg2
from psycopg2.extras import DictCursor

if __name__ == "__main__":
    bw.projects.set_current("ethanol_LCA_test")
    DB = bw.Database("EM_LCA_0")

    with psycopg2.connect(host="walter.nrel.gov", dbname="em_lca") as conn:
        SQL = """SELECT "key", "version" FROM "em_lca"."activity"
         WHERE "key" IN ('1', '2', '7', 'Fuel production', 'Electricity production',
          'Carbon dioxide');"""

        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(SQL)

            VERSIONS = [dict(result) for result in cur.fetchall()]

    KVALS = {"key": [], "local_version": [], "remote_version": []}

    for version in VERSIONS:
        KVALS["key"].append(version["key"])

        try:
            _key = DB.get(version["key"])
        except DoesNotExist:
            if version["key"] == "7":
                KVALS["local_version"].append(None)
            else:
                raise
        else:
            KVALS["local_version"].append(_key["version"])

        KVALS["remote_version"].append(version["version"])

    print(DataFrame(KVALS).set_index("key"))

    for key in (
        "1",
        "2",
        "7",
        "Fuel production",
        "Electricity production",
        "Carbon dioxide",
    ):
        local_exchanges = []
        print(key)
        try:
            activity = DB.get(key)
        except DoesNotExist:
            if key == "7":
                pass
            else:
                raise
        else:
            for exchange in activity.exchanges():
                local_exchanges.append(
                    {"key": exchange.input[1], "version": exchange.get("version")}
                )

        print(f"\tlocal: {local_exchanges}")
        SQL = """SELECT "input", "version" FROM "em_lca"."exchange" WHERE "key" = %(key)s;"""
        with psycopg2.connect(host="walter.nrel.gov", dbname="em_lca") as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(SQL, {"key": key})
                remote_exchanges = [dict(res) for res in cur.fetchall()]
        print(f"\tremote: {remote_exchanges}")
