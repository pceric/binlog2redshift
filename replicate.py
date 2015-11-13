#!/usr/bin/env python

import argparse
import json
import psycopg2
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)

MYSQL_SETTINGS = {
    "host": "localhost",
    "port": 3306,
    "user": "user",
    "passwd": "pass"
}

def main(args):
    with open('checkpoint', 'a+b') as checkpoint:
        try:
            start = json.load(checkpoint)
        except:
            start = []

        conn = psycopg2.connect(database="db1", user="user", password="pass", host="redshift.amazonaws.com", port="5439")
        cur = conn.cursor()

        stream = BinLogStreamReader(
            connection_settings=MYSQL_SETTINGS,
            server_id=128,
            resume_stream=True,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            log_file=start[0] if start else args.log_file[0],
            log_pos=start[1] if start else None,
            only_schemas=args.schemas[0].split(',') if args.schemas[0] else None)

        print "Starting...", stream.log_file, stream.log_pos

        count = 1
        for binlogevent in stream:
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    vals = row["values"]
                    escaped = ' AND '.join(x+'=%s' for x in vals.keys())
                    #print "DELETE FROM {0}.{1} WHERE {2}".format(binlogevent.schema, binlogevent.table, escaped), vals.values()
                    cur.execute("DELETE FROM {0}.{1} WHERE {2}".format(binlogevent.schema, binlogevent.table, escaped), vals.values())
                elif isinstance(binlogevent, UpdateRowsEvent):
                    vals = row["after_values"]
                    escaped = ','.join('%s' for x in vals.keys())
                    #print "UPDATE {0}.{1} SET ({2}) = ({3})".format(binlogevent.schema, binlogevent.table, str(','.join(vals.keys())), escaped), vals.values()
                    cur.execute("UPDATE {0}.{1} SET ({2}) = ({3})".format(binlogevent.schema, binlogevent.table, str(','.join(vals.keys())), escaped), vals.values())
                elif isinstance(binlogevent, WriteRowsEvent):
                    vals = row["values"]
                    escaped = ','.join('%s' for x in vals.keys())
                    #print "INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(binlogevent.schema, binlogevent.table, str(','.join(vals.keys())), escaped), vals.values()
                    cur.execute("INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(binlogevent.schema, binlogevent.table, str(','.join(vals.keys())), escaped), vals.values())
            if count == 100:
                conn.commit()
                checkpoint.truncate(0)
                json.dump([stream.log_file, stream.log_pos], checkpoint)
                count = 1
            else:
                count += 1

        stream.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--schemas',
                        nargs=1,
                        default=[None],
                        dest='schemas',
                        help="MySQL schemas to replicate; comma seperated")
    parser.add_argument('--log_file',
                        nargs=1,
                        default=[None],
                        dest='log_file',
                        help="MySQL bin changelog file to start at (e.g. mysql-bin-changelog.001234)")
    args = parser.parse_args()
    main(args)
