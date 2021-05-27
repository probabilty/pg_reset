import psycopg2


def remove_content(conn, tablename):
    stmt = """
        TRUNCATE {} CASCADE;
    """.format(tablename)
    cur = conn.cursor()
    try:
        cur.execute(stmt)
    except Exception as e:
        pass
    cur.close()


def drop_partition(conn, parent_name):
    stmt = """
        DROP TABLE {} CASCADE;
    """.format(parent_name)
    cur = conn.cursor()
    cur.execute(stmt)
    commit(conn)
    cur.close()


def get_relations(conn, schema_name):
    stmt = """
    SELECT tablename
    FROM pg_catalog.pg_tables
    WHERE
    schemaname ='{}';
    """.format(schema_name)
    cur = conn.cursor()
    cur.execute(stmt)
    row = cur.fetchone()
    while row is not None:
        if len(row) > 0:
            yield row[0]
            row = cur.fetchone()
    cur.close()


def init_connection(host,port,user,password,database):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        port=port,
        password=password)
    return conn


def get_partions(conn, tablename):
    stmt = """
    SELECT
    child.relname       AS child
    FROM pg_inherits
        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
        JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
    WHERE parent.relname='{}';""".format(tablename)
    cur = conn.cursor()
    cur.execute(stmt)
    row = cur.fetchone()
    while row is not None:
        if len(row) > 0:
            yield row[0]
            row = cur.fetchone()
    cur.close()


def commit(connection):
    connection.commit()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--server", type=str, default="127.0.0.1",
                    help="database server address")
    parser.add_argument("-p","--port", type=str, default="5432",
                help="database port")
    parser.add_argument("-u","--user", type=str,default="postgres",
            help="database user")
    parser.add_argument("-w","--password", type=str,default="",
        help="database password")
    parser.add_argument("-d","--database", type=str,default="app",
    help="database name")
    args = parser.parse_args()
    conn = init_connection(args.server,args.port,args.user,args.password,args.database)
    print(conn)
    for relation in get_relations(conn, "public"):
        remove_content(conn, relation)
        for partion in get_partions(conn, relation):
            drop_partition(conn,partion)
    commit(conn)
if __name__ == "__main__":
    main()