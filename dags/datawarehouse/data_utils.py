from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table ="yt_elt"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt",database = "elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor
#    cursor.execute ("SELECT * FROM TABLE X")

def close_conn_cursor(conn,cursor):
    conn.close()
    cursor.close()

def create_schema(schema):
    conn,cursor = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cursor.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn,cursor)


def create_table(schema):
    conn, cursor = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
            """
    cursor.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)

def get_video_ids(cursor, schema):
    cursor.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cursor.fetchall()

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids