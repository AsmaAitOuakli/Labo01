import snowflake.connector
import streamlit as st
def connect_to_snowflake(user, password, account):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    return conn

def list_warehouses(conn):
    cur = conn.cursor()
    cur.execute("SHOW WAREHOUSES")
    data = cur.fetchall()
    cur.close()
    return data

def list_databases(conn):
    cur = conn.cursor()
    cur.execute("SHOW DATABASES")
    data = cur.fetchall()
    cur.close()
    return data

def list_schemas(conn, database):
    cur = conn.cursor()
    cur.execute(f"SHOW SCHEMAS IN DATABASE {database}")
    data = cur.fetchall()
    cur.close()
    return data

def list_tables(conn, schema, database):
    cur = conn.cursor()
    cur.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
    data = cur.fetchall()
    cur.close()
    return data

def fetch_table_data(conn, database, schema, table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {database}.{schema}.{table_name}")
    data = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]  
    cur.close()
    return column_names, data

# def insert_row(conn, table, values, database, schema):
#     cur = conn.cursor()
#     cur.execute(f"INSERT INTO {database}.{schema}.{table} VALUES ({values});")
#     cur.close()
def insert_row(conn, table, values, database, schema):
    cur = conn.cursor()
    try:
        cur.execute(f"INSERT INTO {database}.{schema}.{table} VALUES ({values});")
        conn.commit()
        st.success("Ligne insérée avec succès!")
    except Exception as e:
        conn.rollback()
        st.error(f"Erreur lors de l'insertion de la ligne : {e}")
    finally:
        cur.close()

def update_row(conn, table, set_values, condition, database, schema):
    cur = conn.cursor()
    cur.execute(f"UPDATE {database}.{schema}.{table}  SET {set_values} WHERE {condition};")
    cur.close()

def delete_row(conn, table, condition, database, schema):
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {database}.{schema}.{table}  WHERE {condition};")
    cur.close()
