import pandas as pd
import streamlit as st
from snowflakecon import connect_to_snowflake, list_databases, list_schemas, list_tables, fetch_table_data,delete_row, update_row, insert_row,list_warehouses
# import plotly.express as px

def main():
    st.title("Dashboard Snowflake avec Streamlit")

    # Formulaire de connexion
    st.sidebar.header("Connexion à Snowflake")
    user = st.sidebar.text_input("Utilisateur")
    password = st.sidebar.text_input("Mot de passe", type="password")
    account = st.sidebar.text_input("Compte")

    if st.sidebar.button("Se connecter"):
        conn = connect_to_snowflake(user, password, account)
        st.sidebar.success("Connexion reussie!")
        st.session_state.conn = conn

    if 'conn' in st.session_state:
        conn = st.session_state.conn

        st.header("Gestion des Datawarehouses")
        if st.button("Lister les Datawarehouses"):
            datawarehouses = list_warehouses(conn)
            st.write(datawarehouses)

        st.header("Gestion des Bases de Donnees")
        if st.button("Lister les Bases de Donnees"):
            databases = list_databases(conn)
            st.session_state.databases = databases
            st.write(databases)

        if 'databases' in st.session_state:
            selected_db = st.selectbox("Selectionner une base de donnees", [db[1] for db in st.session_state.databases])

            if st.button("Lister les Schemas"):
                schemas = list_schemas(conn, selected_db)
                st.session_state.schemas = schemas
                st.write(schemas)

        if 'schemas' in st.session_state:
            selected_schema = st.selectbox("Selectionner un schema", [schema[1] for schema in st.session_state.schemas])

            if st.button("Lister les Tables"):
                tables = list_tables(conn, selected_schema, selected_db)
                for table_name in tables:
                    st.subheader(f"Table: {table_name[1]}")
                    column_names, table_data = fetch_table_data(conn, selected_db, selected_schema, table_name[1])
                    df = pd.DataFrame(table_data, columns=column_names)
                    st.dataframe(df)
            st.header("Operations CRUD sur les Tables")
            tables = list_tables(conn, selected_schema, selected_db)
            selected_table = st.selectbox("Selectionner une table", [table[1] for table in tables])

            st.subheader("Inserer une Ligne")
            values = st.text_input("Valeurs (format: 'val1', 'val2', ...)")
            if st.button("Inserer"):
                insert_row(conn, selected_table, values, selected_db,selected_schema)  
            st.subheader("Mettre a Jour une Ligne")
            set_values = st.text_input("Nouvelles valeurs (format: col1='val1', ...)")
            condition = st.text_input("Condition (format: col='val')")
            if st.button("Mettre a Jour"):
                    update_row(conn, selected_table, set_values, condition,selected_db,selected_schema)
                    st.success("Ligne mise a jour!")

            st.subheader("Supprimer une Ligne")
            del_condition = st.text_input("Condition (format: col='val')", key="delete_condition")
            if st.button("Supprimer"):
                delete_row(conn, selected_table, del_condition,selected_db,selected_schema)
                st.success("Ligne supprimee!")

if __name__ == '__main__':
    main()
