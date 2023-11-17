import azure.functions as func
import logging
import psycopg2


def connect_to_postgres():
    try:
        # Establish a connection
        conn = psycopg2.connect(
        database="IRIS",
        user="md",
        password="AVNS_iYThQ9SOCuzGj2-H92d",
        host="iris-iot-dev-royalihc-cff9.a.timescaledb.io",
        port="29268"
        )

        # Create a cursor
        cursor = conn.cursor()

        logging.info("Connected to the PostgreSQL database.")

        return conn, cursor

    except Exception as e:
        logging.error(f"Error: Unable to connect to the database\n{e}")
        return None, None


def check_names_in_tables():
    # Connect to the database
    conn, cursor = connect_to_postgres()

    if conn is None or cursor is None:
        return None

    try:
        # Fetch all distinct vessel number values from the 'vessel_company_prod' table
        cursor.execute("select vessel_no from vessel_company_prod;")
        vessel_names = {name[0] for name in cursor.fetchall()}

        # Fetch all table names in the 'public' schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        all_table_names = {table[0] for table in cursor.fetchall()}

        # Check if each 'name' value is present as a table name using set difference
        names_not_in_tables = vessel_names - all_table_names

        for name in names_not_in_tables:
            logging.info(f"Table with name '{name}' does not exist in the 'public' schema.")

            create_table_query = f"""
                CREATE TABLE public."{name}" (
                    date TIMESTAMP WITH TIME ZONE,
                    tagname text,
                    val DOUBLE PRECISION
                );
            """
            logging.info(create_table_query)
            cursor.execute(create_table_query)

            # Commit inside the loop to ensure each table is committed separately
            conn.commit()

            logging.info(f"Table '{name}' created successfully.")

        return vessel_names

    except Exception as e:
        logging.error(f"Error: Unable to check names in tables\n{e}")
        return None

    finally:
        cursor.close()
        conn.close()
        logging.info("Connection closed.")


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="table_alert")
def table_alert(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    check_names_in_tables()

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
