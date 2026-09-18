import duckdb

def get_minio_connection(pointer=None):
    """
    Establishes a connection to the MinIO server using DuckDB.

    Returns:
        duckdb.DuckDBPyConnection: A DuckDB connection object.
    """
    # Connect to DuckDB and creates in-memory database
    con = duckdb.connect(":memory:" if pointer is None else pointer)
    
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Set the MinIO connection parameters and config the duckdb to use for s3 api compatibile storage
    con.execute("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_url_style='path';
        SET s3_use_ssl=false;        
    """)
        # s3_use_ssl=false cus we is  http for https for connection.
    return con