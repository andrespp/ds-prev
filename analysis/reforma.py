import pandas as pd
import pandas.io.sql as sqlio
import psycopg2

# Conection parameters
host='localhost'
port='5432'
dbname='prevdb'
user='prevdb_user'
pwd='pr3v'
table='fato_auxilio_sample'

def db_connection(host, port, dbname, user, pwd):
    """
    Connect to PostgreSQL database

    host:       server name or ip address
    port:       server port 
    dbname:     database name
    user:       database user
    pwd:        database user's password

    Return connection or -1 on error
    """
    try:
        conn = psycopg2.connect("host='{}' port={} dbname='{}'user={} password={}"
                .format(host, port, dbname, user, pwd))
        return conn
    except:
            return -1

def get_aposentadorias(db_conn, table='fato_auxilio', \
        tipo = ['idade', 'tempo', 'invalidez', 'especial']):
    """
    Return Pandas Dataframe containing 'aposentadorias' of type 'tipo' 

    db_conn:    db connection
    tipo:       List of types desired: default = ['idade', 'tempo', 'invalidez', 'especial']
    """

    # Retrieve desired types
    dtype = []
    if 'idade' in tipo:
        dtype.append(41)
    if 'tempo' in tipo:
        dtype.append(42)
    if 'invalidez' in tipo:
        dtype.append(32)
    if 'especial' in tipo:
        dtype.append(46)
        dtype.append(57)
        dtype.append(92)

    # Query the database and obtain data as Python objects
    sql = """
    SELECT
        ESPECIE, DIB, DDB, MOT_CESSACAO, ULT_COMPET_MR, VL_MR, 
        DT_NASC, VL_RMI, CLIENTELA, SEXO, SITUACAO, DT_OBITO, 
        IDADE_DIB, TEMPO_CONTRIB
    FROM FATO_AUXILIO_SAMPLE
    INNER JOIN DIM_ESPECIE ON FATO_AUXILIO_SAMPLE.ESPECIE = DIM_ESPECIE.COD
    WHERE COD IN (41) ## TODO
    """
    dat = sqlio.read_sql_query(sql, conn)

# main
# Connect to an existing database
conn = db_connection(host, port, dbname, user, pwd)
if conn == -1:
    print("Unable to connect to the database")
    exit(-1)

# Query the database and obtain data as Python objects
sql = "SELECT * FROM DIM_SEXO;"
dat = sqlio.read_sql_query(sql, conn)
print(dat)

# Close communication with the database
conn.close()
