import pandas as pd
import pandas.io.sql as sqlio
import psycopg2

# Conection parameters
host='localhost'
port='5432'
dbname='prevdb'
user='prevdb_user'
pwd='pr3v'
dbtable='fato_auxilio_sample'

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
        conn = \
            psycopg2.connect("host='{}' port={} dbname='{}'user={} password={}"
                .format(host, port, dbname, user, pwd))
        return conn
    except:
            return -1

def get_aposentadorias(db_conn, table='fato_auxilio', \
        tipo = ['idade', 'tempo', 'invalidez', 'especial']):
    """
        Query database and return Pandas Dataframe containing 'aposentadorias'
    of type 'tipo'

    db_conn:    db connection
    tipo:       List of types desired: default = ['idade', 'tempo', 'invalidez',
                                                    'especial']
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
    WHERE COD IN ({dtype_list})
    """.format(dtype_list = ", ".join(map(str, dtype)))
    return sqlio.read_sql_query(sql, conn)

def check_age_eligibility(idade, sexo, clientela, tempo_de_contribuicao):
    """
        Check if current registry is eligible for age retirement.

    idade:
    sexo:
    clientela:
    tempo_de_contribuicao:

    Returs True if elegible, and False otherwise
    """
    if tempo_de_contribuicao < 15:
        return False

    elif clientela == 1: # Urbana
        if sexo == 1:       # Masculino
            if  idade < 65:
                return False
            else:
                return True
        elif sexo == 3:     # Feminino
            if  idade < 60:
                return False
            else:
                return True
        else:                # Ignorado
            return False # unable to determine eligibility

    elif clientela == 2: # Rural
        if sexo == 1:       # Masculino
            if  idade < 60:
                return False
            else:
                return True
        elif sexo == 3:     # Feminino
            if  idade < 55:
                return False
            else:
                return True
        else:               # Ignorado
            return False # unable to determine eligibility

    elif clientela == 9: # Ignorada
        return False # unable to determine eligibility


# main
# Connect to an existing database
conn = db_connection(host, port, dbname, user, pwd)
if conn == -1:
    print("Unable to connect to the database")
    exit(-1)

# Query the database and obtain data as Python objects
df = get_aposentadorias(conn, dbtable, tipo = ['idade'])
print(df.head())
print(df.index)
print(df.columns)

print("Checking age retirement eligibility")
for i in range(10):
    idade = df.loc[i]['idade_dib']
    sexo = df.loc[i]['sexo']
    clientela = df.loc[i]['clientela']
    tempo_de_contribuicao = df.loc[i]['tempo_contrib']
    print(" Idade: {},\tSexo: {},\tClientela: {},\t" \
        "Tempo de Contrib: {}.\tResultado: {}" \
        .format(idade, sexo, clientela, tempo_de_contribuicao, \
        check_age_eligibility(idade,sexo,clientela, tempo_de_contribuicao)))

# Close communication with the database
conn.close()
