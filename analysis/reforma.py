import numpy as np
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
#dbtable='fato_auxilio'
chunk_size=5000

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

def get_year_from_date_sk(date_sk):
    """
        Return year from integer date in form YYYYMMDD.

    date_sk: Integer date in form YYYYMMDD
    """
    return int(date_sk / 10000)

def get_min_contribution_time(dib, tipo='aposentadoria'):
    """
        Return minimal contribution time, considering special cases, given the
    benefit initial date.

    dib:    benefit initial date

    ---------------------------------------------------------------------------
    Base Legal legal - Casos Epeciais:
    * Aposentadorias
        A carência das Aposentadorias (exceto por invalidez) poderá ser menor
    do que 180 contribuições, conforme está previsto no artigo 142 da Lei
    nº 8.213/91, mas apenas para o cidadão que se filiou à Previdência Social
    até 24/07/1991 (trabalhador urbano ou rural, exceto segurado especial) e
    começou a contagem de tempo para efeito de carência.
    """

    if tipo != 'aposentadoria': # TODO
        print('Not implemented')
        return -1

    contrib_arr = np.array ( \
        [[2011, 180],
         [2010, 174],
         [2009, 168],
         [2008, 162],
         [2007, 156],
         [2006, 150],
         [2005, 144],
         [2004, 138],
         [2003, 132],
         [2002, 126],
         [2001, 120],
         [2000, 114],
         [1999, 108],
         [1998, 102],
         [1997, 96],
         [1996, 90],
         [1995, 78],
         [1994, 72],
         [1993, 66],
         [1992, 60],
         [1991, 60]])

    contribution_df = pd.DataFrame(contrib_arr[:,1], index = contrib_arr[:,0], \
                                    columns = ['meses'])
    contribution_df['anos'] = contribution_df['meses'] / 12

    dib_year = get_year_from_date_sk(int(dib))

    if 1991 < dib_year < 2012:
        return contribution_df.loc[dib_year,'anos']
    else:
        return 15

def check_applicable_retirement_rules(idade, sexo, clientela, dib, \
                                    tempo_de_contribuicao):
    """
        Check retirement rules aplicable for a given registry.
    """
    #TODO

def check_contrib_time_eligibility(idade, sexo, clientela, dib, \
                                    tempo_de_contribuicao):
    """
        Check if current registry is eligible for contribution time
    retirement (retirements of types 42 / 46 / 57).

    idade:
    sexo:
    clientela:
    dib:
    tempo_de_contribuicao:

    Returs True if elegible by any contribution time rule, and False
    otherwise
    """

    # Regra 30/35 anos de contribuição
    if tempo_de_contribuicao == 999:
        pass
    elif sexo == 3 and tempo_de_contribuicao >= 30: # Feminino
        return True
    elif sexo == 1 and tempo_de_contribuicao >= 35: # Masculino
        return True

    # Regra 80/95 progressiva
    if tempo_de_contribuicao == 999:
        pass
    elif sexo == 3 \
      and (idade + tempo_de_contribuicao) >= 85: # Feminino
        return True
    elif sexo == 1 \
      and (idade + tempo_de_contribuicao) >= 95: # Masculino
        return True

    # Regra Proporcional
    if sexo == 3 \
      and idade >= 48 \
      and tempo_de_contribuicao >= 25: # Feminino
        return True
    if sexo == 1 \
      and idade >= 53 \
      and tempo_de_contribuicao >= 30: # Feminino
        return True

    return False

def check_age_eligibility(idade, sexo, clientela, dib, \
                            tempo_de_contribuicao):
    """
        Check if current registry is eligible for age retirement (retirement
    of type 41).

    idade:
    sexo:
    clientela:
    dib:
    tempo_de_contribuicao:

    Returs True if elegible, and False otherwise
    """
    if idade == 999:
        return False

    if tempo_de_contribuicao < get_min_contribution_time(dib):
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

def check_pec_287(idade, sexo, dib, tempo_de_contribuicao):
    """
        Check if current registry is elegible for retirement accordingly to
    the PEC 287/2016's criterias
    idade:
    sexo:
    dib:
    tempo_de_contribuicao:

    Returs True if elegible, and False otherwise
    """

    if idade == 999 or tempo_de_contribuicao == 999:
        return False

    if tempo_de_contribuicao < get_min_contribution_time(dib):
        return False

    if sexo == 3 and idade >= 62: # Feminino
        return True
    elif sexo == 1 and idade >= 65: # Feminino
        return True
    else:
        return False

def check_pec_287_df(df):
    """
        Check if current registry is elegible for retirement accordingly
    to the PEC 287/2016's criterias.

    df: Pandas dataframe containing columns IDADE_DIB, TEMPO_CONTRIB,
        SEXO and DIB

        Returs a Pandas Series of boolean values where true means the
    registry is elegile for PEC 287 retirement.
    """

    min_contrib_time = \
        df['DIB'].apply(lambda x: get_min_contribution_time(x))

    female =  (df['SEXO'] == 3) \
            & (df['IDADE_DIB'] >= 62) \
            & (df['IDADE_DIB'] != 999) \
            & (df['TEMPO_CONTRIB'] >= min_contrib_time) \
            & (df['TEMPO_CONTRIB'] != 999)

    male =    (df['SEXO'] == 1) \
            & (df['IDADE_DIB'] >= 65) \
            & (df['IDADE_DIB'] != 999) \
            & (df['TEMPO_CONTRIB'] >= min_contrib_time) \
            & (df['TEMPO_CONTRIB'] != 999)

    return female | male

def perform_analisys(db_conn, table='fato_auxilio', \
        tipo = ['idade', 'tempo', 'invalidez', 'especial'], \
        chunk_size=10000):
    """
       Perfrom PEC 237/2016 Analisys

    db_conn:    db connection
    tipo:       List of types desired: default =
                ['idade', 'tempo', 'invalidez','especial']
    chunk_size: number of rows to fetch on each iteration
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
    fields = 'IDADE_DIB TEMPO_CONTRIB DIB SEXO ESPECIE'.split()
    sql = """
    SELECT {desired_fields}
    FROM {table_name}
    INNER JOIN DIM_ESPECIE ON {table_name}.ESPECIE = DIM_ESPECIE.COD
    WHERE COD IN ({dtype_list})
    """.format(desired_fields = ", ".join(fields), \
         table_name = table, dtype_list = ", ".join(map(str, dtype)))

    # Perform query and retrieve server side cursor
    cur = db_conn.cursor('server_side_cursor')
    cur.itersize = chunk_size
    cur.execute(sql)

    # Fetch results and compute results
    counter = 0
    age_cnt = 0
    time_cnt = 0
    pec287_elegible = 0

    while True:
        chunk = cur.fetchmany(chunk_size)

        if not chunk:
            break

        counter += len(chunk)
        print('\r{} registries read.'.format(counter), end="")

        df = pd.DataFrame(chunk, columns = fields)
        df_subset = df[check_pec_287_df(df)]

        pec287_elegible += df_subset.count()['DIB']
        age_cnt += df_subset.groupby('ESPECIE').count().loc[41]['DIB']
        time_cnt += df_subset.groupby('ESPECIE').count().loc[42]['DIB']

    # Print results
    print("\n{0:0} people out of {1:0} would have retired by PEC " \
            "287/2016 rules ({2:0.2f}%)" \
            .format(pec287_elegible, counter, \
                    pec287_elegible / counter  * 100))
    print("{0:0} ({1:0.2f}%) which actually retired by age, and {2:0}" \
          "by contribution time".format(age_cnt, \
                                        age_cnt / pec287_elegible * 100, \
                                        time_cnt))

########
# main()

# Connect to an existing database
conn = db_connection(host, port, dbname, user, pwd)
if conn == -1:
    print("Unable to connect to the database")
    exit(-1)

# Perform Analisys
perform_analisys(conn, dbtable, tipo = ['tempo', 'idade'], \
    chunk_size=chunk_size)

# Close communication with the database
conn.close()
