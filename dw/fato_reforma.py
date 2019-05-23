# -*- coding: utf-8 -*-
"""fato_reforma.py
"""
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import datetime as dt

# Track execution time
print("Started at: {}\n".format(dt.datetime.now()))
start_time = time.time()

### Parâmetros
LOAD_TABLE_NAME = 'fato_reforma'
DBTABLE = 'FATO_AUXILIO'
CHUNK_SIZE = 100000
#LOAD_TABLE_NAME = 'fato_reforma_sample'
#DBTABLE = 'FATO_AUXILIO_SAMPLE'
#CHUNK_SIZE = 1000
ANO_INICIO = 1995
ANO_FIM = 2016
DADOS_FAZENDA = './dataset/dados_fazenda.xlsx'

# Conection parameters
HOST='localhost'
PORT='5432'
DBNAME='prevdb'
USER='prevdb_user'
PASS='pr3v'

FATO_REFORMA = """
CREATE TABLE fato_reforma
(
  ano_nasc INTEGER
, dt_nasc INTEGER
, dt_obito INTEGER
, sexo INTEGER
, clientela INTEGER
, ano_inicio_contrib INTEGER
, ano_dib INTEGER
, ano_idade_dib INTEGER
, tempo_contrib INTEGER
, especie INTEGER
, pec6_ano_dib INTEGER
, pec6_idade_dib INTEGER
, pec6_gap INTEGER
, pec6_prob NUMERIC
);
"""
###############################################################################
### LIBRARY

def ds_write(table_name, df, if_exists='fail'):
        """Write dataframe to table. Dataframe's Index will be used as a
        column named 'table_name'

        Parameters
        ----------
        table_name : str
            Table to be written

        df : Pandas DataFrame
            Data to be loaded

        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.

        Returns
        -------
            status : int
                Number of registries written
        """
        ## psycopg2
        eng_str = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            USER, PASS, HOST, PORT, DBNAME)
        engine = create_engine(eng_str)
        conn = psycopg2.connect(
            "host='{}' port={} dbname='{}'user={} password={}"
                .format(HOST, PORT, DBNAME, USER, PASS))

        if conn == -1:
            print("query(): Unable to connect to the database.")
            return 0
        else:
            df.to_sql(name=table_name,
                      con=engine,
                      index=True,
                      #index_label=sk,
                      if_exists=if_exists)
            conn.close()
            return len(df)

def get_ano_dib(especie,
                ano_nasc,
                ano_inicio_contrib,
                ano_beneficio,
                tempo_contrib,
                sexo, clientela):
    """
        Retorna ano mínimo para aposentadoria segundo PEC 6/2019

    Parâmetros
    ----------
        especie : int
            Especie do benefício
        ano_nasc : int
            Ano de nascimento do contribuinte
        ano_inicio_contrib : int
            Ano em que o contribuinte iniciou suas contribuições
        ano_beneficio : int
            Ano em que o contribuinte efetivamente se aposentou
        tempo_contrib : int
            Tempo de contibuição no momento da aposentadoria
        sexo : int
            Sexo do contribuinte (1: masculino, 3: feminino)
        clientela : int
            Clientela do contribuinte (1: rural, 2: urbana)

    Retorno
    -------
            Inteiro indicando o ano mínimo em que a pessoa poderá se aposentar
        (-1 em caso de erro)
    """
    if especie in (41, 42): # Idade / Tempo de Contribuição
        if clientela == 2:
            if sexo == 1:
                ano_dib = (ano_nasc+60, ano_inicio_contrib+20)
                ano_dib = int(max(ano_dib))
            elif sexo == 3:
                ano_dib = (ano_nasc+60, ano_inicio_contrib+20)
                ano_dib = int(max(ano_dib))

        elif clientela == 1:
            if sexo == 1:
                ano_dib = (ano_nasc+65, ano_inicio_contrib+20)
                ano_dib = int(max(ano_dib))
            elif sexo == 3:
                ano_dib = (ano_nasc+62, ano_inicio_contrib+20)
                ano_dib =  int(max(ano_dib))

        else:
            ano_dib = -1

    elif especie == 46: # Tempo de Contribuição especial
        if tempo_contrib < 20: # Grupo 55/15
                ano_dib = (ano_nasc+55, ano_inicio_contrib+15)
                ano_dib = int(max(ano_dib))
        if tempo_contrib >= 20 and tempo_contrib < 25: # Grupo 58/20
                ano_dib = (ano_nasc+58, ano_inicio_contrib+20)
                ano_dib = int(max(ano_dib))
        if tempo_contrib >= 25: # Grupo 60/25
                ano_dib = (ano_nasc+60, ano_inicio_contrib+25)
                ano_dib = int(max(ano_dib))

    elif especie == 57: # Professor
        ano_dib = (ano_nasc+60, ano_inicio_contrib+30)
        ano_dib = int(max(ano_dib))
        pec6_gap = ano_dib - ano_beneficio
        if pec6_gap < 0:
            return ano_beneficio
        else:
            return ano_dib

    elif especie in (32, 92): # Invalidez (previdenciária, acidentária)
        return ano_beneficio

    else:
        return -1

    pec6_gap = ano_dib - ano_beneficio

    if pec6_gap < 0:
        return ano_beneficio
    else:
        return ano_dib

def prob_sobrevivencia(ano, idade, sexo, sobrevida):
    """
        Retorna propabilidade de uma pessoa com idade 'idade' no ano 'ano'
    sobreviver por 'sobrevida' anos

    Parâmetros
    ----------
        ano: int
            Ano base para calculo da probabilidade
        idade : int
            Idade da pessoa no ano base
        sexo : int
            Sexo do contribuinte (1: masculino, 3: feminino)
        sobrevida : int
            Numero de anos de sobrevivencia

    Retorno
    -------
        Inteiro entre 0 e 1
    """
    ano = int(ano)
    idade = int(idade)
    sexo = int(sexo)
    sobrevida = int(sobrevida)

    # Aproxima probabilidade caso parametros estejam fora dos limites dos dados
    if ano<2000:
        ano=2000
    if idade>89:
        idade = 89
    if sobrevida < 0:
        return 1

    anoProjetado = ano + sobrevida
    idadeProjetada = idade + sobrevida

    if anoProjetado > 2060:
        anoProjetado = 2060
    if anoProjetado < 2000:
        anoProjetado = 2000
    if idadeProjetada > 89:
        idadeProjetada = 89
    if idadeProjetada < 0:
        idadeProjetada = 0

    # Calcula probabilidades
    if sexo == 1:
        return POPH[anoProjetado].loc[idadeProjetada] / POPH[ano].loc[idade]
    elif sexo == 3:
        return POPM[anoProjetado].loc[idadeProjetada] / POPM[ano].loc[idade]
    else:
        return -1

def transform(df):
    """
        FATO_REFORMA

    Parâmetros
    ----------
        df: Pandas DataFrame
            Resultado da query em fato_auxilio

    Retorno
    -------
        fato_reforma (DataFrame)
    """
    # Cleanup nulls and fix data types
    df['dt_obito'].fillna(value=0, inplace=True, downcast='infer')
    df.dropna(subset=['dt_nasc'], inplace=True)

    # Compute new attributes
    df['ano_dib'] = df['dib'].apply(lambda x: int(x/10000))
    df['ano_nasc'] = df['dt_nasc'].apply(lambda x: int(x/10000))
    df['ano_inicio_contrib'] = df.apply(lambda x:
                int(x['ano_dib'] - x['tempo_contrib']), axis=1)

    # Compute PEC 6/2019 attributes
    df['pec6_ano_dib'] = df.apply(lambda x:
                get_ano_dib(x['especie'],
                            x['ano_nasc'],
                            x['ano_inicio_contrib'],
                            x['ano_dib'],
                            x['tempo_contrib'],
                            x['sexo'],
                            x['clientela']), axis=1)
    df['pec6_idade_dib'] = df.apply(lambda x:
                int(x['pec6_ano_dib'] - x['ano_nasc']), axis=1)
    # Workaround for issue #5 when especie in (32, 92)
    df['pec6_idade_dib'] = df.apply(lambda x:
                x['idade_dib'] if x['especie'] in (32, 92)
                               else x['pec6_idade_dib'], axis=1)
    df['pec6_gap'] = df.apply(lambda x:
                int(x['pec6_idade_dib'] - x['idade_dib']), axis=1)
    df['pec6_prob'] = df.apply(lambda x:
                prob_sobrevivencia(x['ano_dib'],
                                   x['idade_dib'],
                                   x['sexo'],
                                   x['pec6_gap']), axis=1)

    fato_pessoa = df[['ano_nasc','dt_nasc','dt_obito','sexo',
                      'clientela', 'ano_inicio_contrib', 'ano_dib',
                      'idade_dib','tempo_contrib', 'especie',
                      'pec6_ano_dib', 'pec6_idade_dib', 'pec6_gap',
                      'pec6_prob'
                     ]]

    return fato_pessoa

###############################################################################
### MAIN
print('Reading Population dataset...')

### População
POPH = pd.read_excel(DADOS_FAZENDA,
                     sheet_name='PopIbgeH',
                     index_col='ÍNDICE',
                     nrows=91,
                     dtype=int)
POPM = pd.read_excel(DADOS_FAZENDA,
                     sheet_name='PopIbgeM',
                     index_col='ÍNDICE',
                     nrows=91,
                     dtype=int)

print('Reading RGPS dataset...')
fields = "especie dib ddb mot_cessacao ult_compet_mr vl_mr dt_nasc \
          vl_rmi clientela sexo situacao dt_obito idade_dib \
          tempo_contrib".split()
sql = """
SELECT *
FROM {table_name}
WHERE DIB > {ano}*10000
    AND ESPECIE IN (41, 42, 46, 57, 32, 92)
    AND CLIENTELA IN (1, 2)  -- URBANA / RURAL
    AND SEXO IN (3, 1)       -- MULHERES / HOMENS
    AND IDADE_DIB <> 999
""".format(table_name=DBTABLE,
           ano=ANO_INICIO)

# Connect to database
try:
    conn = psycopg2.connect(
        "host='{}' port={} dbname='{}'user={} password={}"
            .format(HOST, PORT, DBNAME, USER, PASS))
except:
    print("Unable to connect to the database")
    exit(-1)

# Perform query and retrieve server side cursor
cur = conn.cursor('server_side_cursor')
cur.itersize = CHUNK_SIZE
cur.execute(sql)

# Fetch and compute results
counter = 0
tf_cnt = 0
wr_cnt = 0

while True:
    # Extract
    chunk = cur.fetchmany(CHUNK_SIZE)

    if not chunk:
        break

    counter += len(chunk)
    print('\rExtract: {}. '.format(counter), end="")

    df = pd.DataFrame(chunk, columns = fields)

    # Transform
    fato_pessoa = transform(df)
    tf_cnt += len(fato_pessoa)
    print('\r\t\t\tTransform: {}. '.format(tf_cnt), end="")

    # Load
    if wr_cnt == 0:
        wr_cnt += ds_write(LOAD_TABLE_NAME, fato_pessoa, 'replace')
    else:
        wr_cnt += ds_write(LOAD_TABLE_NAME, fato_pessoa, 'append')
    print('\r\t\t\t\t\t\tLoad: {} ({}).'.format(
                     wr_cnt, LOAD_TABLE_NAME), end="")

# Close communication with the database
conn.close()

# Print out elapsed time
elapsed_time = (time.time() - start_time) / 60
print("\n\nFinished at: {}. ".format(dt.datetime.now()), end="")
print("Execution time: {0:0.4f} minutes.".format(elapsed_time))
