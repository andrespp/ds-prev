"""fato_reforma.py
"""
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import datetime as dt

# Track execution time
start_time = time.time()

### Parâmetros
TABLE_NAME = 'fato_reforma'
#DBTABLE = 'FATO_AUXILIO_SAMPLE'
DBTABLE = 'FATO_AUXILIO'
ANO_INICIO = 1995
ANO_FIM = 2016
DADOS_FAZENDA = '../dataset/dados_fazenda.xlsx'

# Conection parameters
HOST='tama'
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

def ds_query(sql_query):
    """
        Query Dataset

    Parâmetros
    ----------
        sql : string
            SQL query to be performed against the dataset

    Retorno
    -------
        Pandas Dataframe
    """
    # Connect to an existing database
    try:
        conn = psycopg2.connect("host='{}' port={} dbname='{}'user={} password={}"
                .format(HOST, PORT, DBNAME, USER, PASS))
        df = sqlio.read_sql_query(sql, conn)
        # Close communication with the database
        conn.close()
        return df
    except:
        print("Unable to connect to the database")
        return

def ds_write(table_name, df):
        """Write dataframe to table. Dataframe's Index will be used as a
        column named 'table_name'

        Parameters
        ----------
        table_name : str
            Table to be written

        df | Pandas DataFrame
            Data to be loaded

        Returns
        -------
            status : boolean
                True on success, False otherwise
        """
        ## psycopg2
        eng_str = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            USER, PASS, HOST, PORT, DBNAME)
        engine = create_engine(eng_str)
        conn = psycopg2.connect("host='{}' port={} dbname='{}'user={} password={}"
                .format(HOST, PORT, DBNAME, USER, PASS))

        if conn == -1:
            print("query(): Unable to connect to the database.")
            return False
        else:
            #sk = table_name.split('_')[1]+'_sk' # remove 'dim_' prefix
            df.to_sql(name=table_name,
                      con=engine,
                      index=True,
                      #index_label=sk,
                      if_exists='replace')
            conn.close()
            print('{} registries written to {} table.'.format(
                             len(df), table_name))
            return True

def get_ano_dib(ano_nasc, ano_inicio_contrib, sexo, clientela):
    """
        Retorna ano mínimo para aposentadoria segundo PEC 6/2019

    Parâmetros
    ----------
        ano_nasc : int
            Ano de nascimento do contribuinte
        ano_inicio_contrib : int
            Ano em que o contribuinte iniciou suas contribuições
        sexo : int
            Sexo do contribuinte (1: masculino, 3: feminino)
        clientela : int
            Clientela do contribuinte (1: rural, 2: urbana)

    Retorno
    -------
            Inteiro indicando o ano mínimo em que a pessoa poderá se aposentar
        (-1 em caso de erro)
    """
    if clientela == 2:
        if sexo == 1:
            ano_dib = (ano_nasc+60, ano_inicio_contrib+20)
            return int(max(ano_dib))
        elif sexo == 3:
            ano_dib = (ano_nasc+60, ano_inicio_contrib+20)
            return int(max(ano_dib))

    if clientela == 1:
        if sexo == 1:
            ano_dib = (ano_nasc+65, ano_inicio_contrib+20)
            return int(max(ano_dib))
        elif sexo == 3:
            ano_dib = (ano_nasc+62, ano_inicio_contrib+20)
            return int(max(ano_dib))

    else:
        return -1

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
    #print('{}, {}, {}, {}'.format(ano, idade, sexo, sobrevida))

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
sql = """
SELECT *
FROM {table_name}
WHERE DIB > {ano}*10000
    AND ESPECIE IN (41, 42)  -- APOSENTADORIA POR IDADE / TEMPO DE SERVIÇO
    AND CLIENTELA IN (1, 2)  -- URBANA / RURAL
    AND SEXO IN (3, 1)       -- MULHERES / HOMENS
""".format(table_name=DBTABLE,
           ano=ANO_INICIO)
df = ds_query(sql)

print('Generating "fato_refoma" dataset...')

# Cleanup nulls and fix data types
df['dt_obito'] = df['dt_obito'].fillna(value=0)
df.dropna(subset=['dt_nasc'], inplace=True)

# Compute new attributes
df['ano_dib'] = df['dib'].apply(lambda x: int(x/10000))
df['ano_nasc'] = df['dt_nasc'].apply(lambda x: int(x/10000))
df['ano_inicio_contrib'] = df.apply(lambda x: int(x['ano_dib'] - x['tempo_contrib']), axis=1)

# Compute PEC 6/2019 attributes
df['pec6_ano_dib'] = df.apply(lambda x: get_ano_dib(x['ano_nasc'],
                                                    x['ano_inicio_contrib'],
                                                    x['sexo'],
                                                    x['clientela']),
                              axis=1)
df['pec6_idade_dib'] = df.apply(lambda x: int(x['pec6_ano_dib'] - x['ano_nasc']), axis=1)
df['pec6_gap'] = df.apply(lambda x: int(x['pec6_idade_dib'] - x['idade_dib']), axis=1)
df['pec6_prob'] = df.apply(lambda x: prob_sobrevivencia(x['ano_dib'],
                                                        x['idade_dib'],
                                                        x['sexo'],
                                                        x['pec6_gap']),
                           axis=1)

#FATO_PESSOA

fato_pessoa = df[['ano_nasc','dt_nasc','dt_obito','sexo', 'clientela',
                  'ano_inicio_contrib', 'ano_dib','idade_dib','tempo_contrib',
                  'especie', 'pec6_ano_dib', 'pec6_idade_dib', 'pec6_gap',
                  'pec6_prob'
                 ]]
#print(fato_pessoa.head())
#print(fato_pessoa.columns)

print('Writing "fato_refoma" to database...')
ds_write('fato_reforma', fato_pessoa)

# Print out elapsed time
elapsed_time = (time.time() - start_time) / 60
print("\nExecution time: {0:0.4f} minutes.".format(elapsed_time))
