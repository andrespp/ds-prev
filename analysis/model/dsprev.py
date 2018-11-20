import numpy as np
import pandas as pd
import pandas.io.sql as sqlio

# Auxiliary functions
def get_age_list(group_number):
    """
        Retorna lista de idades correspondentes ao grupo.

    Parâmetros
    ----------
        group_numer : int
            Número do grupo que deseja-se a lista de idades

    Retorno
    -------
        Lista de inteiros com as idades pertencentes ao grupo
    """
    switcher = {
        0:  [0],
        1:  [1, 2, 3, 4],
        2:  [5, 6, 7, 8, 9],
        3:  [10, 11, 12, 13, 14],
        4:  [15, 16, 17, 18, 19],
        5:  [20, 21, 22, 23, 24],
        6:  [25, 26, 27, 28, 29],
        7:  [30, 31, 32, 33, 34],
        8:  [35, 36, 37, 38, 39],
        9:  [40, 41, 42, 43, 44],
        10: [45, 46, 47, 48, 49],
        11: [50, 51, 52, 53, 54],
        12: [55, 56, 57, 58, 59],
        13: [60, 61, 62, 63, 64],
        14: [65, 66, 67, 68, 69],
        15: list(np.arange(70, 151))
    }
    return switcher.get(group_number, [999])

def get_age_description(group_number):
    """
        Retorna descrição textual das idades correspondentes ao grupo.

    Parâmetros
    ----------
        group_numer : int
            Número do grupo que deseja-se a lista de idades

    Retorno
    -------
        Descrição textual das idades pertencentes ao grupo
    """
    switcher = {
        0:  "Menos de 1 ano",
        1:  "1 a 4 anos",
        2:  "5 a 9 anos",
        3:  "10 a 14 anos",
        4:  "15 a 19 anos",
        5:  "20 a 24 anos",
        6:  "25 a 29 anos",
        7:  "30 a 34 anos",
        8:  "35 a 39 anos",
        9:  "40 a 44 anos",
        10: "45 a 49 anos",
        11: "50 a 54 anos",
        12: "55 a 59 anos",
        13: "60 a 64 anos",
        14: "65 a 69 anos",
        15: "70 anos ou mais"
    }
    return switcher.get(group_number, [999])

# Core Functions

def P(i=np.arange(0,16), t=[2013, 2014, 2015], s=[1,3], c=[1,2]):
    """
       População brasileira anual estimada, por: idade, sexo, clientela

    Parâmetros
    ----------
        i : list
            Grupo de idade do beneficiário. Inteiro no intervalo [0, 15]
        t : list
            Ano desejado. Inteiro no intervalo [2013, 2015]
        s : list
            Sexo do beneficiário. Maculino (1), Feminino (3) ou ignorado (9)
        c : list
            Clientela. Urbana (1), Rural (2) ou ignorada (9)

    Retorno
    -------
        Inteiro com o número de habitantes que atendem os critérios solicitados
    """

    pop = pd.read_csv("model/populacao.csv")

    # idade
    pop = pop[pop['idade'].isin(i)]
    # ano
    pop = pop[pop['ano'].isin(t)]
    # sexo
    pop = pop[pop['sexo'].isin(s)]
    # clientela
    pop = pop[pop['clientela'].isin(c)]

    qtd = pop[pop['idade'].isin(i)]['populacao'].sum()
    return qtd

def Eb(conn, dbtable, i=np.arange(0,16), s=[1,3], c=[1,2], k=[41, 42]):
    """
        Calcula estoque de benefícios previdenciários no ano de 2016

    Parâmetros
    ----------
        i : list
            Grupo de idade do beneficiário, inteiro no intervalo [0, 15]
        s : list
            Sexo do beneficiário. Maculino (1), Feminino (3) ou ignorado (9)
        c : list
            Clientela. Urbana (1), Rural (2) ou ignorada (9)
        k : list
            Tipo do benefício
        conn : psycopg2.extensions.connection
            Conexão aberta com o banco de dados (será fechada autimaticamente)
        dbtable : string
            Tabela onde deve ser feita a query

    Retorno
    -------
        Inteiro com o número de benefícios ativos
    """
    age_list = []
    for group in i:
        age_list += get_age_list(group)

    sql = """
    SELECT
        COUNT(SITUACAO) AS QTD
    FROM {table_name}
    INNER JOIN DIM_SITUACAO ON {table_name}.SITUACAO = DIM_SITUACAO.COD
    WHERE   {table_name}.SITUACAO = 0 --Ativo
            AND {table_name}.IDADE_DIB IN ({lista_idade})
            AND {table_name}.SEXO IN ({lista_sexo})
            AND {table_name}.CLIENTELA IN ({lista_clientela})
            AND {table_name}.ESPECIE IN ({lista_beneficios})
    """.format(table_name=dbtable, \
                lista_idade=", ".join(map(str, age_list)), \
                lista_sexo=", ".join(map(str, s)), \
                lista_clientela=", ".join(map(str, c)), \
                lista_beneficios=", ".join(map(str, k)))

    # Query the database and obtain data as Python objects
    dt = sqlio.read_sql_query(sql, conn)

    if len(dt) > 0:
        estoque = dt['qtd'][0]
    else:
        estoque = 0

    return estoque

def concessoes(conn, dbtable, i=np.arange(1,16), t=[2013, 2014, 2015], \
        s=[1,3], c=[1,2], k=[41, 42]):
    """
        Concessões de benefícios previdenciários

    Parâmetros
    ----------
        conn : psycopg2.extensions.connection
            Conexão aberta com o banco de dados
        dbtable : string
            Tabela onde deve ser feita a query
        i : list
            Grupo de idade do beneficiário. Inteiro no intervalo [0, 15]
        t : list
            Ano desejado. Inteiro no intervalo [1995, 2016]
        s : list
            Sexo do beneficiário. Maculino (1), Feminino (3) ou ignorado (9)
        c : list
            Clientela. Urbana (1), Rural (2) ou ignorada (9)
        k : list
            Tipo do benefício

    Retorno
    -------
        Inteiro com o número de benefícios concedidos
    """
    age_list = []
    for group in i:
        age_list += get_age_list(group)

    sql = """
    SELECT
        COUNT({table_name}.ESPECIE) AS QTD
    FROM {table_name}
    INNER JOIN DIM_DATA ON {table_name}.DDB = DIM_DATA.DATE_SK
    WHERE
        DIM_DATA.YEAR_NUMBER IN ({lista_ano})
        AND {table_name}.IDADE_DIB IN ({lista_idade})
        AND {table_name}.SEXO IN ({lista_sexo})
        AND {table_name}.CLIENTELA IN ({lista_clientela})
        AND {table_name}.ESPECIE IN ({lista_beneficios})
    """.format(table_name=dbtable, \
                lista_idade=", ".join(map(str, age_list)), \
                lista_ano=", ".join(map(str, t)), \
                lista_sexo=", ".join(map(str, s)), \
                lista_clientela=", ".join(map(str, c)), \
                lista_beneficios=", ".join(map(str, k)))

    # Query the database and obtain data as Python objects
    dt = sqlio.read_sql_query(sql, conn)

    if len(dt) > 0:
        concessoes = dt['qtd'][0]
    else:
        concessoes = 0

    return concessoes
