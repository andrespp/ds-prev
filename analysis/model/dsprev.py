import numpy as np
import pandas as pd
import pandas.io.sql as sqlio

### TODO
### - Obter dados completos
### - Implementar argumentos da função
def P(i=0, t=0, s=0, c=0):
    """
       População brasileira anual estimada, por: idade, sexo, clientela

    Parâmetros
    ----------
        i : list
            Idade do beneficiário
        t : list
            Ano desejado
        s : list
            Sexo do beneficiário. Maculino (1), Feminino (3) ou ignorado (9)
        c : list
            Clientela. Urbana (1), Rural (2) ou ignorada (9)

    Retorno
    -------
        Pandas Data Frame com os dados solicitados
    """
    cols = "Ano Brasil Norte Nordeste Sudeste Sul Centro-Oeste".split()
    dt = np.array( \
       [[2001, 172385826, 13245084, 48331186, 73470763, 25453264, 11885529],
        [2002, 174632960, 13504599, 48845112, 74447456, 25734253, 12101540],
        [2003, 176871437, 13784881, 49352225, 75391969, 26025091, 12317271],
        [2004, 181569056, 14373260, 50424713, 77374720, 26635629, 12760734],
        [2005, 184184264, 14698878, 51019091, 78472017, 26973511, 13020767],
        [2006, 186770562, 15022060, 51609027, 79561095, 27308863, 13269517],
        [2007, 183987291, 14623316, 51534406, 77873120, 26733595, 13222854],
        [2008, 189605006, 15142686, 53080679, 80187706, 27497986, 13695949],
        [2009, 191480630, 15359608, 53591197, 80915332, 27719118, 13895375],
        [2011, 192379287, 16095187, 53501859, 80975616, 27562433, 14244192],
        [2012, 193904015, 16303145, 53907144, 81565983, 27708514, 14419229],
        [2013, 201032714, 16983484, 55794707, 84465570, 28795762, 14993191],
        [2014, 202768562, 17231027, 56186190, 85115623, 29016114, 15219608],
        [2015, 204450049, 17472636, 56559481, 85745520, 29230180, 15442232],
        [2016, 206081432, 17707783, 56915936, 86356952, 29439773, 15660988],
        [2017, 207660929, 17936201, 57254159, 86949714, 29644948, 15875907],
        [2018, 208494900, 18182253, 56760780, 87711946, 29754036, 16085885],
        [2019, 209328880, 18254982, 56987823, 88062794, 29873052, 16150229],
        [2020, 210124331, 18324351, 57204377, 88397433, 29986570, 16211600],
        [2021, 210882881, 18390502, 57410885, 88716548, 30094822, 16270124],
        [2022, 211606103, 18453572, 57607776, 89020801, 30198032, 16325922],
        [2023, 212295521, 18513694, 57795464, 89310833, 30296418, 16379112],
        [2024, 212952601, 18570996, 57974348, 89587261, 30390189, 16429807],
        [2025, 213578760, 18625602, 58144814, 89850680, 30479547, 16478117],
        [2026, 214175360, 18677630, 58307233, 90101664, 30564687, 16524146],
        [2027, 214743713, 18727194, 58461962, 90340765, 30645796, 16567996],
        [2028, 215285081, 18774405, 58609344, 90568514, 30723054, 16609764],
        [2029, 215800678, 18819369, 58749710, 90785421, 30796634, 16649544],
        [2030, 216291668, 18862187, 58883377, 90991976, 30866703, 16687425],
        [2031, 216759170, 18902956, 59010650, 91188650, 30933420, 16723494],
        [2032, 217204257, 18941771, 59131821, 91375894, 30996938, 16757833],
        [2033, 217627958, 18978721, 59247169, 91554141, 31057404, 16790523],
        [2034, 218031259, 19013892, 59356964, 91723806, 31114958, 16821639],
        [2035, 218415104, 19047366, 59461462, 91885286, 31169736, 16851254]])


    df = pd.DataFrame(dt, columns=cols)

    return df

def Eb(i, s, c, k, conn, dbtable="FATO_AUXILIO_SAMPLE"):
    """
        Calcula estoque de benefícios previdenciários

    Parâmetros
    ----------
        i : list
            Idade do beneficiário
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
    sql = """
    SELECT
        DIM_SITUACAO.COD
        ,DIM_SITUACAO.DESCRICAO
        ,COUNT(SITUACAO) AS QTD
    FROM {table_name}
    INNER JOIN DIM_SITUACAO ON {table_name}.SITUACAO = DIM_SITUACAO.COD
    WHERE   {table_name}.SITUACAO = 0 --Ativo
            AND {table_name}.IDADE_DIB IN ({lista_idade})
            AND {table_name}.SEXO IN ({lista_sexo})
            AND {table_name}.CLIENTELA IN ({lista_clientela})
            AND {table_name}.ESPECIE IN ({lista_beneficios})
    GROUP BY SITUACAO
            ,DIM_SITUACAO.DESCRICAO
            ,DIM_SITUACAO.COD
    ORDER BY QTD DESC
    """.format(table_name=dbtable, \
                lista_idade=", ".join(map(str, i)), \
                lista_sexo=", ".join(map(str, s)), \
                lista_clientela=", ".join(map(str, c)), \
                lista_beneficios=", ".join(map(str, k)))

    # Query the database and obtain data as Python objects
    dt = sqlio.read_sql_query(sql, conn)
    estoque = dt['qtd'][0]

    # Close communication with the database
    conn.close()

    return estoque

def concessoes(i, t, s, c, k, conn, dbtable="FATO_AUXILIO_SAMPLE"):
    """
        Calcula concessões de benefícios previdenciários

    Parâmetros
    ----------
        i : list
            Idade do beneficiário
        t : list
            Ano desejado
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
                lista_idade=", ".join(map(str, i)), \
                lista_ano=", ".join(map(str, t)), \
                lista_sexo=", ".join(map(str, s)), \
                lista_clientela=", ".join(map(str, c)), \
                lista_beneficios=", ".join(map(str, k)))

    # Query the database and obtain data as Python objects
    dt = sqlio.read_sql_query(sql, conn)
    concessoes = dt['qtd'][0]

    # Close communication with the database
    conn.close()

    return concessoes
