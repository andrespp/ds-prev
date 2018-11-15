import numpy as np
import pandas as pd
import pandas.io.sql as sqlio

### TODO obter dados corretos e concluir função
def P2(i, t, s, c):
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
    cols = "Idade Urb_Homens Urb_Mulheres Rur_Homens Rur_Mulheres".split()
    dt = np.array( \
       [[0, 46408, 44681, 16676, 16325],
        [1, 50706, 48833, 17671, 16786],
        [2, 59654, 57015, 21044, 20464],
        [3, 65343, 63559, 23064, 22486],
        [4, 68478, 64898, 24235, 22916],
        [5, 72450, 69471, 26401, 24560],
        [6, 74664, 71236, 27202, 25308],
        [7, 78087, 74948, 28357, 26276],
        [8, 83676, 76901, 30139, 26879],
        [9, 79565, 75985, 28819, 26945],
        [10, 88423, 79985, 33214, 28885],
        [11, 81360, 73277, 30788, 26891],
        [12, 89904, 80331, 34555, 29042],
        [13, 80708, 74563, 30812, 27020],
        [14, 82334, 71387, 31903, 26118],
        [15, 83392, 77554, 32187, 27583],
        [16, 83973, 73724, 33088, 26205],
        [17, 85984, 76048, 32866, 25878],
        [18, 105334, 87785, 37488, 26686],
        [19, 90483, 79393, 30620, 23058],
        [20, 99827, 88418, 34096, 25164],
        [21, 89904, 76055, 28636, 20121],
        [22, 107024, 89389, 35222, 23672],
        [23, 105292, 87979, 33198, 22171],
        [24, 96594, 80179, 28424, 19788],
        [25, 107249, 91616, 31938, 21900],
        [26, 87571, 74789, 25709, 17976],
        [27, 89089, 77434, 25376, 18189],
        [28, 95470, 80389, 26203, 18558],
        [29, 71904, 64631, 19845, 14873],
        [30, 109733, 95516, 30205, 20829],
        [31, 56267, 52661, 15359, 11887],
        [32, 83895, 78620, 22715, 16973],
        [33, 74837, 67555, 20453, 15204],
        [34, 70324, 63547, 18988, 13776],
        [35, 87331, 80563, 22840, 17169],
        [36, 68369, 66059, 18780, 14937],
        [37, 67158, 66049, 18064, 14785],
        [38, 82995, 75476, 21943, 16901],
        [39, 64105, 60400, 16966, 13352],
        [40, 107622, 92855, 27751, 19477],
        [41, 47887, 44201, 12278, 9725],
        [42, 83020, 73268, 21316, 15113],
        [43, 65644, 58657, 17417, 13296],
        [44, 52994, 46578, 15044, 11758],
        [45, 78923, 65984, 21527, 15134],
        [46, 50332, 45332, 14317, 11320],
        [47, 48764, 44680, 13820, 11289],
        [48, 54864, 49975, 16148, 12541],
        [49, 44190, 40725, 12444, 10617],
        [50, 66823, 57023, 18065, 12714],
        [51, 32465, 28936, 9073, 7696],
        [52, 46481, 41130, 13115, 10009],
        [53, 42017, 37791, 12418, 10380],
        [54, 39335, 36218, 11944, 10587],
        [55, 41266, 42353, 13709, 14262],
        [56, 35827, 34267, 12057, 11059],
        [57, 29194, 28435, 10660, 9660],
        [58, 31016, 29372, 11409, 9050],
        [59, 25151, 23913, 10236, 7210],
        [60, 42837, 44190, 15918, 13566],
        [61, 17171, 17262, 7015, 5398],
        [62, 25411, 26203, 10112, 7979],
        [63, 24175, 25453, 9687, 7969],
        [64, 21276, 23410, 8418, 7147],
        [65, 32773, 36051, 12278, 10452],
        [66, 18844, 22126, 8277, 7548],
        [67, 21422, 23815, 8542, 7293],
        [68, 18294, 22270, 7189, 6761],
        [69, 12713, 15157, 4594, 4416],
        [70, 23309, 27923, 9007, 8835],
        [71, 9916, 11445, 3641, 3119],
        [72, 15801, 18778, 6006, 5108],
        [73, 13263, 16157, 4944, 4627],
        [74, 12572, 15628, 4710, 4531],
        [75, 14694, 18322, 5532, 5436],
        [76, 12248, 15663, 4510, 4499],
        [77, 10407, 12767, 4137, 3808],
        [78, 11987, 14938, 4291, 4350],
        [79, 6909, 8852, 2370, 2375],
        [80, 11415, 15680, 4422, 4897],
        [81, 5182, 6740, 1914, 1785],
        [82, 7338, 9644, 2567, 2609],
        [83, 6036, 8313, 2228, 2290],
        [84, 6030, 8294, 2292, 2480],
        [85, 4947, 7157, 1868, 2205],
        [86, 4692, 6757, 1780, 2033],
        [87, 3855, 5686, 1552, 1732],
        [88, 2965, 4335, 1238, 1368],
        [89, 2320, 3368, 869, 1014],
        [90, 2348, 4014, 1039, 1332],
        [91, 1157, 1807, 460, 503],
        [92, 1363, 2235, 585, 692],
        [93, 1126, 1927, 464, 605],
        [94, 951, 1659, 363, 484],
        [95, 690, 1215, 300, 408],
        [96, 703, 1185, 276, 419],
        [97, 452, 823, 209, 301],
        [98, 418, 769, 161, 259],
        [99, 269, 538, 113, 185],
        [100, 396, 1047, 181, 378]])

    df = pd.DataFrame(dt, columns=cols)

    i = [0, 1, 2, 3, 4, 5, 6]
    i = np.arange(0, 101, 1)
    qtd = [0, 0, 0, 0]
    for idade in i:
        qtd[0] = qtd[0] + df.loc[idade]['Urb_Homens']
        qtd[1] = qtd[1] + df.loc[idade]['Urb_Mulheres']
        qtd[2] = qtd[2] + df.loc[idade]['Rur_Homens']
        qtd[3] = qtd[3] + df.loc[idade]['Urb_Mulheres']

    #for ano in t:
    #for sexo in s:
    #for clientela in c:

    pop = pd.DataFrame(qtd, columns=cols)

    return pop

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
    #print(sql)

    # Query the database and obtain data as Python objects
    dt = sqlio.read_sql_query(sql, conn)
    estoque = dt['qtd'][0]

    # Close communication with the database
    conn.close()

    return estoque
