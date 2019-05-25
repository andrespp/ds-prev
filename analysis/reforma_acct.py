import pandas as pd
import psycopg2
import pandas.io.sql as sqlio
import xlwt
import time

###############################################################################
# Parameters
#TB_REFORMA = 'FATO_REFORMA_SAMPLE'
TB_REFORMA = 'FATO_REFORMA'
ANO_FIM = 2016

OUTPUT_FILE = './outputs/2016_PEC_6-2019.xls'
FATO_PESSOA_FILE = './sandbox/FATO_PESSOA_{}.csv'.format(ANO_FIM)
RGPS_TAB = '{}_RGPS_QTD'.format(ANO_FIM)
QTD_TAB = '{}_PEC_QTD'.format(ANO_FIM)
GAP_TAB = '{}_PEC_AVG_GAP'.format(ANO_FIM)
PRB_TAB = '{}_PEC_AVG_PROB'.format(ANO_FIM)

# Conection parameters
HOST='localhost'
PORT='5432'
DBNAME='prevdb'
USER='prevdb_user'
PASS='pr3v'

# Track execution time
start_time = time.time()

###############################################################################
# Library
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

###############################################################################
# FATO_PESSOA
sql = """
SELECT
	ESPECIE
    ,CLIENTELA
	,SEXO
	,IDADE_DIB
	,PEC6_IDADE_DIB
	--,PEC6_TEMPO_CONTRIB
	,PEC6_GAP
	,PEC6_ANO_DIB
	,PEC6_PROB
FROM {table_name}
WHERE PEC6_ANO_DIB = {ano}
""".format(table_name=TB_REFORMA,
           ano=ANO_FIM)
print('ANO FIM = {}'.format(ANO_FIM))
print('Performing query')
fato_pessoa = ds_query(sql)
print('fato_pessoa.shape = {}'.format(fato_pessoa.shape))
df = fato_pessoa[['especie', 'clientela', 'sexo', 'idade_dib',
                  'pec6_idade_dib', 'pec6_prob', 'pec6_gap']]

###############################################################################
# QTD RGPS
df_rgps = df.pivot_table(index='idade_dib',
                        columns=['especie','clientela','sexo'],
                        values='pec6_prob', aggfunc='count').round()
df_rgps.fillna(value=0, inplace=True, downcast='infer')

###############################################################################
# QTD PEC 6/2019
df_qtd = df.pivot_table(index='pec6_idade_dib',
                        columns=['especie','clientela','sexo'],
                        values='pec6_prob', aggfunc='sum').round()
df_qtd.fillna(value=0, inplace=True, downcast='infer')

###############################################################################
# AVG GAP
df_gap = df.pivot_table(index='pec6_idade_dib',
                        columns=['especie','clientela','sexo'],
                    values='pec6_gap', aggfunc='mean')
df_gap.fillna(value=0, inplace=True, downcast='infer')
df_gap

###############################################################################
# AVG GAP
df_prb = df.pivot_table(index='pec6_idade_dib',
                        columns=['especie','clientela','sexo'],
                    values='pec6_prob', aggfunc='mean')
df_prb.fillna(value=0, inplace=True, downcast='infer')
df_prb

###############################################################################
# Write output files
writer = pd.ExcelWriter(OUTPUT_FILE)
df_rgps.to_excel(writer, RGPS_TAB)
df_qtd.to_excel(writer, QTD_TAB)
df_gap.to_excel(writer, GAP_TAB)
df_prb.to_excel(writer, PRB_TAB)
writer.save()
print('{} written!'.format(OUTPUT_FILE))
fato_pessoa.to_csv(FATO_PESSOA_FILE)
print('{} written!'.format(FATO_PESSOA_FILE))

# Print out elapsed time
elapsed_time = (time.time() - start_time) / 60
print("\nExecution time: {0:0.4f} minutes.".format(elapsed_time))
