from KTE_artesian_general import *
###################################################
###############   Costruttore DF   ################
###################################################


# In via di deprecamento
def format_artesian_auction_data(df_artesian):
    '''
        Trasforma il dataframe ricevuto da artesian nella forma "Colonne: Nomi curve" / "Righe: Timestamp"
        :param df_artesian:
        :return df_formattato:
    '''
    list_of_rows_names = df_artesian['N'].unique().tolist()
    dizionario_generatore_dataframe = dict()
    for i in df_artesian.index:
        try:
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']][df_artesian.loc[i]['N']] = df_artesian.loc[i]['D']
        except KeyError:
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']] = dict()
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']][df_artesian.loc[i]['N']] = df_artesian.loc[i]['D']
    lista_generatrice_df = list()
    i = 0
    for k1 in dizionario_generatore_dataframe.keys():
        lista_tmp = list()
        lista_tmp.append(k1)
        for k2 in list_of_rows_names:
            try:
                lista_tmp.append(dizionario_generatore_dataframe[k1][k2])
            except KeyError:
                lista_tmp.append(0)
        lista_generatrice_df.append(lista_tmp)
    df = pd.DataFrame(lista_generatrice_df, columns=['Date'] + list_of_rows_names)
    df['Date'] = df['Date'].apply((lambda data: tempo.string_to_datetime(data, '%Y-%m-%dT%H:%M:%S%z')))
    return df



###################################################
##################   Formatter  ###################
###################################################


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################


###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################


###################################################
# ''''''''''''''''''   UPDATE  ''''''''''''''''''''
###################################################
