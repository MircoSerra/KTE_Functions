from KTE_artesian_general import *
###################################################
##################   Formatter  ###################
###################################################


# Funzione per la creazione del dizionario per il caricamento di curve di tipo Market Assessment
# Questa funzione permette di caricare un solo valore nel campo settlement.
# Il parametro codifica_colonna è la funzione per la codifica dei nomi dei prodotti dal
# formato contenuto nel dataframe al formato funzionale al caricamento delle curve su Artesian
def make_artesian_dict_market_assesment_settlement(df, codifica_colonna):
    dict_of_market_assestments = dict()
    for index, row in df.iterrows():
        list_of_column_names = row.index.to_list()
        giorno_tmp = datetime(index.year, index.month, index.day)
        dict_of_market_assestments[giorno_tmp] = dict()
        for name in list_of_column_names:
            codifica = codifica_colonna(name, index)
            if codifica != 'x':
                dict_of_market_assestments[giorno_tmp][codifica] = MarketData.MarketAssessmentValue(
                    settlement=row[name])

    return dict_of_market_assestments


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_month(Pandas_Timestamp(maggio 2021), 1) --> Jun-21
def codifica_month(data, offset):
    data = data + pd.DateOffset(months=offset)
    mese = datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%b')
    return mese + '-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_season(Pandas_Timestamp(maggio 2021), 1) --> Sum-21
def codifica_season(data, offset):
    data = data + pd.DateOffset(months=offset * 6)
    if data.month < 4 or data.month > 9:
        return 'Win-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')
    else:
        return 'Sum-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_quarter(Pandas_Timestamp(maggio 2021), 1) --> Q321
def codifica_quarter(data, offset):
    data = data + pd.DateOffset(months=offset * 3)
    return 'Q' + str(data.quarter) + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_year(Pandas_Timestamp(maggio 2021), 1) --> 2022
def codifica_year(data, offset):
    data = data + pd.DateOffset(years=offset)
    return datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%Y')


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################

def post_artesian_market_assestments_daily(data, dict_of_tags, provider, curve_name):
    cfg = get_configuration()
    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Day,
        type=MarketData.MarketDataType.MarketAssessment,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    marketAssessment = MarketData.UpsertData(MarketData.MarketDataIdentifier(provider, curve_name), 'CET',
                                             marketAssessment=data,
                                             downloadedAt=datetime.now().replace(tzinfo=tz.UTC)
                                             )

    mkservice.upsertData(marketAssessment)


###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################

def get_artesian_data_market_assesment(arr_id_curva,
                                       str_data_inizio_estrazione,
                                       str_data_fine_estrazione,
                                       arr_products,
                                       filler_str='no',
                                       filler_value=0):
    if filler_str == 'no':
        return get_artesian_data_market_assesment_with_no_fill(arr_id_curva,
                                                               str_data_inizio_estrazione,                                                               str_data_fine_estrazione,
                                                               arr_products)
    if filler_str == 'last':
        return get_artesian_data_market_assesment_fill_latest_value(arr_id_curva,
                                                                    str_data_inizio_estrazione,
                                                                    str_data_fine_estrazione,
                                                                    arr_products)
    if filler_str == 'custom':
        if type(filler_value) == 'int':
            return get_artesian_data_market_assesment_fill_custom_value(arr_id_curva,
                                                                        str_data_inizio_estrazione,
                                                                        str_data_fine_estrazione,
                                                                        arr_products)
        else:
            return get_artesian_data_market_assesment_fill_custom_value(arr_id_curva,
                                                                        str_data_inizio_estrazione,
                                                                        str_data_fine_estrazione,
                                                                        arr_products,
                                                                        settlement=filler_value.settlement,
                                                                        apertura=filler_value.apertura,
                                                                        close=filler_value.close,
                                                                        high=filler_value.high,
                                                                        low=filler_value.low,
                                                                        volume_paid=filler_value.volume_paid,
                                                                        volume_given=filler_value.volume_given,
                                                                        volume=filler_value.volume)
    if filler_str == 'none':
        return get_artesian_data_market_assesment_fill_with_none(arr_id_curva,
                                                                 str_data_inizio_estrazione,
                                                                 str_data_fine_estrazione,
                                                                 arr_products)


def get_artesian_data_market_assesment_fill_latest_value(arr_id_curva,
                                                         str_data_inizio_estrazione,
                                                         str_data_fine_estrazione,
                                                         arr_products):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillLatestValue("P5D") \
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


def get_artesian_data_market_assesment_fill_custom_value(arr_id_curva,
                                                         str_data_inizio_estrazione,
                                                         str_data_fine_estrazione,
                                                         arr_products,
                                                         settlement=0,
                                                         apertura=0,
                                                         close=0,
                                                         high=0,
                                                         low=0,
                                                         volume_paid=0,
                                                         volume_given=0,
                                                         volume=0):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillCustomValue(settlement=settlement,
                             apertura=apertura,
                             close=close,
                             high=high,
                             low=low,
                             volume_paid=volume_paid,
                             volume_given=volume_given,
                             volume=volume)\
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


def get_artesian_data_market_assesment_with_no_fill(arr_id_curva,
                                                    str_data_inizio_estrazione,
                                                    str_data_fine_estrazione,
                                                    arr_products):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillNone()\
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


def get_artesian_data_market_assesment_fill_with_none(arr_id_curva,
                                                      str_data_inizio_estrazione,
                                                      str_data_fine_estrazione,
                                                      arr_products):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillNull()\
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


###################################################
# ''''''''''''''''''   UPDATE  ''''''''''''''''''''
###################################################
