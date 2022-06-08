from KTE_artesian_general import *

###################################################
##################   Formatter  ###################
###################################################


# La funzione rende il dizionario utile al caricamento dei dati su Artesian.
# Il parametro column_name contiene il nome della colonna che si desidera caricare come valore della time series.
# Il parametro column_date_name contiene il nome della colonna che contiene la data.
def get_artesian_dict_versioned(df, column_name, column_date_name, correttore=False):
    cet = pytz.timezone('CET')
    tempi = df.apply(lambda x: tempo.format_date(x[column_date_name]), axis=1).tolist()
    values = df[column_name].values.tolist()
    dict_of_value_to_send = dict()
    for i in range(len(tempi)):
        giorno_ora = datetime(tempi[i].year, tempi[i].month, tempi[i].day, tempi[i].hour, 0)
        if correttore:
            correttore = giorno_ora.astimezone(cet).utcoffset()
            giorno_ora = giorno_ora - correttore
        dict_of_value_to_send[giorno_ora] = values[i]
    return dict_of_value_to_send


# Rende un dizionario per il caricamento dati su Artesian ma con granularità mensile
def get_artesian_dict_versioned_monthly_or_daily(df, column_name, column_date_name):
    tempi = df.apply(lambda x: tempo.format_date(x[column_date_name]), axis=1).tolist()
    values = df[column_name].values.tolist()
    dict_of_value_to_send = dict()
    for i in tempi:
        giorno_ora = datetime(tempi[i].year, tempi[i].month, 1, 0)
        dict_of_value_to_send[giorno_ora] = values[i]
    return dict_of_value_to_send


def get_artesian_dict_versioned_daily_by_index(df, column_name):
    dict_artesian = dict()
    for index, row in df.iterrows():
        if np.isnan(row[column_name]):
            dict_artesian[datetime(index.year, index.month, index.day)] = np.nan
        else:
            dict_artesian[datetime(index.year, index.month, index.day)] = row[column_name]
    return dict_artesian


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################

def post_artesian_versioned_time_series(data, dict_of_tags, provider, curve_name, string_granularity, version=datetime.now(), original_timezone='CET'):
    cfg = get_configuration()
    gran = get_granularity(string_granularity)

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=gran,
        type=MarketData.MarketDataType.VersionedTimeSerie,
        originalTimezone=original_timezone,
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)

    if registered is None:
        mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'UTC',
                                 rows=data,
                                 version=version,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)


def post_artesian_versioned_time_series_hourly(data, dict_of_tags, provider, curve_name, version=datetime.now()):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Hour,
        type=MarketData.MarketDataType.VersionedTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)

    if registered is None:
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 version=version,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)


###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################

def get_artesian_data_versioned(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione,
                                ganularity='h', time_zone='CET'):
    cfg = get_configuration()
    qs = QueryService(cfg)
    return qs.createVersioned() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone(time_zone) \
        .inGranularity(get_granularity(ganularity))

###################################################
# ''''''''''''''''''   UPDATE  ''''''''''''''''''''
###################################################
