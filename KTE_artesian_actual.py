from KTE_artesian_general import *

###################################################
# '''''''''''''''''  Formatter  ''''''''''''''''
###################################################


# Il dataframe passato come input deve avere l'index ti tipo Pandas Timestamp, il parametro colonna è il nome della
# colonna che si vuole usare come valore della time series
def make_artesian_dict_actual(df, colonna):
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime(index.year, index.month, index.day, index.hour)] = row[colonna]
    return dict_artesian


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################


def post_artesian_actual_time_series_daily(data, dict_of_tags, provider, curve_name):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Day,
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 downloadedAt=datetime.now().replace(tzinfo=tz.UTC)
                                 )

    mkservice.upsertData(data)


def post_artesian_actual_time_series_monthly(data, dict_of_tags, provider, curve_name):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Month,
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)


def post_artesian_actual_time_series(data, dict_of_tags, provider, curve_name, string_granularity):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=get_granularity(string_granularity),
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)

###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################


def get_artesian_data_actual(arr_id_curva,
                      str_data_inizio_estrazione,
                      str_data_fine_estrazione,
                      ganularity='h', time_zone='CET'):
    cfg = get_configuration()
    gran = get_granularity(ganularity)

    qs = QueryService(cfg)
    data = qs.createActual() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone(time_zone) \
        .inGranularity(gran) \
        .execute()

    df_curva = pd.DataFrame(data)
    return df_curva


def get_artesian_data_actual_daily(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione):
    cfg = get_configuration()

    qs = QueryService(cfg)

    data = qs.createActual() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone("CET") \
        .inGranularity(Granularity.Day) \
        .execute()

    df_curva = pd.DataFrame(data)
    return df_curva


###################################################
# ''''''''''''''''''   UPDATE  ''''''''''''''''''''
###################################################
