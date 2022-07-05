from KTE_artesian_general import *


###################################################
# '''''''''''''''''  Formatter  ''''''''''''''''
###################################################
def get_correct_args(arguments):
    try:
        ganularity = arguments['ganularity']
    except:
        ganularity = 'h'
    try:
        time_zone = arguments['time_zone']
    except:
        time_zone = 'CET'
    try:
        str_extaction_window = arguments['str_extaction_window']
    except:
        str_extaction_window = 'Abs'
    try:
        relative_period = arguments['relative_period']
    except:
        relative_period = 'Rolling week'
    try:
        period = arguments['period']
    except:
        period = 'P5D'
    try:
        period_start = arguments['period_start']
    except:
        period_start = 'P-3D'
    try:
        period_end = arguments['period_end']
    except:
        period_end = 'P10D'
    try:
        filler_strat = arguments['filler_strat']
    except:
        filler_strat = 'null'
    try:
        custom = arguments['custom']
    except:
        custom = 0
    try:
        max_older = arguments['max_older']
    except:
        max_older = 0
    try:
        end_value = arguments['end_value']
    except:
        end_value = 0
    try:
        time_trans = arguments['time_trans']
    except:
        time_trans = False
    return ganularity, time_zone, str_extaction_window, relative_period, period, period_start, period_end, \
           filler_strat, custom, max_older, end_value, time_trans


def make_artesian_dict_actual(df, colonna):
    '''
        Il dataframe passato come input deve avere l'index ti tipo Pandas Timestamp, il parametro colonna è il nome della
        colonna che si vuole usare come valore della time series
            :param df: Pandas DataFrame
            :param colonna: Stringa nome della colonna da trasformare in dizionario
            :return: Dizionario formattato per Artesian
    '''
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime(index.year, index.month, index.day, index.hour)] = row[colonna]
    return dict_artesian


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################


def post_artesian_actual_time_series_daily(data, dict_of_tags, provider, curve_name):
    '''
        Carica su Artesian una Actual Time Series con granularità giornaliera
            :param data: Dizionario formattato secondo specifiche Artesian. Chiave(Datetime)->Valore(Float)
            :param dict_of_tags: Dizionario contenente nome e categoria dei tag.
                                    Chiave(Stringa-Categoria)->Valore(ArrayString-> Tags)
            :param provider: Stringa contenente il nome del provider del dato
            :param curve_name: Stringa contenente il nome univoco della curva
            :return: None
    '''
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
    '''
        Carica su Artesian una Actual Time Series con granularità mensile
            :param data: Dizionario formattato secondo specifiche Artesian. Chiave(Datetime)->Valore(Float)
            :param dict_of_tags: Dizionario contenente nome e categoria dei tag.
                                    Chiave(Stringa-Categoria)->Valore(ArrayString-> Tags)
            :param provider: Stringa contenente il nome del provider del dato
            :param curve_name: Stringa contenente il nome univoco della curva
            :return: None
    '''
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


def post_artesian_actual_time_series(data, dict_of_tags, provider, curve_name,
                                     string_granularity, original_timezone='CET'):
    '''
        Carica su Artesian una Actual Time Series
            :param data: Dizionario formattato secondo specifiche Artesian. Chiave(Datetime)->Valore(Float)
            :param dict_of_tags: Dizionario contenente nome e categoria dei tag.
                                    Chiave(Stringa-Categoria)->Valore(ArrayString-> Tags)
            :param provider: Stringa contenente il nome del provider del dato
            :param curve_name: Stringa contenente il nome univoco della curva
            :param string_granularity: Char che indica la granularità secondo lo schema
                            if string_granularity == 'd':
                                return Granularity.Day
                            if string_granularity == 'm':
                                return Granularity.Minute
                            if string_granularity == 'M':
                                return Granularity.Month
                            if string_granularity == 'f':
                                return Granularity.FifteenMinute
                            if string_granularity == 'h':
                                return Granularity.Hour
                            if string_granularity == 'q':
                                return Granularity.Quarter
                            if string_granularity == 't':
                                return Granularity.TenMinute
                            if string_granularity == 'T':
                                return Granularity.ThirtyMinute
                            if string_granularity == 'w':
                                return Granularity.Week
                            if string_granularity == 'y':
                                return Granularity.Year
            :param original_timezone: Stringa contenente il nome della timezone originale della curve, es 'CET'
            :return: None
    '''
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=get_granularity(string_granularity),
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone=original_timezone,
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
    '''
        Rende un dataframe in formato Artesian rappresentatne le Actual Time Series richieste
            :param arr_id_curva: Array d'interi rappresentanti le curve richieste
            :param str_data_inizio_estrazione: Stringa rappresentante la data d'inizio estrazione nel formato YYYY-MM-DD
            :param str_data_fine_estrazione: Stringa rappresentante la data di fine estrazione nel formato YYYY-MM-DD
            :param string_granularity: Char che indica la granularità secondo lo schema
                            if string_granularity == 'd':
                                return Granularity.Day
                            if string_granularity == 'm':
                                return Granularity.Minute
                            if string_granularity == 'M':
                                return Granularity.Month
                            if string_granularity == 'f':
                                return Granularity.FifteenMinute
                            if string_granularity == 'h':
                                return Granularity.Hour
                            if string_granularity == 'q':
                                return Granularity.Quarter
                            if string_granularity == 't':
                                return Granularity.TenMinute
                            if string_granularity == 'T':
                                return Granularity.ThirtyMinute
                            if string_granularity == 'w':
                                return Granularity.Week
                            if string_granularity == 'y':
                                return Granularity.Year
            :param time_zone: Stringa contenente il nome della timezone originale della curve, es 'CET'
            :return: Pandas DataFrame
    '''
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


def get_artesian_actual_data(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione, **arguments):
    ganularity, time_zone, str_extaction_window, relative_period, period, period_start, period_end, \
    filler_strat, custom, max_older, end_value, time_trans = get_correct_args(arguments)
    cfg = get_configuration()

    qs = QueryService(cfg)
    qs.createVersioned() \
        .forMarketData(arr_id_curva) \
        .inTimeZone(time_zone) \
        .inGranularity(get_granularity(ganularity))
    qs = get_extraction_window(qs, str_extaction_window, str_data_inizio_estrazione=str_data_inizio_estrazione,
                               str_data_fine_estrazione=str_data_fine_estrazione, relative_period=relative_period,
                               period=period, period_start=period_start, period_end=period_end)
    qs = get_filler_strategy(qs, filler_strat, custom=custom, max_older=max_older, end_value=end_value)
    if time_trans:
        qs = qs.withTimeTransform(time_trans)

    data = qs.execute()
    df_curva = pd.DataFrame(data)
    return df_curva

def get_artesian_data_actual_daily(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione):
    '''
        Rende un dataframe con granularità giornaliera, in formato Artesian, rappresentatne le Actual Time Series richieste
            :param arr_id_curva: Array d'interi rappresentanti le curve richieste
            :param str_data_inizio_estrazione: Stringa rappresentante la data d'inizio estrazione nel formato YYYY-MM-DD
            :param str_data_fine_estrazione: Stringa rappresentante la data di fine estrazione nel formato YYYY-MM-DD
            :return: Pandas DataFrame
    '''
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
