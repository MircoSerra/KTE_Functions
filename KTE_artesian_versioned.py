from KTE_artesian_general import *


def get_version(query, time_version, version_info1='P0Y1M0D', version_info2='P0Y1M0D'):
    if time_version == 'muv':
        return query.forMUV()
    elif time_version == 'lastNVersion':
        return query.forLastNVersions(version_info1)
    elif time_version == 'version':
        return query.forVersion(version_info1)
    elif time_version == 'lastOfDays':
        if version_info2:
            return query.forLastOfDays(version_info1, version_info2)
        else:
            return query.forLastOfDays(version_info1)
    elif time_version == 'lastOfMonths':
        if version_info2:
            return query.forLastOfMonths(version_info1, version_info2)
        else:
            return query.forLastOfMonths(version_info1)
    elif time_version == 'mostRecent':
        if version_info2:
            return query.forMostRecent(version_info1, version_info2)
        else:
            return query.forMostRecent(version_info1)


def get_correct_args_versioned(arguments):
    try:
        version = arguments['version']
    except:
        version = 'muv'
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
    try:
        version_info1 = arguments['version_info1']
    except:
        version_info1 = 'P0Y1M0D'
    try:
        version_info2 = arguments['version_info2']
    except:
        version_info2 = 'P0Y1M0D'
    return version, ganularity, time_zone, str_extaction_window, relative_period, period, period_start, period_end, \
           filler_strat, custom, max_older, end_value, time_trans, version_info1, version_info2


###################################################
#'''''''''''''''''   Formatter  ''''''''''''''''''#
###################################################


def get_artesian_dict_versioned(df, column_name, column_date_name, correttore=False):
    '''
        La funzione rende il dizionario utile al caricamento dei dati su Artesian.
        Il parametro column_name contiene il nome della colonna che si desidera caricare come valore della time series.
        Il parametro column_date_name contiene il nome della colonna che contiene la data.
            :param df: Pandas DataFrame
            :param column_name:  Stringa
            :param column_date_name:  Stringa
            :param correttore:  Booleano
            :return: Dizionario formattato secondo specifiche Artesian
    '''
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


#
def get_artesian_dict_versioned_monthly(df, column_name, column_date_name):
    '''
        Rende un dizionario per il caricamento dati su Artesian ma con granularità mensile
            :param df: Pandas DataFrame
            :param column_name: Stringa
            :param column_date_name: Stringa
            :return: Dizionario formattato secondo specifiche Artesian
    '''
    tempi = df.apply(lambda x: tempo.format_date(x[column_date_name]), axis=1).tolist()
    values = df[column_name].values.tolist()
    dict_of_value_to_send = dict()
    for i in tempi:
        giorno_ora = datetime(tempi[i].year, tempi[i].month, 1, 0)
        dict_of_value_to_send[giorno_ora] = values[i]
    return dict_of_value_to_send


def get_artesian_dict_versioned_daily_by_index(df, column_name):
    '''
        Rende un dizionario per il caricamento dati su Artesian ma con granularità giornaliera
            :param df: Pandas DataFrame
            :param column_name: Stringa
            :return: Dizionario formattato secondo specifiche Artesian
    '''
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
    '''
        Carica su Artesian una Versioned Time Series
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
            :param version: Datetime contenente la versione desiderata
            :param original_timezone: Stringa contenente il nome della timezone originale della curve, es 'CET'
            :return: None
    '''
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
    '''
        Carica su Artesian una Versioned Time Series con ganularità oraria
            :param data: Dizionario formattato secondo specifiche Artesian. Chiave(Datetime)->Valore(Float)
            :param dict_of_tags: Dizionario contenente nome e categoria dei tag.
                                    Chiave(Stringa-Categoria)->Valore(ArrayString-> Tags)
            :param provider: Stringa contenente il nome del provider del dato
            :param curve_name: Stringa contenente il nome univoco della curva
            :param version: Datetime contenente la versione desiderata
            :return: None
    '''
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

def get_artesian_versioned(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione,
                                ganularity='h', time_zone='CET'):
    '''
        Rende la query alle curve, per avere i dati bisognerà chiedere la versione al dato restituito
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
            :return: QueryService, Oggetto contenuto nell'sdk Artesian
    '''
    cfg = get_configuration()
    qs = QueryService(cfg)
    return qs.createVersioned() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone(time_zone) \
        .inGranularity(get_granularity(ganularity))


def get_artesian_versioned_data(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione, **arguments):
    '''
        Rende la query alle curve, per avere i dati bisognerà chiedere la versione al dato restituito
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
            :return: DataFrame conenente le curve Artesian desiderate
    '''
    version, ganularity, time_zone, str_extaction_window, relative_period, period, period_start, period_end, \
    filler_strat, custom, max_older, end_value, time_trans, \
    version_info1, version_info2 = get_correct_args_versioned(arguments)

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
    qs = get_version(qs, version, version_info1, version_info2)
    return qs.execute()

###################################################
# ''''''''''''''''''   UPDATE  ''''''''''''''''''''
###################################################
