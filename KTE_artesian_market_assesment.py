from KTE_artesian_general import *


# Classe atta a contenere i valori per il filling delle curve market assessment
class FillerValueMarketAssessment:
    '''
        Classe costruita con lo scopo di uniformare i valori passati alle funzioni per il recupero delle curve.
        Serve per modellare un dato da passare come valore di riempimento dei dati mancanti.
    '''
    def __init__(self, settlement=0, apertura=0, chiusura=0, high=0, low=0, volume_paid=0, volume_given=0, volume=0):
        self.settlement = settlement
        self.apertura = apertura
        self.close = chiusura
        self.high = high
        self.low = low
        self.volume_paid = volume_paid
        self.volume_given = volume_given
        self.volume = volume

###################################################
#'''''''''''''''''   Formatter  ''''''''''''''''''#
###################################################


def make_artesian_dict_market_assesment_settlement(df, codifica_colonna):
    '''
       Funzione per la creazione del dizionario per il caricamento di curve di tipo Market Assessment
        Questa funzione permette di caricare un solo valore nel campo settlement.
        Il parametro codifica_colonna è la funzione per la codifica dei nomi dei prodotti dal
        formato contenuto nel dataframe al formato funzionale al caricamento delle curve su Artesian
            :param df: Pandas DataFrame
            :param codifica_colonna: Funzione che codifica la colonna nel modo desiderato
            :return: Dizionario per caricare Market Assessment Time Series su Artesian
    '''
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


def codifica_month(data, offset):
    '''
        Passare la data come Pandas Timestamp e l'offset come intero
        ES. codifica_month(Pandas_Timestamp(maggio 2021), 1) --> Jun-21
            :param data: Pandas Timestamp, timestamp relativo alla pubblicazione del dato
            :param offset: Intero, rappresenta il salto di mesi che si vuole fare
            :return: Stringa contenente la codifica assoluta del periodo alla quale si riferisce il dato
    '''
    data = data + pd.DateOffset(months=offset)
    mese = datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%b')
    return mese + '-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_season(data, offset):
    '''
        Passare la data come Pandas Timestamp e l'offset come intero
        ES. codifica_season(Pandas_Timestamp(maggio 2021), 1) --> Sum-21
            :param data: Pandas Timestamp, timestamp relativo alla pubblicazione del dato
            :param offset: Intero, rappresenta il salto di mesi che si vuole fare
            :return: Stringa contenente la codifica assoluta del periodo alla quale si riferisce il dato
    '''
    data = data + pd.DateOffset(months=offset * 6)
    if data.month < 4 or data.month > 9:
        return 'Win-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')
    else:
        return 'Sum-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_quarter(data, offset):
    '''
        Passare la data come Pandas Timestamp e l'offset come intero
        ES. codifica_quarter(Pandas_Timestamp(maggio 2021), 1) --> Q321
            :param data: Pandas Timestamp, timestamp relativo alla pubblicazione del dato
            :param offset: Intero, rappresenta il salto di mesi che si vuole fare
            :return: Stringa contenente la codifica assoluta del periodo alla quale si riferisce il dato
    '''
    data = data + pd.DateOffset(months=offset * 3)
    return 'Q' + str(data.quarter) + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_year(data, offset):
    '''
        Passare la data come Pandas Timestamp e l'offset come intero
        ES. codifica_year(Pandas_Timestamp(maggio 2021), 1) --> 2022
            :param data: Pandas Timestamp, timestamp relativo alla pubblicazione del dato
            :param offset: Intero, rappresenta il salto di mesi che si vuole fare
            :return: Stringa contenente la codifica assoluta del periodo alla quale si riferisce il dato
    '''
    data = data + pd.DateOffset(years=offset)
    return datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%Y')


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################



def post_artesian_market_assestments_daily(data, dict_of_tags, provider, curve_name):
    '''
        Carica su Artesian una Market Assessment Time Series con granularità giornaliera
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

def post_artesian_actual_time_series(data, dict_of_tags, provider, curve_name,
                                     string_granularity, original_timezone='CET'):
    '''
        Carica su Artesian una Market Assessment Time Series
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
        type=MarketData.MarketDataType.MarketAssessment,
        originalTimezone=original_timezone,
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
    '''

        Rende un dataframe rappresentatne le Market Assessment Time Series richieste
            :param arr_id_curva: Array d'interi rappresentanti le curve richieste
            :param str_data_inizio_estrazione: Stringa rappresentante la data d'inizio estrazione nel formato YYYY-MM-DD
            :param str_data_fine_estrazione: Stringa rappresentante la data di fine estrazione nel formato YYYY-MM-DD
            :param arr_products: Array di stringhe rappresentanti i nomi dei prodotti richiesti. Es: ['Q122']
            :param filler_str: Stringa indicante la strategia di filling da adottare durante l'estrazione.
                                Le varie strategie si indicano secondo lo schema:
                                    - 'no'     : Nessun filling
                                    - 'last'   : Riempimento con l'ultimo valore disponible
                                    - 'custom' : Riempimento dei buchi con il valore passato come parametro
                                    - 'none'   : Riempimento dei buchi con il valore None
            :param filler_value: Custom Class FillerValueMarketAssessment.
                                 Parametro che rappresenta il valore da utilizzare come filler in caso di custom filling
            :return:
    '''
    if filler_str == 'no':
        return get_artesian_data_market_assesment_with_no_fill(arr_id_curva,
                                                               str_data_inizio_estrazione,
                                                               str_data_fine_estrazione,
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
