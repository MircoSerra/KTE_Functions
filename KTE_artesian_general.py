from datetime import datetime
import numpy as np
from Artesian import Granularity, ArtesianConfig
from Artesian import MarketData
from Artesian.MarketData import MarketDataService
from Artesian.Query import QueryService
import Artesian as art
from dateutil import tz
import pytz
import src.KTE_artesian.KTE_time as tempo
import src.KTE_artesian.settings_and_imports as setting
import pandas as pd


# In via di deprecamento
def format_artesian_data(df_artesian):
    '''
        Trasforma il dataframe ricevuto da artesian nella forma "Colonne: Nomi curve" / "Righe: Timestamp"
        :param df_artesian:
        :return df_formattato:
    '''
    list_of_rows_names = df_artesian['C'].unique().tolist()
    dizionario_generatore_dataframe = dict()
    for i in df_artesian.index:
        try:
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']][df_artesian.loc[i]['C']] = df_artesian.loc[i]['D']
        except KeyError:
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']] = dict()
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']][df_artesian.loc[i]['C']] = df_artesian.loc[i]['D']

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


# Rende l'oggetto che autentica e configura la connessione con artesian
def get_configuration():
    '''
        Autentica l'utente alla connessione con Artesian
    :return: None
    '''
    return ArtesianConfig(setting.Settings.URL_SERVER, setting.Settings.API_KEY)


# Cancella tutte le curve i cui id sono contenuti nell'array d'input
def delete_curve(arr_cureve_id):
    '''
        Cancella tutte le curve contenute nel array passato come parametro.
        L'array deve contenere gl'id delle curve
    :param arr_cureve_id:
    :return:
    '''
    cfg = get_configuration()
    mkservice = MarketDataService(cfg)

    for c_id in arr_cureve_id:
        mkservice.deleteMarketData(c_id)


# Rende l'oggetto granularity desiderato
def get_granularity(string_granularity):
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


def get_extraction_window(query, str_type, **extract_data):
    if str_type == 'Abs':
        return query.inAbsoluteDateRange(extract_data['str_data_inizio_estrazione'],
                                         extract_data['str_data_fine_estrazione'])
    if str_type == 'Relative':
        return query.inRelativeInterval(extract_data['relative_period'])
    if str_type == 'Period':
        return query.inRelativePeriod(extract_data['period'])
    if str_type == 'Period range':
        return query.inRelativePeriodRange(extract_data['period_start'], extract_data['period_end'])


def get_relative_interval(str_interval):
    if str_interval == 'Month to date':
        return art.Query.RelativeInterval.MonthToDate
    if str_interval == 'Quarter to date':
        return art.Query.RelativeInterval.QuarterToDate
    if str_interval == 'Rolling month':
        return art.Query.RelativeInterval.RollingMonth
    if str_interval == 'Rolling quarter':
        return art.Query.RelativeInterval.RollingQuarter
    if str_interval == 'Rolling week':
        return art.Query.RelativeInterval.RollingWeek
    if str_interval == 'Rolling year':
        return art.Query.RelativeInterval.RollingYear
    if str_interval == 'Week to date':
        return art.Query.RelativeInterval.WeekToDate
    if str_interval == 'Year to date':
        return art.Query.RelativeInterval.YearToDate


def get_filler_strategy(query, fill_strat, **fill_values):
    if fill_strat == 'null':
        return query.withFillNull()
    elif fill_strat == 'none':
        return query.withFillNone()
    elif fill_strat == 'customValue':
        return query.withFillCustomValue(fill_values['custom'])
    elif fill_strat == 'latestValue':
        if fill_values['end_value']:
            return query.withFillLatestValue(fill_values['max_older'], fill_values['end_value'])
        else:
            return query.withFillLatestValue(fill_values['max_older'])
