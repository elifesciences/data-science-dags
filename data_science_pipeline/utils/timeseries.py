from datetime import datetime
import pandas as pd


def to_date_isoformat(d: datetime) -> datetime:
    return d.date().isoformat()


def to_timestamp_isoformat(dt: datetime) -> datetime:
    return dt.strftime('%Y-%m-%dT00:00:00Z')


def get_month_start_date(d: datetime) -> datetime:
    return d + pd.offsets.DateOffset(days=1) - pd.offsets.MonthBegin(1)


def get_quarter_start_date(d: datetime) -> datetime:
    return d + pd.offsets.DateOffset(days=1) - pd.offsets.QuarterBegin(1, startingMonth=1)


def get_year_start_date(d: datetime) -> datetime:
    return d + pd.offsets.DateOffset(days=1) - pd.offsets.YearBegin(1)


def get_week_start_date(d: datetime) -> datetime:
    weekday = d.weekday()
    return d - pd.offsets.DateOffset(days=weekday)


def get_quarter_week_date(d: datetime) -> datetime:
    week_start_date = get_week_start_date(d)
    quarter_start_date = get_quarter_start_date(d)
    if week_start_date < quarter_start_date:
        return quarter_start_date
    return week_start_date


def filter_date_between(
        df: pd.DataFrame, start_date, excl_end_date, date_column='ds') -> pd.DataFrame:
    date_column_ser = df[date_column]
    return df[(date_column_ser >= start_date) & (date_column_ser < excl_end_date)]


def filter_by_month(
        df: pd.DataFrame, month_date, **kwargs) -> pd.DataFrame:
    month_start_date = get_month_start_date(month_date)
    next_month_start_date = month_start_date + pd.offsets.MonthBegin(1)
    return filter_date_between(df, month_start_date, next_month_start_date, **kwargs)


def filter_by_quarter(df: pd.DataFrame, quarter_date, **kwargs) -> pd.DataFrame:
    quarter_start_date = get_quarter_start_date(quarter_date)
    next_quarter_start_date = quarter_start_date + pd.offsets.MonthBegin(3)
    return filter_date_between(df, quarter_start_date, next_quarter_start_date, **kwargs)


def filter_by_year(df: pd.DataFrame, year_date, **kwargs) -> pd.DataFrame:
    year_start_date = get_year_start_date(year_date)
    next_year_start_date = year_start_date + pd.offsets.YearBegin(1)
    return filter_date_between(df, year_start_date, next_year_start_date, **kwargs)
