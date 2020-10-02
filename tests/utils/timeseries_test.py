import pandas as pd

from data_science_pipeline.utils.timeseries import (
    to_date_isoformat,
    get_month_start_date,
    get_quarter_start_date,
    get_year_start_date,
    get_week_start_date,
    get_quarter_week_date,
    filter_by_month,
    filter_by_quarter,
    filter_by_year
)


class TestGetMonthStartDate:
    def test_should_return_passed_in_date_if_already_month_start(self):
        assert (
            to_date_isoformat(get_month_start_date(pd.to_datetime('2020-11-01')))
            == '2020-11-01'
        )

    def test_should_return_start_of_month_for_second_day_of_month(self):
        assert (
            to_date_isoformat(get_month_start_date(pd.to_datetime('2020-11-02')))
            == '2020-11-01'
        )

    def test_should_return_start_of_month_for_last_day_of_month(self):
        assert (
            to_date_isoformat(get_month_start_date(pd.to_datetime('2020-11-30')))
            == '2020-11-01'
        )


class TestGetQuarterStartDate:
    def test_should_return_passed_in_date_if_already_quarter_start(self):
        assert (
            to_date_isoformat(get_quarter_start_date(pd.to_datetime('2020-10-01')))
            == '2020-10-01'
        )

    def test_should_return_start_of_quarter_for_second_day_of_quarter(self):
        assert (
            to_date_isoformat(get_quarter_start_date(pd.to_datetime('2020-10-02')))
            == '2020-10-01'
        )

    def test_should_return_start_of_quarter_for_last_day_of_quarter(self):
        assert (
            to_date_isoformat(get_quarter_start_date(pd.to_datetime('2020-12-31')))
            == '2020-10-01'
        )


class TestGetYearStartDate:
    def test_should_return_passed_in_date_if_already_year_start(self):
        assert (
            to_date_isoformat(get_year_start_date(pd.to_datetime('2020-01-01')))
            == '2020-01-01'
        )

    def test_should_return_start_of_quarter_for_second_day_of_year(self):
        assert (
            to_date_isoformat(get_year_start_date(pd.to_datetime('2020-01-02')))
            == '2020-01-01'
        )

    def test_should_return_start_of_quarter_for_last_day_of_year(self):
        assert (
            to_date_isoformat(get_year_start_date(pd.to_datetime('2020-12-31')))
            == '2020-01-01'
        )


class TestGetWeekStartDate:
    def test_should_return_passed_in_date_if_already_week_start(self):
        assert (
            to_date_isoformat(get_week_start_date(pd.to_datetime('2020-10-05')))
            == '2020-10-05'
        )

    def test_should_return_passed_in_date_if_second_day_of_week(self):
        assert (
            to_date_isoformat(get_week_start_date(pd.to_datetime('2020-10-06')))
            == '2020-10-05'
        )

    def test_should_return_passed_in_date_if_last_day_of_week(self):
        assert (
            to_date_isoformat(get_week_start_date(pd.to_datetime('2020-10-11')))
            == '2020-10-05'
        )


class TestGetQuarterWeekDate:
    def test_should_return_passed_in_date_if_already_week_start(self):
        assert (
            to_date_isoformat(get_quarter_week_date(pd.to_datetime('2020-10-05')))
            == '2020-10-05'
        )

    def test_should_return_beginning_of_quarter_if_week_start_is_before_quarter(self):
        assert (
            to_date_isoformat(get_quarter_week_date(pd.to_datetime('2020-10-04')))
            == '2020-10-01'
        )


class TestFilterByMonth:
    def test_should_only_keep_entries_within_the_same_month(self):
        assert list(filter_by_month(
            pd.DataFrame({'ds': pd.to_datetime([
                '2020-09-30', '2020-10-01', '2020-10-31', '2020-11-01'
            ])}),
            pd.to_datetime('2020-10-01')
        )['ds'].apply(to_date_isoformat)) == [
            '2020-10-01', '2020-10-31'
        ]


class TestFilterByQuarter:
    def test_should_only_keep_entries_within_the_same_quarter(self):
        assert list(filter_by_quarter(
            pd.DataFrame({'ds': pd.to_datetime([
                '2020-09-30', '2020-10-01', '2020-12-31', '2021-01-01'
            ])}),
            pd.to_datetime('2020-10-01')
        )['ds'].apply(to_date_isoformat)) == [
            '2020-10-01', '2020-12-31'
        ]


class TestFilterByYear:
    def test_should_only_keep_entries_within_the_same_year(self):
        assert list(filter_by_year(
            pd.DataFrame({'ds': pd.to_datetime([
                '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01'
            ])}),
            pd.to_datetime('2020-01-01')
        )['ds'].apply(to_date_isoformat)) == [
            '2020-01-01', '2020-12-31'
        ]
