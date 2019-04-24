import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

from airflow.hooks.base_hook import BaseHook


class CreateCalendarHook(BaseHook):
    def __init__(self,
                 start="",
                 end=""):
        self.start_date = start
        self.end_date = end

    def create_dataframe(self):
        dates = pd.DataFrame()
        dates['date'] = pd.date_range(start='1/1/2000', end='12/31/2040')
        dates['key'] = dates['date'].map(
            lambda d: f'{d.year}{d.month:02d}{d.day:02d}')
        dates['year'] = dates['date'].map(lambda d: d.year)
        dates['month'] = dates['date'].map(lambda d: d.month)
        dates['day'] = dates['date'].map(lambda d: d.day)
        cal = USFederalHolidayCalendar()
        holidays = pd.DataFrame()
        holidays['date'] = cal.holidays(start='2000-01-01', end='2040-12-31')
        holidays['holiday'] = True
        dates = pd.merge(dates, holidays, how='left', left_on='date',
                         right_on='date')
        dates['holiday'] = dates.holiday.fillna(False)
        return dates
