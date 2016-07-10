#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm

import datetime
import numpy as np
import pandas as pd

from pandas import DataFrame
from pandas import TimeSeries
from pandas import DatetimeIndex


class Dividend(object):
    def __init__(self, div):
        """
        Paramaters:
          div: numpy dividend data.
        """
        assert div['time'] > 0
        assert abs(div['split']) > 0 or \
               abs(div['purchase']) > 0 or \
               abs(div['dividend']) > 0

        self._npd = div

    def adjust(self, frame):
        '''Adjust price, volume of quotes data.
    
        Paramaters
        ----------
        frame: DataFrame of OHLCs.
        '''
        if self.ex_date <= frame.index[0].date():  # no adjustment needed
            return True

        if self.ex_date > datetime.date.today():  # not mature
            return True

        self._divide(frame)
        self._split(frame)

    def _divide(self, frame):
        """divided close price to adjclose column

        WARNING
        =======
        frame should be chronological ordered otherwise wrong backfill.
        """
        if self.cash_afterward == 0:
            return

        cashes = [self.cash_afterward, 0.0]
        adj_day = self.ex_date - datetime.timedelta(days=1)
        indexes = []
        indexes.append(self.d2t(adj_day))
        indexes.append(self.d2t(datetime.date.today()))

        cashes = TimeSeries(cashes, index=indexes)
        ri_cashes = cashes.reindex(frame.index, method='backfill')

        frame['adjclose'] = frame['adjclose'] - ri_cashes

    def _split(self, frame):
        if self.share_afterward == 1:
            return

        splits = [self.share_afterward, 1.0]
        adj_day = self.ex_date - datetime.timedelta(days=1)
        indexes = []
        indexes.append(self.d2t(adj_day))
        indexes.append(self.d2t(datetime.date.today()))

        splits = TimeSeries(splits, index=indexes)
        ri_splits = splits.reindex(frame.index, method='backfill')

        frame['adjclose'] = frame['adjclose'] / ri_splits

    @property
    def ex_date(self):
        return datetime.date.fromtimestamp(self._npd['time'])

    @property
    def cash_afterward(self):
        return self._npd['dividend'] - self._npd['purchase'] * self._npd['purchase_price']

    @property
    def share_afterward(self):
        return 1 + self._npd['purchase'] + self._npd['split']

    def d2t(self, date):
        return datetime.datetime.combine(date, datetime.time())


def adjust(y, divs, capitalize=False):
    """Return fully adjusted OHLCs data base on dividends

    Paramaters:
    y: numpy
    divs: numpy of dividends

    Return:
    DataFrame objects
    """
    # 从通达信中取出来的已经做过处理了
    # index = DatetimeIndex([datetime.datetime.fromtimestamp(v) for v in y['time']])
    # y = DataFrame.from_records(y, index=index, exclude=['time'])
    y['adjclose'] = y['close']

    for div in divs:
        if div['split'] + div['purchase'] + div['dividend'] == 0:
            continue
        d = Dividend(div)
        d.adjust(y)

    factor = y['adjclose'] / y['close']
    frame = y.copy()
    frame['open'] = frame['open'] * factor
    frame['high'] = frame['high'] * factor
    frame['low'] = frame['low'] * factor
    frame['close'] = frame['close'] * factor
    frame['volume'] = frame['volume'] * (1 / factor)

    if capitalize:
        columns = [k.capitalize() for k in frame.columns]
        columns[-1] = 'Adjusted'
        frame.columns = columns
        del (frame['Amount'])
    return frame


def sort_dividend(divs):
    if len(divs) > 0:
        df = DataFrame(divs)
        df = df.sort_values(by='time')
        df.time = df.time.apply(lambda x: pd.datetime.utcfromtimestamp(x))
        df = df.set_index('time')

    return df


# 计算得到向后复权因子
def factor(y, divs):
    # 排序复权因子,
    df = sort_dividend(divs)

    # 平移一下，取昨收盘价
    pre_close = y['close'].shift(1)
    # 担心会不会选不出来？
    df['pre_close'] = pre_close.loc[df.index]

    # 除权价
    df['dr_close'] = (df['pre_close'] - df['dividend'] + df['purchase'] * df['purchase_price']) / (
    1 + df['split'] + df['purchase'])
    # 除权因子
    df['dr_factor'] = df['pre_close'] / df['dr_close']

    # 在最前插件一条特殊的记录，用于表示在第一次除权之前系数为1
    # 由于不知道上市是哪一天，只好用最小日期
    first_ = pd.DataFrame({'dr_factor': 1}, index=[pd.datetime(1900, 1, 1)])
    df = df.append(first_)
    df = df.sort_index()

    # 向后复权因子，注意对除权因子的累乘
    df['backward_factor'] = df['dr_factor'].cumprod()
    # 向前复权因子
    df['forward_factor'] = df['backward_factor'] / float(df['backward_factor'][-1:])

    return df


# 通过复权因子列表计算价格,注意因子列表最好是完整的
def adjust_with_factor(y, f, start_index_datetime):

    insert_ = pd.DataFrame({'dr_factor': 1}, index=[start_index_datetime])
    try:
        f = f.append(insert_, verify_integrity=True)
        f = f.sort_index()
    except ValueError:
        pass

    # 向后复权因子
    f['backward_factor'] = f['dr_factor'].cumprod()
    # 向前复权因子
    f['forward_factor'] = f['backward_factor'] / float(f['backward_factor'][-1:])

    # 只取这两列进行后面的操作，因为其它部分可能是nan，fillna后再dropna可能将数据全删了
    f2 = f[['backward_factor', 'forward_factor']]

    # 注意，日线与分钟线合并，由于时间点不对应，只能使用outer方法，然后看情况删除nan
    df = pd.merge(y, f2, how='outer', left_index=True, right_index=True, sort=True)

    # 只对两种复权因子进行填充
    df[['backward_factor', 'forward_factor']] = df[['backward_factor', 'forward_factor']].fillna(method='pad')

    df = df.dropna()

    return df
