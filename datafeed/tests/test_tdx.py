# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""

from datafeed.providers.dzh import *
from datafeed.dividend import *
from datafeed.providers.tdx import *
from datetime import *
import pandas as pd
import numpy as np

io = DzhDividend(r'D:\dzh2\Download\PWR\full.PWR')
r = io.read()

tdx_day = tdx_read(r'D:\new_hbzq\vipdoc\sz\lday\sz300352.day')
tdx_5min = tdx_read(r'D:\new_hbzq\vipdoc\sz\minline\sz300352.lc1', file_ext='lc1')


# http://bbs.pinggu.org/thread-3668396-1-1.html
def print_divs(divs):
    if len(divs) > 0:
        d_ = pd.DataFrame(divs)
        d_ = d_.sort_values(by='time')

        d_.time = d_.time.apply(lambda x: date.fromtimestamp(x))

    return d_


for data in r:
    symbol = data[0]
    print symbol
    divs = data[1]
    y = tdx_5min
    df = factor(tdx_day, divs)

    a = adjust_with_factor(y, df, y.index[0])

    break

