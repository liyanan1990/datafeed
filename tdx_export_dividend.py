# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
from datafeed.tests.test_tdx import *

if __name__ == '__main__':
    # 复权因子的导出,OK
    dividend_input = r'D:\dzh2\Download\PWR\full.PWR'
    dividend_output = 'D:\\DATA_STK\\dividend\\'
    daily_input = "D:\\new_hbzq\\vipdoc\\"
    export_dividend(daily_input, dividend_input, dividend_output)

    # 日线数据的导出
    # daily_input = "D:\\new_hbzq\\vipdoc\\sh\\lday"
    # daily_output = "D:\\DATA_STK\\day\\sh"
    # export_data(daily_input, daily_output)

    # daily_input = "D:\\new_hbzq\\vipdoc\\sz\\lday"
    # daily_output = "D:\\DATA_STK\\day\\sz"
    # export_data(daily_input, daily_output)








