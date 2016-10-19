# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
from datafeed.tests.test_tdx import *

if __name__ == '__main__':
    # 日线数据的导出
    daily_input = "D:\\new_hbzq\\vipdoc\\sh\\lday"
    daily_output = "D:\\DATA_STK\\day\\sh"
    endswith = '.day'
    export_data(daily_input, daily_output, endswith)

    daily_input = "D:\\new_hbzq\\vipdoc\\sz\\lday"
    daily_output = "D:\\DATA_STK\\day\\sz"
    endswith = '.day'
    export_data(daily_input, daily_output, endswith)








