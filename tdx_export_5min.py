# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
from datafeed.tests.test_tdx import *

if __name__ == '__main__':
    # 五分钟线的导出
    daily_input = "D:\\new_hbzq\\vipdoc\\sh\\fzline"
    daily_output = "D:\\DATA_STK\\5_tdx\\sh"
    endswith = '.lc5'
    export_data(daily_input, daily_output, endswith)

    daily_input = "D:\\new_hbzq\\vipdoc\\sz\\fzline"
    daily_output = "D:\\DATA_STK\\5_tdx\\sz"
    endswith = '.lc5'
    export_data(daily_input, daily_output, endswith)







