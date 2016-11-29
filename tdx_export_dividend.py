# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
from datafeed.tests.test_tdx import *

if __name__ == '__main__':
    # 复权因子的导出
    # 因为在生成复权数据时同时还会读取日线数据，可以一同操作
    # 将日线保存，然后再看还有哪些没有生成的，再做一次
    dividend_input = r'D:\dzh2\Download\PWR\full.PWR'
    dividend_output = 'D:\\DATA_STK_HDF5\\dividend\\'
    daily_input = "D:\\new_hbzq\\vipdoc\\"
    daily_output = "D:\\DATA_STK_HDF5\\day_tmp\\"
    export_dividend(daily_input, dividend_input, dividend_output, daily_output)









