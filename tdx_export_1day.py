# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
from datafeed.tests.test_tdx import *

if __name__ == '__main__':
    # 日线数据的导出
    daily_input = "D:\\new_hbzq\\vipdoc\\sh\\lday"
    daily_output = "D:\\DATA_STK_HDF5\\day\\sh"
    endswith = '.day'
    exclude_input = r'D:\DATA_STK_HDF5\day_tmp\sh'+'\\'
    export_data_exclude(daily_input, daily_output, endswith, exclude_input)


    daily_input = "D:\\new_hbzq\\vipdoc\\sz\\lday"
    daily_output = "D:\\DATA_STK_HDF5\\day\\sz"
    endswith = '.day'
    exclude_input = r'D:\DATA_STK_HDF5\day_tmp\sz'+'\\'
    export_data_exclude(daily_input, daily_output, endswith, exclude_input)
