# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
import os
import sys
from datafeed.dividend import *
from datafeed.providers.tdx import *
from datafeed.providers.dzh import *

import os.path
import scipy.io as sio


def is_stock(symbol):
    symbol_tolower = symbol.upper()
    if symbol_tolower.startswith('SZ0') or symbol_tolower.startswith('SZ3') or symbol_tolower.startswith('SH6'):
        return True
    return False


def is_index(symbol):
    symbol_tolower = symbol.upper()
    if symbol_tolower.startswith('SH000001'):
        return True
    return False


def export_data(tdx_input, tdx_output,endswith):
    # 三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
    for parent, dirnames, filenames in os.walk(tdx_input):
        for filename in filenames:  #输出文件信息
            # 用于将通达信的两种5分钟数据区分开来
            if not filename.endswith(endswith):
                continue

            # 将6,0,3以外的都过滤掉
            if not is_stock(filename) and not is_index(filename):
                continue

            # 只处理某一个合约
            # if not filename.startswith('sh000001'):
            #     continue

            sufix = os.path.splitext(filename)[1][1:]
            daily_input_path = os.path.join(parent, filename)
            daily_output_path = os.path.join(tdx_output, filename+'.mat')
            tdx_day = tdx_read(daily_input_path, file_ext=sufix)

            sio.savemat(daily_output_path, { 'tohlcav': tdx_day.as_matrix()}, do_compression=True)

            print daily_output_path


def export_dividend(tdx_input, dzh_input, dzh_output):
    io = DzhDividend(dzh_input)
    r = io.read()

    for data in r:
        symbol = data[0]
        print symbol
        divs = data[1]

        if not is_stock(symbol):
            continue

        # test，以后要删除
        # if not symbol.startswith('SH600145'):
        #     continue

        if symbol.startswith('SH'):
            daily_input_path = tdx_input + "sh\\lday\\" + symbol.lower() +'.day'
        else:
            daily_input_path = tdx_input + "sz\\lday\\" + symbol.lower() +'.day'

        try:
            # 宏证证券000562退市了，导致找不到股票行情，但还有除权数据
            tdx_day = tdx_read(daily_input_path)

            df = factor(tdx_day, divs)

            # 存csv
            # dividend_output_path = dzh_output + symbol + '.csv'
            # df.to_csv(dividend_output_path)

            # 存mat
            dividend_output_path = dzh_output + symbol + '.mat'
            sio.savemat(dividend_output_path, { 'divs': df.as_matrix()}, do_compression=True)

        except Exception as e:
            print e

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

    # 五分钟线的导出
    daily_input = "D:\\new_hbzq\\vipdoc\\sh\\fzline"
    daily_output = "D:\\DATA_STK\\5_tdx\\sh"
    endswith = '.lc5'
    export_data(daily_input, daily_output, endswith)
    daily_input = "D:\\new_hbzq\\vipdoc\\sz\\fzline"
    daily_output = "D:\\DATA_STK\\5_tdx\\sz"
    endswith = '.lc5'
    export_data(daily_input, daily_output, endswith)







