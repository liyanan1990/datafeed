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
import h5py
import numpy as np

bar_type = np.dtype([
    ('time', np.long),
    ('open', np.float32),
    ('high', np.float32),
    ('low', np.float32),
    ('close', np.float32),
    ('amount', np.float32),
    ('volume', np.uint32)
])

# 'time', 'pre_day', 'pre_close', 'split', 'purchase', 'purchase_price', 'dividend', 'dr_pre_close', 'dr_factor', 'backward_factor', 'forward_factor'
dividend_type = np.dtype([
    ('time', np.float64),
    ('pre_day', np.float64),
    ('pre_close', np.float64),
    ('split', np.float64),
    ('purchase', np.float64),
    ('purchase_price', np.float64),
    ('dividend', np.float64),
    ('dr_pre_close', np.float64),
    ('dr_factor', np.float64),
    ('backward_factor', np.float64),
    ('forward_factor', np.float64),
])


def is_stock(symbol):
    symbol_tolower = symbol.upper()
    if symbol_tolower.startswith('SZ00') or symbol_tolower.startswith('SZ30') or symbol_tolower.startswith('SH6'):
        return True
    return False


def is_index(symbol):
    symbol_tolower = symbol.upper()
    if symbol_tolower.startswith('SH000001'):
        return True
    return False


def bars_to_h5(input_path, data):
    ff = h5py.File(input_path, 'w')

    r = data.to_records()
    d = np.array(r, dtype=bar_type)

    # 这种写法没有表头
    # d = data.as_matrix()

    ff.create_dataset('BarData', data=d, compression="gzip", compression_opts=6)
    ff.close()
    return


def dividend_to_h5(input_path, data):
    ff = h5py.File(input_path, 'w')

    r = data.to_records()
    d = np.array(r, dtype=dividend_type)

    # 这种写法没有表头
    # d = data.as_matrix()

    ff.create_dataset('Dividend', data=d, compression="gzip", compression_opts=6)
    ff.close()
    return


def export_data(tdx_input, tdx_output, endswith):
    # 三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
    for parent, dirnames, filenames in os.walk(tdx_input):
        for filename in filenames:  # 输出文件信息
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
            daily_output_path = os.path.join(tdx_output, filename + '.h5')
            tdx_day = tdx_read(daily_input_path, file_ext=sufix)

            bars_to_h5(daily_output_path, tdx_day)

            print daily_output_path


def export_data_exclude(tdx_input, tdx_output, endswith, exclude_input):
    # 三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
    for parent, dirnames, filenames in os.walk(tdx_input):
        for filename in filenames:  # 输出文件信息
            # 用于将通达信的两种5分钟数据区分开来
            if not filename.endswith(endswith):
                continue

            # 将6,0,3以外的都过滤掉
            if not is_stock(filename) and not is_index(filename):
                continue

            exclude_path = exclude_input + filename + '.h5'
            if os.path.exists(exclude_path):
                print '%s file is exists' % filename
                continue

            sufix = os.path.splitext(filename)[1][1:]
            daily_input_path = os.path.join(parent, filename)
            daily_output_path = os.path.join(tdx_output, filename + '.h5')
            tdx_day = tdx_read(daily_input_path, file_ext=sufix)

            bars_to_h5(daily_output_path, tdx_day)

            print daily_output_path


def export_dividend(tdx_input, dzh_input, dzh_output, daily_output):
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
            daily_input_path = tdx_input + "sh\\lday\\" + symbol.lower() + '.day'
            dividend_output_path = daily_output + "sh\\" + symbol.lower() + '.day.h5'
        else:
            daily_input_path = tdx_input + "sz\\lday\\" + symbol.lower() + '.day'
            dividend_output_path = daily_output + "sz\\" + symbol.lower() + '.day.h5'

        try:
            # 宏证证券000562退市了，导致找不到股票行情，但还有除权数据
            tdx_day = tdx_read(daily_input_path)

            df = factor(tdx_day, divs)

            # 存csv
            # dividend_output_path = dzh_output + symbol + '.csv'
            # df.to_csv(dividend_output_path)

            # 存mat
            # dividend_output_path = dzh_output + symbol + '.mat'
            # sio.savemat(dividend_output_path, {'divs': df.as_matrix()}, do_compression=True)


            bars_to_h5(dividend_output_path, tdx_day)

            dividend_output_path = dzh_output + symbol + '.h5'
            dividend_to_h5(dividend_output_path, df)

            # 感觉这里可以边处理除权，一边处理日线

        except Exception as e:
            print e


if __name__ == '__main__':
    # # 日线数据的导出
    # daily_input = "D:\\new_hbzq\\vipdoc\\sh\\lday"
    # daily_output = "D:\\DATA_STK\\day\\sh"
    # endswith = '.day'
    # export_data(daily_input, daily_output, endswith)
    # exit

    # 复权因子的导出,OK
    dividend_input = r'D:\dzh2\Download\PWR\full.PWR'
    dividend_output = 'D:\\DATA_STK\\dividend\\'
    daily_input = "D:\\new_hbzq\\vipdoc\\"
    export_dividend(daily_input, dividend_input, dividend_output)

    # # 日线数据的导出
    # # daily_input = "D:\\new_hbzq\\vipdoc\\sh\\lday"
    # # daily_output = "D:\\DATA_STK\\day\\sh"
    # # export_data(daily_input, daily_output)
    #
    # # daily_input = "D:\\new_hbzq\\vipdoc\\sz\\lday"
    # # daily_output = "D:\\DATA_STK\\day\\sz"
    # # export_data(daily_input, daily_output)
    #
    # # 五分钟线的导出
    # daily_input = "D:\\new_hbzq\\vipdoc\\sh\\fzline"
    # daily_output = "D:\\DATA_STK\\5_tdx\\sh"
    # endswith = '.lc5'
    # export_data(daily_input, daily_output, endswith)
    # daily_input = "D:\\new_hbzq\\vipdoc\\sz\\fzline"
    # daily_output = "D:\\DATA_STK\\5_tdx\\sz"
    # endswith = '.lc5'
    # export_data(daily_input, daily_output, endswith)
