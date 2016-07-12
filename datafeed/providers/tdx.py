#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

"""通达信数据读取

http://www.cnblogs.com/zeroone/archive/2013/07/10/3181251.html

文件路径
-------
D:\new_hbzq\vipdoc\sh\lday
D:\new_hbzq\vipdoc\sh\fzline
D:\new_hbzq\vipdoc\sh\minline

"""
import numpy as np
import pandas as pd
import scipy.io as sio

__all__ = ['tdx_read',]


def min_datetime_parse(dt):
    # 由于dt << 16会由int变成long，所以转换失败
    # tnum, dnum = dt>>16, dt << 16 >> 16  #little endian
    (tnum, dnum) = divmod(dt, 1 << 16)  # 2**16
    (ym, res) = divmod(dnum, 2048)
    y = ym + 2004
    (m, d) = divmod(res, 100)
    h, t = divmod(tnum, 60)

    return pd.datetime(y, m, d, h, t)


def day_datetime_parse(dt):
    (yyyy, mmdd) = divmod(dt, 10000)
    (m, d) = divmod(mmdd, 100)
    return pd.datetime(yyyy, m, d, 0, 0)


def min_datetime_long(dt):
    # 由于dt << 16会由int变成long，所以转换失败
    # tnum, dnum = dt>>16, dt << 16 >> 16  #little endian
    (tnum, dnum) = divmod(dt, 1 << 16)  # 2**16
    (ym, res) = divmod(dnum, 2048)
    y = ym + 2004
    (m, d) = divmod(res, 100)
    h, t = divmod(tnum, 60)

    return y*100000000+res*10000+h*100+t*1


def day_datetime_long(dt):
    # 传入的数据是到日，需要转成分钟
    return dt*10000


def long_2_datetime(dt):
    (yyyyMMdd, hhmm) = divmod(dt, 10000)
    (yyyy, MMdd) = divmod(yyyyMMdd, 10000)
    (MM, dd) = divmod(MMdd, 100)
    (hh, mm) = divmod(hhmm, 100)

    return pd.datetime(yyyy, MM, dd, hh, mm)

# 读TDX
# http://www.tdx.com.cn/list_66_68.html
# 通达信本地目录有day/lc1/lc5三种后缀名，两种格式
# 从通达信官网下载的5分钟后缀只有5这种格式，为了处理方便，时间精度都只到分钟
def tdx_read(path, file_ext='day'):
    ohlc_type = {'day': 'i4', '5': 'i4', 'lc1': 'f4', 'lc5': 'f4'}[file_ext]
    date_parser = {'day': day_datetime_long,
                   '5': min_datetime_long,
                   'lc1': min_datetime_long,
                   'lc5': min_datetime_long,
                   }[file_ext]
    columns = ['time', 'open', 'high', 'low', 'close', 'amount', 'volume', 'na']
    formats = ['i4'] + [ohlc_type] * 4 + ['f4'] + ['i4'] * 2
    dtype = np.dtype({'names': columns, 'formats': formats})
    data = np.fromfile(path, dtype=dtype)
    df = pd.DataFrame(data)
    # 为了处理的方便，存一套long类型的时间,
    df.time = df.time.apply(date_parser)
    df['datetime'] = df.time.apply(long_2_datetime)
    df = df.set_index('datetime')

    df = df.drop('na', 1)

    # 有两种格式的数据需要调整
    if file_ext == 'day' or file_ext == '5':
        tmp = df[:10]
        r = tmp.amount / tmp.volume / tmp.close
        # 为了解决价格扩大了多少倍的问题
        type_unit = np.power(10, np.round(np.log10(r))).median()
        # 这个地方要考虑到实际情况，不要漏价格，也不要把时间做了除法
        df.ix[:, 1:5] = df.ix[:, 1:5] * type_unit

    return df


if __name__ == '__main__':
    # filename = r"D:\new_hbzq\vipdoc\sz\lday\sz000001.day"
    # a = tdx_read(filename, file_ext='day')
    filename = r"D:\new_hbzq\vipdoc\sz\minline\sz300352.lc1"
    a = tdx_read(filename, file_ext='lc1')
    #filename = r"D:\new_hbzq\vipdoc\sz\minline\sz300352.lc1"

    print a.head(10)

    # 默认是行向量，保存也是行，直接转置不成，只能通过强行改shape才能转成列
    tt = a['time'].as_matrix()
    m, = tt.shape
    tt.shape = (m,1)

    # 分成两部分保存是因为直接全保存会转成single,导致时间丢失
    ohlcv = a.iloc[:, 1:7].as_matrix()

    # 这里是保存为mat文件的示例，MATLAB保存时需要加参数-v6才能被Python读出来，同时还不支持cell
    sio.savemat(r'd:\5.mat', {'time': tt, 'ohlcv': ohlcv}, do_compression=True)
