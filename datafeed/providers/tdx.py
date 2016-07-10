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
import dateutil.parser

__all__ = ['tdx_read',
           ]


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


# 读TDX
# http://www.tdx.com.cn/list_66_68.html
# 通达信本地目录有day/lc1/lc5三种后缀名，两种格式
# 从通达信官网下载的5分钟后缀只有5这种格式
def tdx_read(path, file_ext='day'):
    ohlc_type = {'day': 'i4', '5': 'i4', 'lc1': 'f4', 'lc5': 'f4'}[file_ext]
    date_parser = {'day': day_datetime_parse,
                   '5': min_datetime_parse,
                   'lc1': min_datetime_parse,
                   'lc5': min_datetime_parse,
                   }[file_ext]
    columns = ['time', 'open', 'high', 'low', 'close', 'amount', 'volume', 'na']
    formats = ['i4'] + [ohlc_type] * 4 + ['f4'] + ['i4'] * 2
    dtype = np.dtype({'names': columns, 'formats': formats})
    data = np.fromfile(path, dtype=dtype)
    df = pd.DataFrame(data)
    df.time = df.time.apply(date_parser)
    df = df.set_index('time')

    df = df.drop('na', 1)

    # 有两种格式的数据需要调整
    if file_ext == 'day' or file_ext == '5':
        tmp = df[:10]
        r = tmp.amount / tmp.volume / tmp.close
        # 为了解决价格扩大了多少倍的问题
        type_unit = np.power(10, np.round(np.log10(r))).median()
        df.ix[:, :4] = df.ix[:, :4] * type_unit

    return df


if __name__ == '__main__':
    filename = r"D:\new_hbzq\vipdoc\sz\lday\sz000001.day"
    #filename = r"D:\new_hbzq\vipdoc\sz\minline\sz300352.lc1"
    filename = r"D:\new_hbzq\vipdoc\sz\minline\sz300352.lc1"
    a = tdx_read(filename, file_ext='day')
    print a.head(10)