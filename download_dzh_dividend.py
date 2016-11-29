# -*- coding: utf-8 -*-
"""
Created on Fri Jul 08 13:14:05 2016

@author: Kan
"""
import urllib
import urllib2


def callbackfunc(blocknum, blocksize, totalsize):
    '''回调函数
    @blocknum: 已经下载的数据块
    @blocksize: 数据块的大小
    @totalsize: 远程文件的大小
    '''
    percent = 100.0 * blocknum * blocksize / totalsize
    if percent > 100:
        percent = 100
    print "%.2f%%"% percent
	
	
#import urllib2 
#import requests
print "downloading with urllib" 
# ('222.73.103.181', '222.73.103.183')
url = 'http://222.73.103.181/platform/download/PWR/full.PWR'  

#proxies = {'http':'http://192.168.1.60:808'}

#urllib.urlretrieve(url, r"D:\dzh2\Download\PWR\full.PWR",callbackfunc)
#print "Done"


#create the object, assign it to a variable
proxy = urllib2.ProxyHandler({'http': '192.168.1.60:808'})
# construct a new opener using your proxy settings
opener = urllib2.build_opener(proxy)
# install the openen on the module-level
urllib2.install_opener(opener)

local_file = r"D:\dzh2\Download\PWR\full.PWR"

f = urllib2.urlopen(url) 
data = f.read() 
with open(local_file, "wb") as code:     
    code.write(data)
	
print 'Done'







