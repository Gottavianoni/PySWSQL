#!/usr/bin/env python
# -*- coding: utf-8 -*-
 

import pandas
import requests
import json
import re
 
"""
    Implémentation de la librairie.
 
    Usage:
 
    >>> import PySWSQL
    >>> getSWquery(proxy = proxy, baseurl = url, 	
"""
 
__all__ = ['swds_json']
 
def swds_json(proxy=None, idcol=None, idstart=None, idstop=None, columns = [], limit = None,* , token , branch , dbpath , urlbase ) :
	#SET PROXY PARAMETERS TO CALL
	proxy_flag = 0
	
	if proxy is not None :
		proxy_flag = 1
		http_proxy  = proxy
		proxyDict = { "http"  : http_proxy, "https" : http_proxy,}
	if len(columns) > 0 :
		columns_flag = 1 
		select_part = "SELECT " + ', '.join(columns) +  " FROM "
	else :
		select_part = "SELECT * FROM "
	if limit is not None :
		filter_part = " LIMIT " + str(limit)
	elif idcol is not None :
		filter_part = " WHERE " + idcol + " BETWEEN " + str(idstart) + " AND " + str(idstop)
	else :
		filter_part = " " 
	from_part = "`" + branch + "`.`" + dbpath + "`"
		
	#RAW RESULTS
	tken = token  
	b_tken = "Bearer " + tken
	url2 = re.sub("\/$","",urlbase) + "/foundry-data-proxy/api/dataproxy/query"
	#HTTP REQUESTS PARAMETERS
	header = { "Authorization" : b_tken,"Content-Type" : "application/json" }
	data = '{"query":' + '"' + select_part + from_part + filter_part + '"}'
	
	#INTEGRATION DU PROXY
	if proxy_flag == 1 :
		res = requests.post(url2 , headers = header , data = data , proxies = proxyDict)
	else :
		res = requests.post(url2 , headers = header , data = data )
	res_json = res.json()
	return(res_json)
 
def speakSW() :
	print("Hello Skywise")