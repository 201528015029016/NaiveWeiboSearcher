#!/usr/env/python
# -*- coding: utf-8 -*-

import os
import csv
import pymongo
import jieba
import re
import math

cutPattern = u'@[-_0-9a-zA-Z\u4e00-\u9fa5]+[:\s]|@[-_0-9a-zA-Z\u4e00-\u9fa5]+$|\[.+\]|http://sinaurl\.cn/[0-9a-zA-Z]{5}|http://t\.cn/[0-9a-zA-Z]{7}'
datafilter = re.compile(cutPattern)
stopPattern = u'^`|~|!|@|#|\$|%|\^|&|\*|\(|\)|_|\+|-|=|\{|\}|\[|\]|\||\\|:|;|<|,|>|\.|\?|/|\s|\'|"|[A-Za-z]|[0-9]+$'
stopfilter = re.compile(stopPattern)
topicPattern = u'#[^#]+#'
topicFilter = re.compile(topicPattern)
with open('./stopList.csv','r') as stopListFile:
	stopList = set()
	reader = csv.reader(stopListFile)
	for row in reader:
		stopList.add(unicode(row[0],'utf-8'))	
database = pymongo.database.Database(pymongo.MongoClient('localhost',27017), u'InvertedIndexDatabase')
indexDB = database.indexDB
articleDB = database.articleDB
topicIndexDB = database.topicIndexDB
topicDB = database.topicDB

'''
build invindex from input csv file at 'path'
input: path to csvfile
output: no output
'''
def index(path):
	docID = 0
	invindex = {}
	topicSet = set()
	with open(path,'r') as csvfile:
		reader = csv.reader(csvfile,delimiter='\t')
		for row in reader:
			docDict = {}
			segtext = datafilter.split(unicode(row[2], 'utf-8'))
			segTopic = topicFilter.findall(unicode(row[2], 'utf-8'))
			seglist = []
			for item in segtext:
				seglist.extend(jieba.cut_for_search(item))
			for item in seglist:
				if item not in stopList and not re.match(stopPattern,item):
					tf = docDict.setdefault(item, 0)
					docDict[item] = tf + 1
			for item in docDict.iteritems():
				table = invindex.setdefault(item[0],{})
				table.setdefault(docID,item[1])
			for item in segTopic:
				topicSet.add(item)
			postArticle = {
				u'DocID': docID, 
				u'User': unicode(row[0], 'utf-8'), 
				u'Time': unicode(row[1], 'utf-8'),#time.strptime(row[1], '%Y-%m-%d %H:%M'),
				u'Article': unicode(row[2], 'utf-8')
				}
			articleDB.save(postArticle)
			docID = docID + 1
	N = docID
	for index in invindex.iteritems():
		df = len(index[1])
		idf = math.log10(float(N)/float(df))
		table = []
		for item in index[1].iteritems():#calc tf*idf/docID (docID represents time in reverse order)
			rank = round((math.log10(float(item[1]+1))*idf)/(math.log10(float(docID+1))), 3)
			table.append((item[0], rank))
		table.sort(cmp=lambda x,y: cmp(x[1],y[1]))
		postIndex = {
			u'Word': index[0],
			u'DF': df,
			u'IndexTable': table
			}
		indexDB.save(postIndex)
	topicID = 0
	topicindex = {}
	for topic in topicSet:
		seglist = jieba.cut_for_search(topic)
		postTopic = {
			u'TopicID': topicID, 
			u'Topic': topic
			}
		for item in seglist:
			if item not in stopList and not re.match(stopPattern,item):
				table = topicindex.setdefault(item, [])
				table.append(topicID)
		topicDB.save(postTopic)
		topicID = topicID + 1
	N = topicID
	for index in topicindex.iteritems():#topic's tf===1
		postIndex = {
			u'Word': index[0],
			u'IDF': round(math.log10(float(N)/float(len(index[1]))),3),
			u'IndexTable': index[1]
			}
		topicIndexDB.save(postIndex)
try:
	index('./library/weibodata.csv')
except Exception,e:
	print(e)
	raise
else:
	print("index build successfully")