#!/usr/env/python
# -*- coding: utf-8 -*-

import os
import csv
import pymongo
import jieba
import re

class Indexer:

	def __init__(this):
		this.cutPattern = u'@[-_0-9a-zA-Z\u4e00-\u9fa5]+[:\s]|@[-_0-9a-zA-Z\u4e00-\u9fa5]+$|\[.+\]|#.+#|http://sinaurl\.cn/[0-9a-zA-Z]{5}|http://t\.cn/[0-9a-zA-Z]{7}'
		this.datafilter = re.compile(this.cutPattern)
		
		this.stopPattern = u'^`|~|!|@|#|\$|%|\^|&|\*|\(|\)|_|\+|-|=|\{|\}|\[|\]|\||\\|:|;|<|,|>|\.|\?|/|\s|\'|"|[A-Za-z]|[0-9]+$'
		this.stopfilter = re.compile(this.stopPattern)
		
		with open('./library/stopList.csv','r') as stopListFile:
			this.stopList = set()
			reader = csv.reader(stopListFile)
			for row in reader:
				this.stopList.add(unicode(row[0],'utf-8'))
			
		this.invindex = {}
		this.database = pymongo.database.Database(pymongo.MongoClient('localhost',27017), u'InvertedIndexDatabase')
		this.indexDB = this.database.indexDB
		this.articleDB = this.database.articleDB

	'''
	build invindex from input csv file at 'path'
	input: path to csvfile
	output: no output
	'''
	def Index(this, path):
		docID = 0
		with open(path,'r') as csvfile:
			reader = csv.reader(csvfile,delimiter='\t')
			for row in reader:		
				docDict = {}
				segtext = this.datafilter.split(unicode(row[2], 'utf-8'))
				seglist = []
				for item in segtext:
					seglist.extend(jieba.cut(item))
				for item in seglist:
					if item not in this.stopList and not re.match(this.stopPattern,item):
						tf = docDict.setdefault(item, 0)
						docDict[item] = tf + 1
				for item in docDict.iteritems():
					table = this.invindex.setdefault(item[0],{})
					table.setdefault(docID,item[1])	
				
				postArticle = {
					u'DocID': docID, 
					u'User': unicode(row[0], 'utf-8'), 
					u'Time': unicode(row[1], 'utf-8'),#time.strptime(row[1], '%Y-%m-%d %H:%M'),
					u'Article': unicode(row[2], 'utf-8')}
				this.articleDB.save(postArticle)				
				docID = docID + 1
						
		for index in this.invindex.iteritems():
			df = len(index[1])
			table = []
			for item in index[1].iteritems():
				table.append(tuple(item))
			table.sort(cmp=lambda x,y: cmp(x[0],y[0]))
			postIndex = {
				u'Word': index[0],
				u'DF': df,
				u'IndexTable': table}
			this.indexDB.save(postIndex)
'''
	def PrintIndexToFile(this):
		with open('./indexAll.log','w') as output:
			for index in this.invindex.iteritems():
				output.write('%s:%d\n'%(index[0].encode('utf-8'), len(index[1])))
				for item in index[1].iteritems():
					output.write('[%d,%d]\t'%(item[0],item[1]))
				output.write('\n\n')
'''
indexer = Indexer()
try:
	indexer.Index('./library/weibodata.csv')
except Exception,e:
	print(e)
	raise
else:
	print("index build successfully")
#indexer.PrintIndexToFile()

