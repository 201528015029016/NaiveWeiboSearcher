#!/usr/env/python
# -*- coding: gb2312 -*-

import pymongo
import jieba
import re
import time
#import thread

class Searcher:

	def __init__(this):
		this.database = pymongo.database.Database(pymongo.MongoClient('localhost',27017), u'InvertedIndexDatabase')
		this.indexDB = this.database.indexDB
		this.articleDB = this.database.articleDB
		this.topicIndexDB = this.database.topicIndexDB
		this.topicDB = this.database.topicDB
		this.articleList = []
		this.topicList = []

	'''
	sortOrder == 0: related
	sortOrder == 1: time
	'''
	
	def SearchArticle(this, seglist, sortOrder):
		listOfIndex = []
		articleList = {}
		
		try:
			for item in seglist:
				result = this.indexDB.find({u'Word': item})
				for post in result:
					i = 0
					for item in post[u'IndexTable']:
						weight = articleList.setdefault(item[0], [0.0,0])
						articleList[item[0]][0] = weight[0] + item[1]
						articleList[item[0]][1] = weight[1] + 1
						i = i+1
			if sortOrder==0:
				this.articleList = sorted(articleList.iteritems(), cmp=lambda x,y:cmp(y[1][1],x[1][1]) or cmp(y[1][0],x[1][0]) or cmp(y[0],x[0]))
			else:
				this.articleList = sorted(articleList.iteritems(), cmp=lambda x,y:cmp(y[0],x[0]) or cmp(y[1][1],x[1][1]) or cmp(y[1][0],x[1][0]))
		except:
			print('error')
			raise

	def SearchRelated(this, seglist):
		listOfIndex = []
		topicList = {}
		
		try:
			for item in seglist:
				result = this.topicIndexDB.find({u'Word': item})

				for post in result:
					idf = post[u'IDF']
					for item in post[u'IndexTable']:
						weight = topicList.setdefault(item, [0.0,0])
						topicList[item][0] = weight[0] + idf
						topicList[item][1] = weight[1] + 1

			this.topicList = sorted(topicList.iteritems(), cmp=lambda x,y:cmp(y[1][1],x[1][1]) or cmp(y[1][0],x[1][0]))
		except:
			print('error')
			raise
		
	def Search(this, searchString, sortOrder):
		seglist = jieba.cut_for_search(searchString)
		timeSummary = {}
		articleList = []
		topicList = []
		this.SearchArticle(seglist, sortOrder)
		this.SearchRelated(seglist)
		print('search complete')
		i = 1
		for article in this.articleList:
			for post in this.articleDB.find({u'DocID':article[0]}):
				postTime = time.strptime((post[u'Time'].split(' '))[0], u'%Y-%m-%d')
				timeSummary.setdefault(postTime, 0)
				timeSummary[postTime] = timeSummary[postTime] + 1
				articleList.append(post)
			i = i+1
			if i>100:
				break
				
		for topic in this.topicList:
			for post in this.topicDB.find({u'TopicID':topic[0]}):
				topicList.append(post)
		
		finalResult = {
			u'Article': articleList,
			u'Topic': topicList,
			u'Summary': sorted(timeSummary.iteritems(),cmp = lambda x,y:cmp(x[0],y[0]))
			}
		return finalResult
		
	def ReOrderLast(this, sortOrder):
		if sortOrder==0:
			this.articleList.sort(cmp=lambda x,y:cmp(y[1][1],x[1][1]) or cmp(y[1][0],x[1][0]) or cmp(y[0],x[0]))
		else:
			this.articleList.sort(cmp=lambda x,y:cmp(y[0],x[0]) or cmp(y[1][1],x[1][1]) or cmp(y[1][0],x[1][0]))
		articleList = []
		i = 1
		for article in this.articleList:
			for post in this.articleDB.find({u'DocID':article[0]}):
				articleList.append(post)
			i = i+1
			if i>100:
				break
		return articleList

searcher = Searcher()
result = searcher.Search(u'我相信我是傻逼',0)
print('search result analysis complete')
with open('dsb.log','w') as output:
	output.write('related: ')
	for topic in result[u'Topic']:
		output.write('%s\n'%(topic[u'Topic'].encode('utf-8')))
	output.write('\n')
	for article in result[u'Article']:
		output.write('%s\n'%(article[u'User'].encode('utf-8')))
		output.write('%s\n'%(article[u'Article'].encode('utf-8')))
		output.write('%s\n\n'%(article[u'Time'].encode('utf-8')))
		
	for item in result[u'Summary']:
		output.write('%s:\t'%(time.strftime('%Y-%m-%d',item[0])))
		output.write('%d\n'%(item[1]))
		
	result2 = searcher.ReOrderLast(1)
	print('reorder complete')
	for article in result2:
		output.write('%s\n'%(article[u'User'].encode('utf-8')))
		output.write('%s\n'%(article[u'Article'].encode('utf-8')))
		output.write('%s\n\n'%(article[u'Time'].encode('utf-8')))

