#!/usr/env/python

import pymongo
import jieba
import re
import time

class Searcher:

	def __init__(this):
		this.database = pymongo.database.Database(pymongo.MongoClient('localhost',27017), u'InvertedIndexDatabase')
		this.indexDB = this.database.indexDB
		this.articleDB = this.database.articleDB
		
	def search(this, searchString):
		seglist = jieba.cut(searchString)
