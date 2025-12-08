from commandwork.worker import Worker
import os
import time
from datetime import datetime as dt
from datetime import timedelta
from datetime import timezone as tzone
from collections import deque,OrderedDict # deque is thread-safe as used here
import requests
import logging
import glob
import numpy as np
import pytz
import json
import portalocker # needed for cross-process awareness
import threading

from datalogger.dlcommon import *

class DataWriter(Worker):
	def Init(self):
		self.loop_count=-1
		self.blocking=False
		self.loop_interval=1
		if not 'data_root' in self.__dict__.keys():
			self.data_root=default_data_root # in execution folder
		if not 'data_url' in self.__dict__.keys():
			self.data_url=None # ignore
		self.data_values={} # to be logged
		self.data_sync=threading.Lock() # need to lock top-level value,cache dicts while modifying with new origins

		# make sure we have write access to root 
		# if any of these throw an exception then handle here
		try:
			if not os.path.isdir(self.data_root):
				os.makedirs(self.data_root)
		except OSError as ox:
			self.logger.error("Failed to create dirs at {0}".format(self.data_root))
			self.data_root=None # will cause exceptions down the line if used
		try:
			# unique-ish name
			tfile_name = 'delete.me.' + str(time.perf_counter()).split('.')[-1]
			# avoid name collisions
			tfile=os.path.join(self.data_root,tfile_name)
			pre_tfile=tfile
			i=0
			while(os.path.exists(tfile)):
				tfile=pre_tfile+'.'+str(i)
				i+=1 
			with portalocker.Lock(tfile,'a+b',timeout=5) as f:
				# arbitrary; just makes sure file is there
				f.write('#'.encode()) 
			os.remove(tfile)
		except OSError as ox:
			self.logger.error("Failed to gain write access to {0}".format(self.data_root))
			self.data_root=None # will cause exceptions down the line if used


	def LogData(self,origin,value,timestamp=None,precision=3):
		if not origin in self.data_values:
			with self.data_sync:
				self.data_values[origin]=deque()
		if timestamp == None:
			ts=dt.timestamp(dt.now(pytz.utc))
		else:
			ts=timestamp
		self.data_values[origin].append([float(ts),float(value),precision])
	def Execute(self):
		# this is where data gets logged periodically
		# since this could be in a completely separate process, made this process aware, which is where the 
		# complexity arises in the data reader
		# note that since every new data timestamp might change the directory boundary, have to check each and every time
		# we log a new timestamp, reading pair			
		for origin in self.safe_dict(self.data_values):
			pairs_for_url=[]
			if len(self.data_values[origin]) > 0:
				# data_path=file_for_date(self.data_root,dt.utcfromtimestamp(self.data_values[origin][0][0]),origin)
				data_path=file_for_date(self.data_root,dt.fromtimestamp(self.data_values[origin][0][0],tzone.utc),origin)
				self.logger.debug("{0} Writing {1} data pairs to (initially) {2}".format(self,len(self.data_values[origin]),data_path))
				while len(self.data_values[origin]) > 0:
					pair_to_write=self.data_values[origin].popleft()
					fmt="{0:.3f}"+data_sep+"{1:."+str(pair_to_write[2])+"e}\n"
					#data_path=file_for_date(self.data_root,dt.utcfromtimestamp(pair_to_write[0]),origin)
					data_path=file_for_date(self.data_root,dt.fromtimestamp(pair_to_write[0],tzone.utc),origin)
					dpath=os.path.dirname(data_path)
					if not os.path.isdir(dpath): # small chance some other proc is making these as we check...
						os.makedirs(dpath,exist_ok=True)
					with portalocker.Lock(data_path,'a+b',timeout=5) as f:
						f.write(fmt.format(pair_to_write[0],pair_to_write[1]).encode('utf-8'))
						pairs_for_url.append(pair_to_write)
			# try to upload
			if self.data_url:
				# TODO: aggregate in one request
				# TODO: add retry logic
				for ptw in pairs_for_url:
					url=self.data_url+"/"+origin
					payload={'time':str(ptw[0]),'reading':str(ptw[1])}
					try:
						requests.get(url,params=payload,timeout=2)
					except Exception as ex:
						self.logger.error("{0} Error data logging to {1}: {2}".format(self,self.data_url,ex))

	# make a thread-safe copy - since there aren't very many origins this should be pretty fast
	# the underlying items in the data dicts are usually deque's which should be thread-safe as used in the writer
	def safe_dict(self,d):
		ret={}
		with self.data_sync:
			for k in d:
				ret[k]=d[k]
		return(ret)
	def Exit(self):
		# this should dump any remaining values if we get Stop()'ed 
		# with values left to write
		self.logger.info(str(self) + " Last Execute before normal exit") 
		self.Execute()
