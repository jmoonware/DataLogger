
import sys

sys.path.append("..")
sys.path.append("../datalogger")

from datalogger.datareader import DataReader
from datalogger.datawriter import DataWriter
import logging
import time

logger = logging.getLogger(__name__) 

num_writers = 10

def test_multiwrite(caplog):
	dws = []
	for i in range(num_writers):
		dw = DataWriter()
		dw.Run()
		dws.append(dw)
	dr = DataReader()
	dr.stats_interval=1
	dr.stats_interval_counter=1
	dr.Run()
	for dn, dw in enumerate(dws):
		dw.LogData('testval', dn)
	time.sleep(1)	
	dr.RebuildCache()
	latest = dr.GetLatestReadings()
	print(latest)
	assert 'testval' in latest
	assert len(latest['testval']) > 0
	assert 'reading' in latest['testval']
	cs = dr.GetCacheStats('testval')
	print(cs)
	
