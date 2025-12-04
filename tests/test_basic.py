
import sys

sys.path.append("..")
sys.path.append("../datalogger")

from datalogger.datareader import DataReader
from datalogger.datawriter import DataWriter
import logging
import time

logger = logging.getLogger(__name__) 

def test_basic(caplog):
	dr = DataReader()
	dr.stats_interval=1
	dr.stats_interval_counter=1
	dw = DataWriter()
	dw.Run()
	dr.Run()
	dw.LogData('testval', 1.0)
	time.sleep(1)	
	dr.RebuildCache()
	latest = dr.GetLatestReadings()
	print(latest)
	assert 'testval' in latest
	assert len(latest['testval']) > 0
	assert 'reading' in latest['testval']
	assert latest['testval']['reading']==1.0
	cs = dr.GetCacheStats('testval')
	print(cs)
	
