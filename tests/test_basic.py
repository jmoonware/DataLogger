
import sys

sys.path.append("..")
sys.path.append("../datalogger")

from datalogger.datareader import DataReader
from datalogger.datawriter import DataWriter
import logging
import time

logger = logging.getLogger(__name__) 

origin = 'testval'

def test_basic(caplog):
	dr = DataReader()
	dr.stats_interval=1
	dr.stats_interval_counter=1
	dw = DataWriter()
	dw.Run()
	dr.Run()
	dw.LogData(origin, 1.0)
	time.sleep(1)	
	dr.RebuildCache()
	latest = dr.GetLatestReadings()
	print(latest)
	assert origin in latest
	assert len(latest[origin]) > 0
	assert 'reading' in latest[origin]
	# test_multi sends 0-9 to the files 
	# so latest could be different than 1.0
	assert latest[origin]['reading']<10
	cs = dr.GetCacheStats(origin)
	print(cs)
	
