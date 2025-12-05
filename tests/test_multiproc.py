
import sys
import multiprocessing as mp

sys.path.append("..")
sys.path.append("../datalogger")

from datalogger.datareader import DataReader
from datalogger.datawriter import DataWriter
import logging
import time

logger = logging.getLogger(__name__) 

num_writers = 10
origin = 'testval_multiproc'


class TraceDataWriter(DataWriter):
	def Exit(self):
		self.Execute()
		self.logger.info(self.Prefix() + " Exit: {0}".format(self.settings['id']))

# TODO: figure out how to get the logger output to pytest from separate
# processes
def writer_proc(origin, logvalue):
	dw = DataWriter(settings={'id':logvalue,'loop_interval':0.3})
	dw.Run()
	dw.LogData(origin, logvalue)
	time.sleep(.5)
	dw.Stop()

def test_multiwrite(caplog):
	mp.set_start_method("spawn")
	dr = DataReader()
	dr.stats_interval=1
	dr.stats_interval_counter=1
	dr.Run()
	dr.RebuildCache()
	t_iso, orig_stats = dr.GetCacheStats(origin)
	initial_data_count=0
	if len(orig_stats) > 0:
		assert 'N' in orig_stats
		for sv in orig_stats['N']:
			initial_data_count += sv 
	time.sleep(2)	
	dws = []
	for i in range(num_writers):
		p =  mp.Process(target=writer_proc, args = (origin, i))
		p.start()
		dws.append(p)
	for p in dws:
		p.join()
	dr.RebuildCache()
	latest = dr.GetLatestReadings()
	print(latest)
	assert origin in latest
	assert len(latest[origin]) >= 0
	assert 'reading' in latest[origin]
	# give the stats update loop of the datareader time to catch up
	time.sleep(2)
	t_iso, stats = dr.GetCacheStats(origin)
	assert len(stats) > 0
	assert 'N' in stats
	total_count = 0
	for sv in stats['N']:
		total_count+=sv
	print(stats)
	assert initial_data_count + num_writers == total_count
	
