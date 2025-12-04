import os

data_sep="\t"
data_ext=".dat"
report_timezone="US/Pacific"
default_data_root="data"

#
# max_data_cache is the number of ~16 byte (2 Python floats) held in memory for each
# for logging at 1 s intervals, data accumulates at ~80k s*16 bytes=1.3 MB/day per measurement
# 
max_data_cache=10000000

# utility for getting file name from path, date, origin
def file_for_date(data_root,c_dt,origin,ext='.dat'):
	return(os.path.join(data_root,str(c_dt.year),str(c_dt.month),str(c_dt.day),origin+'.dat'))


