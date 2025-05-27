# 2024-08-21, hannes@duckdblabs.com

import threading
import duckdb
import pathlib
import time
import functools
import operator
import os
import shutil
import psutil
import datetime

scale_factor = int(os.environ.get("SF"))

print(f"Running the TPC-H Benchmark on scale factor {scale_factor}")

datadir = f'gen/sf{scale_factor}'

if (not os.path.exists(datadir)):
	print(f"Data directory {datadir} does not exist, exiting")
	exit(-1)

# from section 5.3.4 of tpch spec
streams = 1

print(f"Scale factor {scale_factor}")
use_parquet = True
reader = 'read_csv'
ext = ''
if use_parquet:
	ext = '.parquet'
	reader = 'read_parquet'
	print("Parquet refresh")
else:
	print("CSV refresh")

logfile = f'log-sf{scale_factor}-{str(datetime.datetime.now(datetime.UTC)).replace(' ', 'T').replace(':', '-')}.tsv'
proceed = True

def monitor():
	log = open(logfile, 'wb')
	print(f"Logging to {logfile}")

	proc = psutil.Process()
	log.write('\t'.join([
		'time_offset',
		'cpu_percent',
		'cpu_user',
		'cpu_system',
		'memory_rss',
		'memory_vms'
		]).encode('utf8') + b'\n')
	start = time.time()
	while proceed:
		cpu_times = proc.cpu_times()
		memory_info = proc.memory_info()
		log.write('\t'.join(str(x) for x in [
			round(time.time()-start,2),
			round(proc.cpu_percent()),
			round(cpu_times.user,2),
			round(cpu_times.system,2),
			memory_info.rss,
			memory_info.vms
			]).encode('utf8') + b'\n')
		time.sleep(1)
		log.flush()

threading.Thread(target=monitor).start()

print(f"Begin loading")
start = time.time()
con0 = duckdb.connect()
con0.sql("ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (DATA_PATH 'data_files');")
con0.sql("USE my_ducklake;")
schema = pathlib.Path('schema.sql').read_text()
con0.execute(schema)
for t in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
	con0.execute(f"COPY {t} FROM '{datadir}/{t}.tbl'")
con0.commit()

load_duration = time.time() - start
print(f"Done loading in {load_duration:.1f} seconds")

def query(n):
	pass

def RF1(n):
	con0.begin()
	lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
	orders = f"{datadir}/orders.tbl.u{n}{ext}"
	con0.execute(f"INSERT INTO lineitem FROM '{lineitem}'")
	con0.execute(f"INSERT INTO orders FROM '{orders}'")
	con0.commit()


def RF2(n):
	con0.begin()
	delete = f"{datadir}/delete.{n}{ext}"
	con0.execute(f"DELETE FROM orders WHERE o_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con0.execute(f"DELETE FROM lineitem WHERE l_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con0.commit()


def RF(con0, n):	
	print(f"start refresh {n}")
	con0.begin()
	lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
	orders = f"{datadir}/orders.tbl.u{n}{ext}"
	delete = f"{datadir}/delete.{n}{ext}"
	con0.execute(f"INSERT INTO lineitem FROM '{lineitem}'")
	con0.execute(f"INSERT INTO orders FROM '{orders}'")
	con0.execute(f"DELETE FROM orders WHERE o_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con0.execute(f"DELETE FROM lineitem WHERE l_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con0.commit()
	print(f"done refresh {n}")


def timeit(fun, p):
	start = time.time()
	fun(p)
	return time.time() - start

def refresh(ns):
	for n in ns:
		RF(con0, n)

n_refresh = 10

start = time.time()
time_rf1 = timeit(RF1, 1)
time_rf2 = timeit(RF2, 1)
refresh_duration = time.time() - start

#tpch_power_at_size = round((3600*scale_factor)/ ((time_q*time_rf1*time_rf2)**(1/24)), 2)
#print(f"tpch_power_at_size              = {tpch_power_at_size:.2f}")

start = time.time()


threads = []
print(f"Running {streams} query streams, {n_refresh} refresh sets")

for i in range(1, streams+1):
	t = threading.Thread(target=query, args=[i])
	t.start()
	threads.append(t)

r = threading.Thread(target=refresh, args=[range(2, n_refresh+2)])
r.start()
threads.append(r)

for t in threads:
	t.join()

proceed = False

throughput_measurement_interval = round(time.time() - start, 2)
#tpch_throughput_at_size = round((streams * 22 * 3600) / throughput_measurement_interval * scale_factor, 2)
#tpch_qphh_at_size = round((tpch_power_at_size * tpch_throughput_at_size)**(1/2), 2)

print()
print(f"tpch_load_time                  = {load_duration:.2f}")
print(f"tpch_refresh_duration           = {refresh_duration:.2f}")
#print(f"throughput_measurement_interval = {throughput_measurement_interval:.2f}")
#print(f"tpch_power_at_size              = {tpch_power_at_size:.2f}")
#print(f"tpch_throughput_at_size         = {tpch_throughput_at_size:.2f}")
#print(f"tpch_qphh_at_size               = {tpch_qphh_at_size:.2f}")
