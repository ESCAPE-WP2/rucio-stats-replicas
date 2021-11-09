#!/usr/bin/env python3

import sqlalchemy
from sqlalchemy import select, MetaData
from sqlalchemy import create_engine

# import misc
import math
import json
import time
import requests
import argparse
from datetime import datetime

# import all needed rucio clients
from rucio.client.rseclient import RSEClient
from rucio.client.scopeclient import ScopeClient
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient

# database preparation
f = open('engine_path','r')
ENGINE_PATH_WIN_AUTH = f.read()

engine = create_engine(ENGINE_PATH_WIN_AUTH)
conn = engine.connect()

# global experiment info
experiments = [
    "SKA", "LSST", "CTA", "LOFAR", "MAGIC", "ATLAS", "FAIR", "CMS", "VIRGO"
]

# Func post to ES
def _post_to_es(es_url, data_dict):
    """
    post data to ES datasource, raise Exception if not successful
    """
    status_code = requests.post(
        es_url,
        data=json.dumps(data_dict),
        headers={"Content-Type": "application/json; charset=UTF-8"})
    status_code.raise_for_status()

def get_qos(rse):
	rse_atr = RSEClient().list_rse_attributes(rse)
	rse_qos = "NULL"
	if 'QOS' in rse_atr:
		rse_qos = rse_atr['QOS']
		return(rse_qos)
	else:
		return(rse_qos)

def get_replicas(push_to_es=False, es_url=None):
	did_client = DIDClient()
	scopes_list = list(ScopeClient().list_scopes())

	rse_client = RSEClient()
	rses_list = list(rse_client.list_rses())

	for scope in scopes_list:
		rse_found, rse_found_bytes = {}, {}
		for rse in rses_list:
			rse_name = rse ['rse']
			rse_qos = get_qos(rse_name)

			#Get amount replicas per RSE
			query_rep = conn.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"+scope+"' AND rse='"+rse_name+"'")
			query_rep_bytes = conn.execute("SELECT SUM(rep.bytes) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"+scope+"' AND rse='"+rse_name+"'")

			tReplicas = query_rep.first()[0]
			tReplicas_bytes = query_rep_bytes.first()[0]
		
			# Replica state
			query_rep_A = conn.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"+scope+"' AND rse='"+rse_name+"' AND state='A'")
			query_rep_B = conn.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='B' AND scope='"+scope+"' AND rse='"+rse_name+"' AND state='B'")
			query_rep_C = conn.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='C' AND scope='"+scope+"' AND rse='"+rse_name+"' AND state='C'")
			query_rep_U = conn.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='U' AND scope='"+scope+"' AND rse='"+rse_name+"' AND state='U'")
			
			# Files, datasets, containers
			tReplicasA = query_rep_A.first()[0]
			tReplicasB = query_rep_B.first()[0]
			tReplicasC = query_rep_C.first()[0]
			tReplicasU = query_rep_U.first()[0]

			# Check if there are replicas:
			if tReplicas > 0:
				# Preparation for the experiment filter:
				experiment_name = 'None'
				for experiment in experiments:
					if scope.startswith(
						experiment) and "test" not in scope and "TEST" not in scope:
						experiment_name = experiment


				# Replica info push
				if push_to_es:
					print("PUSH total")
					rucio_rep_stats = {}
					rucio_rep_stats["producer"] = "escape_wp2"
					rucio_rep_stats["type"] = "alba_rep_stats"
					rucio_rep_stats["timestamp"] = int(time.time())
					rucio_rep_stats["scope"] = scope
					rucio_rep_stats["rse"] = rse_name
					rucio_rep_stats["qos"] = rse_qos
					rucio_rep_stats["experiment"] = experiment_name
					
					rucio_rep_stats["total_replicas"] = tReplicas	
					rucio_rep_stats["total_replicas_bytes"] = tReplicas_bytes

					rucio_rep_stats["total_replicasA"] = tReplicasA
					rucio_rep_stats["total_replicasB"] = tReplicasB	
					rucio_rep_stats["total_replicasC"] = tReplicasC
					rucio_rep_stats["total_replicasU"] = tReplicasU

					_post_to_es(es_url, rucio_rep_stats)


def get_dids(push_to_es=False, es_url=None):
	#query_dids = conn.execute("SELECT COUNT(*) FROM dids WHERE availability='A'")
	#query_dids_bytes = conn.execute("SELECT SUM(bytes) FROM dids WHERE availability='A'")
	#tDids = query_dids.first()[0]
	#tDids_bytes = query_dids_bytes.first()[0]
	#print(tDids)
	#print(tDids_bytes)

	query_f = conn.execute("SELECT count(*) FROM dids WHERE did_type='F' AND availability='A'")
	query_d = conn.execute("SELECT count(*) FROM dids WHERE did_type='D' AND availability='A'")
	query_c = conn.execute("SELECT count(*) FROM dids WHERE did_type='C' AND availability='A'")

	tFiles = query_f.first()[0]
	tDatasets = query_d.first()[0]
	tContainers = query_c.first()[0]

	if push_to_es:
		print("PUSH DIDS INFO")
		rucio_dids_stats = {}
		rucio_dids_stats["producer"] = "escape_wp2"
		rucio_dids_stats["type"] = "alba_dids_stats"
		rucio_dids_stats["timestamp"] = int(time.time())
		#rucio_dids_stats["total_dids"] = tDids
		#rucio_dids_stats["total_dids_bytes"] = tDids_bytes	

		rucio_dids_stats["total_files"] = tFiles
		rucio_dids_stats["total_datasets"] = tDatasets
		rucio_dids_stats["total_containers"] = tContainers
						
		_post_to_es(es_url, rucio_dids_stats)

def main():
	parser = argparse.ArgumentParser(description="Rucio Stats Interconnection Probe")

	parser.add_argument("--push",required=False,action='store_true',default=False,help="Push to an ES datasource")

	parser.add_argument("--url",required=False,dest='es_url',help="ES datasource url")

	args = parser.parse_args()
	push_to_es = args.push
	es_url = args.es_url
	
	if push_to_es and es_url is None:
		parser.error("--push requires --url.")


	it = datetime.now()
	init_time = it.strftime("%H:%M:%S")
	print("Current Time =", init_time)
	get_dids(push_to_es,es_url)
	get_replicas(push_to_es,es_url)

	et = datetime.now()
	end_time = et.strftime("%H:%M:%S")
	print("Current Time =", end_time)

if __name__ == '__main__':
    main()
