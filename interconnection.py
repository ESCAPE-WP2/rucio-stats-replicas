#!/usr/bin/env python3

# import misc
import math
import json
import time
import requests
import argparse
from datetime import datetime

from rucio.db.sqla import session

session = session.get_session()

# Experiments names
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

# Func get QoS for each RSE
def get_qos(rse):
	query_get_qos = session.execute("SELECT VALUE a FROM RSE_ATTR_MAP a INNER JOIN RSES r ON r.id=a.rse_id AND a.KEY='QOS' AND r.RSE='"+rse+"'").fetchone()
	if not query_get_qos:
		rse_qos = "NULL"
		return(rse_qos)
	else:
		rse_qos = query_get_qos[0]
		return(rse_qos)

# Main func
def get_replicas(push_to_es=False, es_url=None):
	query_get_scopes = session.execute("SELECT SCOPE FROM SCOPES")
	scopes_list = []
	for row in query_get_scopes:
		scopes_list.append(row[0])

	query_get_rses = session.execute("SELECT RSE FROM RSES")
	rses_list = []
	for row in query_get_rses:
		rses_list.append(row[0])

	for scope in scopes_list:
		rse_found, rse_found_bytes = {}, {}
		for rse in rses_list:
			rse_qos = get_qos(rse)
			
			#Get amount replicas per RSE
			tReplicas = session.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"+scope+"' AND rse='"+rse+"'").fetchone()
			tReplicas_bytes = session.execute("SELECT SUM(rep.bytes) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"+scope+"' AND rse='"+rse+"'").fetchone()

			# Replica state
			tReplicasA = session.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"+scope+"' AND rse='"+rse+"' AND state='A'").fetchone()
			tReplicasB = session.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='B' AND scope='"+scope+"' AND rse='"+rse+"' AND state='B'").fetchone()
			tReplicasC = session.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='C' AND scope='"+scope+"' AND rse='"+rse+"' AND state='C'").fetchone()
			tReplicasU = session.execute("SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='U' AND scope='"+scope+"' AND rse='"+rse+"' AND state='U'").fetchone()

			# Check if there are replicas:
			if tReplicas[0] > 0:
				# Preparation for the experiment filter:
				experiment_name = 'None'
				for experiment in experiments:
					if scope.startswith(
						experiment) and "test" not in scope and "TEST" not in scope:
						experiment_name = experiment

				# Replica info push
				if push_to_es:
					rucio_rep_stats = {}
					rucio_rep_stats["producer"] = "escape_wp2"
					rucio_rep_stats["type"] = "alba_rep_stats"
					rucio_rep_stats["timestamp"] = int(time.time())
					rucio_rep_stats["scope"] = scope
					rucio_rep_stats["rse"] = rse
					rucio_rep_stats["qos"] = rse_qos
					rucio_rep_stats["experiment"] = experiment_name
					
					rucio_rep_stats["total_replicas"] = tReplicas[0]	
					rucio_rep_stats["total_replicas_bytes"] = tReplicas_bytes[0]

					rucio_rep_stats["total_replicasA"] = tReplicasA[0]
					rucio_rep_stats["total_replicasB"] = tReplicasB[0]
					rucio_rep_stats["total_replicasC"] = tReplicasC[0]
					rucio_rep_stats["total_replicasU"] = tReplicasU[0]

					_post_to_es(es_url, rucio_rep_stats)


def get_dids(push_to_es=False, es_url=None):
	#query_dids = session.execute("SELECT COUNT(*) FROM dids WHERE availability='A'")
	#query_dids_bytes = session.execute("SELECT SUM(bytes) FROM dids WHERE availability='A'")
	#tDids = query_dids.first()[0]
	#tDids_bytes = query_dids_bytes.first()[0]
	#print(tDids)
	#print(tDids_bytes)

	query_f = session.execute("SELECT count(*) FROM dids WHERE did_type='F' AND availability='A'")
	query_d = session.execute("SELECT count(*) FROM dids WHERE did_type='D' AND availability='A'")
	query_c = session.execute("SELECT count(*) FROM dids WHERE did_type='C' AND availability='A'")

	tFiles = query_f.first()[0]
	tDatasets = query_d.first()[0]
	tContainers = query_c.first()[0]

	if push_to_es:
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
