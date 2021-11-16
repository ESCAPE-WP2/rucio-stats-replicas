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

# Global experiment info
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
    query_get_qos = session.execute(
        "SELECT VALUE a FROM RSE_ATTR_MAP a INNER JOIN RSES r ON r.id=a.rse_id AND a.KEY='QOS' AND r.RSE='"
        + rse + "'").fetchone()
    if not query_get_qos:
        rse_qos = "NULL"
        return (rse_qos)
    else:
        rse_qos = query_get_qos[0]
        return (rse_qos)


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

        for rse in rses_list:
            rse_qos = get_qos(rse)
            #Get amount replicas per RSE
            tReplicas = session.execute(
                "SELECT COUNT(*) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"
                + scope + "' AND rse='" + rse + "'").fetchone()
            tReplicas_bytes = session.execute(
                "SELECT SUM(rep.bytes) FROM replicas rep INNER JOIN rses r ON rep.rse_id=r.id WHERE state='A' AND scope='"
                + scope + "' AND rse='" + rse + "'").fetchone()

            # Protected Replicas:
            tRules = session.execute(
                "SELECT COUNT(*) FROM rules ru INNER JOIN replicas rep ON ru.scope=rep.scope AND ru.name=rep.name INNER JOIN rses r ON rep.rse_id=r.id AND rep.state='A' AND ru.state='O' AND ru.scope='"
                + scope + "' AND r.rse='" + rse + "'").fetchone()
            tRules_bytes = session.execute(
                "SELECT SUM(rep.bytes) FROM replicas rep INNER JOIN rules ru ON ru.scope=rep.scope AND ru.name=rep.name INNER JOIN rses r ON rep.rse_id=r.id AND rep.state='A' AND rep.scope='"
                + scope + "' AND r.rse='" + rse + "'").fetchone()

            #Not protected Replicas:
            tNoRules = tReplicas[0] - tRules[0]
            if tReplicas_bytes[0] is None:
                tNoRules_bytes = 0
            else:
                if tRules_bytes[0] is None:
                    tNoRules_bytes = tReplicas_bytes[0]
                else:
                    tNoRules_bytes = tReplicas_bytes[0] - tRules_bytes[0]

            # Check if there are replicas:
            if tReplicas[0] > 0:
                # Preparation for the experiment filter:
                experiment_name = 'None'
                for experiment in experiments:
                    if scope.startswith(
                            experiment
                    ) and "test" not in scope and "TEST" not in scope:
                        experiment_name = experiment

                # Replica info push
                # Protected replicas
                if push_to_es:
                    rucio_rep_stats = {}
                    rucio_rep_stats["producer"] = "escape_wp2"
                    rucio_rep_stats["type"] = "alba_rep_stats"
                    rucio_rep_stats["timestamp"] = int(time.time())
                    rucio_rep_stats["scope"] = scope
                    rucio_rep_stats["rse"] = rse
                    rucio_rep_stats["qos"] = rse_qos
                    rucio_rep_stats["experiment"] = experiment_name

                    rucio_rep_stats["protectedVar"] = "protected"
                    rucio_rep_stats["total_replicas"] = tRules[0]
                    rucio_rep_stats["total_replicas_bytes"] = tRules_bytes[0]

                    _post_to_es(es_url, rucio_rep_stats)

                #Not protected replicas
                if push_to_es:
                    rucio_rep_stats = {}
                    rucio_rep_stats["producer"] = "escape_wp2"
                    rucio_rep_stats["type"] = "alba_rep_stats"
                    rucio_rep_stats["timestamp"] = int(time.time())
                    rucio_rep_stats["scope"] = scope
                    rucio_rep_stats["rse"] = rse
                    rucio_rep_stats["qos"] = rse_qos
                    rucio_rep_stats["experiment"] = experiment_name

                    rucio_rep_stats["protectedVar"] = "not protected"
                    rucio_rep_stats["total_replicas"] = tNoRules
                    rucio_rep_stats["total_replicas_bytes"] = tNoRules_bytes

                    _post_to_es(es_url, rucio_rep_stats)


def main():
    parser = argparse.ArgumentParser(
        description="Rucio Stats Interconnection Probe")
    parser.add_argument("--push",
                        required=False,
                        action='store_true',
                        default=False,
                        help="Push to an ES datasource")
    parser.add_argument("--url",
                        required=False,
                        dest='es_url',
                        help="ES datasource url")

    args = parser.parse_args()
    push_to_es = args.push
    es_url = args.es_url

    if push_to_es and es_url is None:
        parser.error("--push requires --url.")

    it = datetime.now()
    init_time = it.strftime("%H:%M:%S")
    print("Current Time =", init_time)
    get_replicas(push_to_es, es_url)

    et = datetime.now()
    end_time = et.strftime("%H:%M:%S")
    print("Current Time =", end_time)


if __name__ == '__main__':
    main()
