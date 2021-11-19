#!/usr/bin/env python3

# import misc
import json
import math
import time
import requests
import argparse
import logging
from datetime import datetime
from rucio.db.sqla import session

logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d/%m/%y %H:%M:%S %Z',
                    level=logging.INFO)

# get global session
session = session.get_session()

# global experiment info
experiments = [
    "SKA", "LSST", "CTA", "LOFAR", "MAGIC", "ATLAS", "FAIR", "CMS", "VIRGO"
]


def _pprint_size(size_bytes):
    """
    convert bytes to user friendly output
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1000, i)
    s = round(size_bytes / p, 2)
    return "{}{}".format(s, size_name[i])


def _print_experiments():
    """
    print experiments
    """
    logging.info("EXPERIMENTS:")
    for experiment in experiments:
        logging.info("\t" + experiment)


def _print_rses(rses_list):
    """
    print rses
    """
    logging.info("RSES:")
    for rse in rses_list:
        logging.info("\t" + rse)


def _print_scopes(scope_list):
    """
    print scopes
    """
    logging.info("SCOPES:")
    for scope in scope_list:
        logging.info("\t" + scope)


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
    """
    get QOS info for RSE
    """

    query = '''
    SELECT
        rse_map.value
    FROM
        rse_attr_map rse_map,
        rses rse
    WHERE
        rse_map.rse_id = rse.id AND
        rse_map.key = 'QOS' AND
        rse.rse = '{rse}'
    '''.format(rse=rse)

    query_get_qos = session.execute(query).fetchone()

    if not query_get_qos:
        rse_qos = "NULL"
        return (rse_qos)
    else:
        rse_qos = query_get_qos[0]
        return (rse_qos)


def get_replicas(push_to_es=False, es_url=None):
    """
    get replicas per scope, per RSE
    """

    # get all rses & print them
    query_get_rses = session.execute("SELECT rse FROM rses WHERE deleted = 0")
    rses_list = []
    for row in query_get_rses:
        rses_list.append(row[0])
    _print_rses(rses_list)

    # get all scopes & print them
    query_get_scopes = session.execute(
        "SELECT scope FROM scopes WHERE status != 'D'")
    scopes_list = []
    for row in query_get_scopes:
        scopes_list.append(row[0])
    _print_scopes(scopes_list)

    # print experiments
    _print_experiments()

    logging.info("TOTAL REPLICA USAGE:")

    for rse in rses_list:
        logging.info("\tRSE:{}".format(rse))
        for scope in scopes_list:

            # get QOS class for RSE
            rse_qos = get_qos(rse)

            # get count & storage used of all replicas per scope, per RSE
            query = '''
            SELECT
                count(*) as count,
                sum(replica.bytes) as bytes
            FROM 
                replicas replica,
                rses rse
            WHERE
                replica.rse_id = rse.id AND
                replica.state = 'A' AND
                replica.scope = '{scope}' AND
                rse.rse = '{rse}'
            '''.format(scope=scope, rse=rse)
            results = dict(session.execute(query).fetchone())

            num_available_replicas = results["count"]
            sum_bytes_available_replicas = results["bytes"]

            if num_available_replicas == 0:
                sum_bytes_available_replicas = 0

            # get count & storage used of protected replicas per scope, per RSE
            query = '''
            SELECT
                count(*) as count,
                sum(replica.bytes) as bytes
            FROM 
                replicas replica,
                rses rse
            WHERE
                replica.rse_id = rse.id AND
                replica.state = 'A' AND
                replica.scope = '{scope}' AND
                rse.rse = '{rse}' AND
                EXISTS (
                    SELECT 
                        * 
                    FROM 
                        rules rule
                    WHERE
                        rule.scope = replica.scope AND
                        rule.name = replica.name AND
                        rule.state = 'O' AND
                        rule.rse_expression LIKE '%{rse}%'
                )
            '''.format(scope=scope, rse=rse, rse_qos=rse_qos)
            results = dict(session.execute(query).fetchone())

            num_available_replicas_protected = results["count"]
            sum_bytes_available_replicas_protected = results["bytes"]

            if num_available_replicas_protected == 0:
                sum_bytes_available_replicas_protected = 0

            # calculate unprotected number of replicas & storage used
            num_replicas_with_no_rules = num_available_replicas - num_available_replicas_protected

            sum_bytes_available_replicas_with_no_rules = sum_bytes_available_replicas - sum_bytes_available_replicas_protected

            # check if there are replicas:
            if num_available_replicas > 0:

                # Preparation for the experiment filter:
                experiment_name = 'None'
                for experiment in experiments:
                    if scope.startswith(
                            experiment
                    ) and "test" not in scope and "TEST" not in scope:
                        experiment_name = experiment

                logging.info(
                    "\tSCOPE:{} (P)Replicas:[{}|{}] (U)Replicas:[{}|{}]".format(
                        scope, num_available_replicas_protected,
                        _pprint_size(sum_bytes_available_replicas_protected),
                        num_replicas_with_no_rules,
                        _pprint_size(
                            sum_bytes_available_replicas_with_no_rules)))

                # post protected replicas json
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
                    rucio_rep_stats[
                        "total_replicas"] = num_available_replicas_protected
                    rucio_rep_stats[
                        "total_replicas_bytes"] = sum_bytes_available_replicas_protected

                    _post_to_es(es_url, rucio_rep_stats)

                # post unprotected replicas json
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
                    rucio_rep_stats[
                        "total_replicas"] = num_replicas_with_no_rules
                    rucio_rep_stats[
                        "total_replicas_bytes"] = sum_bytes_available_replicas_with_no_rules

                    _post_to_es(es_url, rucio_rep_stats)


def main():
    parser = argparse.ArgumentParser(
        description="Rucio Stats Probe for Replicas")
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
        parser.error("--push requires --url")

    get_replicas(push_to_es, es_url)


if __name__ == '__main__':
    main()
