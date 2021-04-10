#!/usr/bin/env python

# import all needed rucio clients
from rucio.client.rseclient import RSEClient
from rucio.client.scopeclient import ScopeClient
from rucio.client.didclient import DIDClient

# import misc
import math
import json
import time
import requests
import argparse

# global experiment info
experiments = [
    "SKA", "LSST", "CTA", "LOFAR", "MAGIC", "ATLAS", "FAIR", "CMS", "VIRGO"
]
experiment_map = {}


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


def _post_to_es(es_url, data_dict):
    """
    post data to ES datasource, raise Exception if not successful
    """
    status_code = requests.post(
        es_url,
        data=json.dumps(data_dict),
        headers={"Content-Type": "application/json; charset=UTF-8"})
    status_code.raise_for_status()


def _print_rses():
    """
    print rses
    """
    print("RSES:")
    rses_list = list(RSEClient().list_rses())
    for rse in rses_list:
        print("\t" + rse['rse'])


def _print_scopes():
    """
    print scopes
    """
    print("SCOPES:")
    scope_list = list(ScopeClient().list_scopes())
    for scope in scope_list:
        print("\t" + scope)


def _print_experiments():
    """
    print experiments
    """
    print("EXPERIMENTS:")
    for experiment in experiments:
        print("\t" + experiment)


def _setup_experiments():
    """
    setup experiments dictionary
    """
    for experiment in experiments:
        experiment_map[experiment] = {}
        experiment_map[experiment]["total_dids_count"] = 0
        experiment_map[experiment]["total_files_count"] = 0
        experiment_map[experiment]["total_datasets_count"] = 0
        experiment_map[experiment]["total_containers_count"] = 0
        experiment_map[experiment]["total_files_bytes"] = 0


def _print_rse_usage(push_to_es=False, es_url=None):
    """
    print usage of rses, optionally push to an ES datasource
    """
    print("TOTAL USAGE (RSE):")

    rses_total_used = 0
    rse_client = RSEClient()
    rses_list = list(rse_client.list_rses())
    for rse in rses_list:
        rse_name = rse['rse']
        for rse_usage in rse_client.get_rse_usage(rse_name):

            if rse_usage['source'] != "rucio":
                continue

            if rse_usage['files'] is None:
                rse_usage['files'] = 0

            print("\tRSE:{} | Files:{} | Used:{}".format(
                rse_name, rse_usage['files'], _pprint_size(rse_usage['used'])))

            rses_total_used += rse_usage['used']

            if push_to_es:
                rucio_rse_stats = {}
                rucio_rse_stats["producer"] = "escape_wp2"
                rucio_rse_stats["type"] = "rucio_rse_stats"
                rucio_rse_stats["timestamp"] = int(time.time())
                # schema:rucio_rse_stats (rse)
                rucio_rse_stats["rse"] = rse_name
                rucio_rse_stats["files"] = int(rse_usage['files'])
                rucio_rse_stats["used_bytes"] = int(rse_usage['used'])
                _post_to_es(es_url, rucio_rse_stats)

    print("\tRSES TOTAL USED:{}".format(_pprint_size(rses_total_used)))

    if push_to_es:
        rucio_rse_stats = {}
        rucio_rse_stats["producer"] = "escape_wp2"
        rucio_rse_stats["type"] = "rucio_rse_stats"
        rucio_rse_stats["timestamp"] = int(time.time())
        # schema:rucio_rse_stats (rses total)
        rucio_rse_stats["total_used"] = int(rses_total_used)
        _post_to_es(es_url, rucio_rse_stats)


def _print_scope_usage(push_to_es=False, es_url=None):
    """
    print usage of scopes, optionally push to ES datasource
    return: experiment usage map
    """
    print("TOTAL USAGE (SCOPE):")

    did_client = DIDClient()
    scope_list = list(ScopeClient().list_scopes())
    _setup_experiments()

    # calculate stats per scope (and keep info for experiments)
    scope_total_used = 0
    for scope in scope_list:
        filters = {}

        # get scope data from did client
        dids = list(did_client.list_dids(scope, filters, type='all', long=True))
        files = list(
            did_client.list_dids(scope, filters, type='file', long=True))
        datasets = list(
            did_client.list_dids(scope, filters, type='dataset', long=True))
        containers = list(
            did_client.list_dids(scope, filters, type='container', long=True))

        # calculate total bytes used for files
        fsize = 0
        for file in files:
            fsize += int(file['bytes'])

        # gather counts
        dids_count = int(len(dids))
        files_count = int(len(files))
        datasets_count = int(len(datasets))
        containers_count = int(len(containers))

        print(
            "\tSCOPE:{} | DIDs:{} | Files:{} ({}) | Datasets:{} | Containers:{}"
            .format(scope, dids_count, files_count, _pprint_size(fsize),
                    datasets_count, containers_count))

        scope_total_used += fsize

        # consistency check
        all_dids_count = files_count + datasets_count + containers_count
        if dids_count != all_dids_count:
            print(
                "\t>> Inconsistent number of DIDS | dids_count:{} != all_dids_count:{} <<"
                .format(dids_count, all_dids_count))

        if push_to_es:
            rucio_scope_stats = {}
            rucio_scope_stats["producer"] = "escape_wp2"
            rucio_scope_stats["type"] = "rucio_scope_stats"
            rucio_scope_stats["timestamp"] = int(time.time())
            # schema:rucio_scope_stats (scope)
            rucio_scope_stats["scope"] = scope
            rucio_scope_stats["dids"] = dids_count
            rucio_scope_stats["files"] = files_count
            rucio_scope_stats["datasets"] = datasets_count
            rucio_scope_stats["containers"] = containers_count
            rucio_scope_stats["files_bytes"] = fsize
            _post_to_es(es_url, rucio_scope_stats)

        # experiments provision
        for experiment in experiments:
            if scope.startswith(experiment):
                experiment_map[experiment]["total_dids_count"] += dids_count
                experiment_map[experiment]["total_files_count"] += files_count
                experiment_map[experiment][
                    "total_datasets_count"] += datasets_count
                experiment_map[experiment][
                    "total_containers_count"] += containers_count
                experiment_map[experiment]["total_files_bytes"] += fsize

    print("\tSCOPES TOTAL USED:{}".format(_pprint_size(scope_total_used)))

    if push_to_es:
        rucio_scope_stats = {}
        rucio_scope_stats["producer"] = "escape_wp2"
        rucio_scope_stats["type"] = "rucio_scope_stats"
        rucio_scope_stats["timestamp"] = int(time.time())
        # schema:rucio_scope_stats (scopes total)
        rucio_scope_stats["total_used"] = int(scope_total_used)
        _post_to_es(es_url, rucio_scope_stats)


def _print_experiment_usage(push_to_es=False, es_url=None):
    """
    print usage of experiments, optionally push to ES datasource
    """
    print("TOTAL USAGE (EXPERIMENT):")

    experiment_total_used = 0

    for experiment in experiment_map:

        # gather counts
        dids_count = experiment_map[experiment]["total_dids_count"]
        files_count = experiment_map[experiment]["total_files_count"]
        datasets_count = experiment_map[experiment]["total_datasets_count"]
        containers_count = experiment_map[experiment]["total_containers_count"]
        files_bytes = experiment_map[experiment]["total_files_bytes"]

        print(
            "\tEXPERIMENT:{} | DIDs:{} | Files:{} ({}) | Datasets:{} | Containers:{}"
            .format(experiment, dids_count, files_count,
                    _pprint_size(files_bytes), datasets_count,
                    containers_count))

        experiment_total_used += files_bytes

        if push_to_es:
            rucio_experiment_stats = {}
            rucio_experiment_stats["producer"] = "escape_wp2"
            rucio_experiment_stats["type"] = "rucio_experiment_stats"
            rucio_experiment_stats["timestamp"] = int(time.time())
            # schema:rucio_experiment_stats (experiment)
            rucio_experiment_stats["experiment"] = experiment
            rucio_experiment_stats["total_dids"] = dids_count
            rucio_experiment_stats["total_files"] = files_count
            rucio_experiment_stats["total_datasets"] = datasets_count
            rucio_experiment_stats["total_containers"] = containers_count
            rucio_experiment_stats["total_files_bytes"] = files_bytes
            _post_to_es(es_url, rucio_experiment_stats)

    print("\tEXPERIMENTS TOTAL USED:{}".format(
        _pprint_size(experiment_total_used)))

    if push_to_es:
        rucio_experiment_stats = {}
        rucio_experiment_stats["producer"] = "escape_wp2"
        rucio_experiment_stats["type"] = "rucio_experiment_stats"
        rucio_experiment_stats["timestamp"] = int(time.time())
        # schema:rucio_experiment_stats (experiments total)
        rucio_experiment_stats["total_used"] = experiment_total_used
        _post_to_es(es_url, rucio_experiment_stats)


def main():

    parser = argparse.ArgumentParser(description="Rucio Stats Probe")

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

    # display rses, scopes and experiments
    _print_rses()
    _print_scopes()
    _print_experiments()

    # display/push rse, scope and experiment usage
    _print_rse_usage(push_to_es, es_url)
    _print_scope_usage(push_to_es, es_url)
    _print_experiment_usage(push_to_es, es_url)


if __name__ == '__main__':
    main()
