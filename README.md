# Rucio Stats Probe for Replica info
Probe that populates the [Rucio Stats Replicas dashboard](https://monit-grafana.cern.ch/d/a74yXDN2Gk/rucio-stats-replicas?orgId=51).

In order to run this code you will need to install the dependencies that can be found in the ```requirments.txt``` file:
```bash
pip install -r requirements.txt
```
You should additionally configure your ```rucio.cfg``` file to point towards the Rucio server.

Examples of how to run the script can be found in the Makefile.
