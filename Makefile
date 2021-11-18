format:
	yapf -i rucio_stats_probe_replicas.py --style=google
dry:
	python rucio_stats_probe_replicas.py
push:
	python rucio_stats_probe_replicas.py --push --url=http://monit-metrics:10012/
