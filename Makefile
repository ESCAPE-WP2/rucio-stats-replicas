format:
	yapf -i rucio_stats_probe.py --style=google
dry:
	python rucio_stats_probe.py
push:
	python rucio_stats_probe.py --push --url=http://monit-metrics:10012/
