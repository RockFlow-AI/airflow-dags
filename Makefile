HOMEDIR := $(shell pwd)
DAGDIR := $(HOMEDIR)/dags
PYTHONPATH := $(DAGDIR):$(PYTHONPATH)

.PHONY: test
test: venv
	PYTHONPATH=$(PYTHONPATH) $(VENV)/python -m unittest discover dags/utest

include Makefile.venv
Makefile.venv:
	curl \
		-o Makefile.fetched \
		-L "https://github.com/sio/Makefile.venv/raw/v2021.12.01/Makefile.venv"
	echo "f0aaca6f2a33b65b7f40ab8bff150b6b216b1ca9c91a2a4e4e8a9d9718834139 *Makefile.fetched" \
		| sha256sum --check - \
		&& mv Makefile.fetched Makefile.venv