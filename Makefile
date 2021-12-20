HOMEDIR := $(shell pwd)
MODULEDIR := $(HOMEDIR)/dags
DAGDIR := $(MODULEDIR)/rockflow/dags
PYTHONPATH := $(MODULEDIR):$(PYTHONPATH)
AIRFLOW_VERSION := 2.2.1
PY?=python3
PYTHON_VERSION := $(shell $(PY) --version | cut -d " " -f 2 | cut -d "." -f 1-2)
CONSTRAINT_URL := "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt"

# https://lithic.tech/blog/2020-05/makefile-dot-env
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# install python deps
.PHONY: init
init: venv
	$(VENV)/pip install "apache-airflow[async,postgres,google,alibaba]==$(AIRFLOW_VERSION)" --constraint "$(CONSTRAINT_URL)"
	$(VENV)/pip install -r requirements_extra.txt
	$(VENV)/pip install -r requirements_test.txt

# format code
.PHONY: format
format: venv
	$(VENV)/pip install autopep8
	$(VENV)/autopep8 --in-place --recursive $(MODULEDIR)

# run all unit test
.PHONY: test
test: venv
	PYTHONPATH=$(PYTHONPATH) $(VENV)/python -m unittest discover dags/utest

# test load all dags
.PHONY: load
load: venv
	PYTHONPATH=$(PYTHONPATH) $(foreach file, $(wildcard $(DAGDIR)/*.py), $(VENV)/python $(file);)

# init venv
include Makefile.venv
Makefile.venv:
	curl \
		-o Makefile.fetched \
		-L "https://github.com/sio/Makefile.venv/raw/v2021.12.01/Makefile.venv"
	echo "f0aaca6f2a33b65b7f40ab8bff150b6b216b1ca9c91a2a4e4e8a9d9718834139 *Makefile.fetched" \
		| sha256sum --check - \
		&& mv Makefile.fetched Makefile.venv