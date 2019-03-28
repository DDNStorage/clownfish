PYTHON_FILES = $(wildcard *.py)
PYTHON_CHECKS = $(PYTHON_FILES:%=%.python_checked)
CHECKS = $(PYTHON_CHECKS)

%.python_checked: % ../.pylintrc
	PYLINTRC=../.pylintrc pylint --disable=I $<
	pep8 --ignore=E501 $<
	touch $@

check_clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

all: all-am $(CHECKS)
