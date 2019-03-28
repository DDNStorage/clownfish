SUBDIRS = pybuild .

build_dir = `pwd`/build
rpmbuild_opt =
ISO_PATH = `pwd`/ISO
PACKAGE_PATH = ${ISO_PATH}/Packages

AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

pyclownfish/clownfish_pb2.py: pyclownfish/clownfish.proto
	protoc -I=pyclownfish --python_out pyclownfish pyclownfish/clownfish.proto

example_configs/clownfish/clownfish_test.conf: \
		gen_clownfish_test pyclownfish/gen_clownfish_test.py \
		pyclownfish/clownfish_test.py \
		pyclownfish/clownfish_common.py \
		pyclownfish/clownfish_pb2.py \
		pylustre/cstr.py pylustre/lvirt.py
	./gen_clownfish_test example_configs/clownfish/clownfish_test.conf

PYTHON_COMMANDS = \
	clownfish_console \
	clownfish_install \
	clownfish_server \
	clownfish_test \
	gen_clownfish_test \
	ltest_console \
	ltest_launch \
	ltest_scheduler \
	lvirt \
	pyltest_import_check

EXTRA_DIST= \
	$(PYTHON_COMMANDS) \
	detect-distro.sh \
	example_configs/clownfish/clownfish_install.conf \
	example_configs/clownfish/seperate_mgs/clownfish.conf \
	example_configs/clownfish/seperate_mgs/lvirt.conf \
	example_configs/ltest/ltest_scheduler.conf \
	init.d/* \
	clownfish.spec \
	pybuild/*.py \
	systemd/* \
	pyclownfish/*.py \
	pyclownfish/clownfish.proto \
	pyclownfish/.pylintrc \
	pylustre/*.py \
	pyltest/*.py \
	.pylintrc

PYTHON_PROTOBUF = pyclownfish/clownfish_pb2.py
PYLTEST_FILES = $(wildcard pyltest/*.py)
PYTHON_LIB_FILES = $(wildcard pyclownfish/*.py pylustre/*.p)
PYTHON_LIB_FILES += $(PYLTEST_FILES)
PYTHON_FILES = $(PYTHON_LIB_FILES) $(PYTHON_COMMANDS)
PYTHON_CHECKS = $(PYTHON_FILES:%=%.python_checked)
PYLTEST_CHECKS = $(PYLTEST_FILES:%=%.pyltest_import_checked)
PYTHON_CHECKS += $(PYLTEST_CHECKS)
CHECKS = $(PYTHON_CHECKS)

%.pyltest_import_checked: %
	./pyltest_import_check $<
	touch $@

%.python_checked: % pyclownfish/.pylintrc
	@if test $< != $(PYTHON_PROTOBUF); then \
		PYLINTRC=pyclownfish/.pylintrc pylint --disable=I $< || exit 1; \
		pep8 --ignore=E501 $< || exit 1; \
	fi
	touch $@

check_clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

check:
	make check_clean
	make

all: all-am pyclownfish/clownfish_pb2.py \
	example_configs/clownfish/clownfish_test.conf $(CHECKS)

# Clean up all the generated files that are ignored in the source repo
#
mrproper: maintainer-clean
	rm -f Makefile.in aclocal.m4 configure
	rm -f compile depcomp install-sh missing

PYLUSTRE_RPM = build/RPMS/x86_64/clownfish-pylustre-$(PACKAGE_VERSION)-$(CLOWNFISH_RELEASE).el$(DISTRO_RELEASE)*.x86_64.rpm
PYLTEST_RPM = build/RPMS/x86_64/clownfish-pyltest-$(PACKAGE_VERSION)-$(CLOWNFISH_RELEASE).el$(DISTRO_RELEASE)*.x86_64.rpm
CLOWNFISH_RPM = build/RPMS/x86_64/clownfish-$(PACKAGE_VERSION)-$(CLOWNFISH_RELEASE).el$(DISTRO_RELEASE)*.x86_64.rpm

rpms: clownfish.spec dist
	mkdir -p $(build_dir)/BUILD $(build_dir)/SPECS $(build_dir)/SRPMS $(build_dir)/RPMS \
		&& rpmbuild $(rpmbuild_opt) --define="_topdir $(build_dir)" \
			--define="_prefix $(prefix)" -tb $(distdir).tar.gz \
		&& echo "RPMs successfully generated in $(build_dir)/RPMS"

clownfish-$(PACKAGE_VERSION).x86_64.iso: rpms
	@if test -z "$(CACHED_ISO_PATH)"; then \
		echo -e "Error: Can not build ISO without the cached ISO path," \
			"please reconfigure using --with-cached-iso=path option"; \
		exit 1; \
	fi
	rm $(ISO_PATH) -fr
	rm -f clownfish-*.iso
	rm -f clownfish-*.md5
	cp -a $(CACHED_ISO_PATH) $(ISO_PATH)
	cp $(CLOWNFISH_RPM) $(PACKAGE_PATH)
	cp $(PYLUSTRE_RPM) $(PACKAGE_PATH)
	cp $(PYLTEST_RPM) $(PACKAGE_PATH)
	createrepo $(PACKAGE_PATH)
	mkisofs -joliet-long -R -o clownfish-$(PACKAGE_VERSION).x86_64.iso $(ISO_PATH)

clownfish-$(PACKAGE_VERSION).x86_64.md5: clownfish-$(PACKAGE_VERSION).x86_64.iso
	md5sum clownfish-$(PACKAGE_VERSION).x86_64.iso \
		> clownfish-$(PACKAGE_VERSION).x86_64.md5

iso: clownfish-$(PACKAGE_VERSION).x86_64.iso clownfish-$(PACKAGE_VERSION).x86_64.md5
