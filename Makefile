# Description: Makefile for building and publishing the SDK

# External tools
POETRY := poetry
MD5SUM := md5sum
TAR := tar
GIT := git
PYTHON := python3

# Subprojects
SUBPROJECTS := src/sdk src/dto
DIST_DIRS := $(SUBPROJECTS:%=%/dist)
PYPROJECTS := $(SUBPROJECTS:%=%/pyproject.toml)
MD5S := $(DIST_DIRS:%=%/md5.published)
VERSIONS := $(SUBPROJECTS:%=%/version.txt)

# Get the current version from the git tags
CURRENT_VERSION := $(shell $(GIT) describe --tags --abbrev=0)

#
# Rules
#

# Build the distribution
%/dist: %/pyproject.toml
	cd $(dir $@) && $(POETRY) build

# Create the md5 hash of the distribution
%/dist/md5: %/dist
	$(TAR) -cf - $(dir $@) | $(MD5SUM) > $@

# Publish the distribution
%/dist/md5.published: %/dist/md5
	cd $(dir $@) && $(POETRY) publish --dry-run
	cp $< $@

# Update the version in the pyproject.toml
%/version.txt: %/pyproject.toml
	echo "Poetry version: $(CURRENT_VERSION)"
	$(POETRY) run python scripts/migrate_local_deps.py $(CURRENT_VERSION) $< --overwrite
	cd $(dir $<) && $(POETRY) version $(CURRENT_VERSION)
	echo $(CURRENT_VERSION) > $@

# Update the version in all subprojects
version: $(VERSIONS)

# Build all subprojects
build: $(DIST_DIRS)

# Publish all subprojects
publish: $(MD5S)

# Clean up
clean:
	rm -vrf $(DIST_DIRS)
	rm -f $(VERSIONS)

# Default target
all: build

# Phony targets
.PHONY: build publish version clean all
