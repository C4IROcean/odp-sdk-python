# -*- coding: utf-8 -*-
from os import path, listdir
import setuptools

local_path = path.dirname(__file__)

# Fix for tox which manipulates execution path
if not local_path:
    local_path = "."
here = path.abspath(local_path)


def read_reqs(fname):
    req_path = path.join(here, fname)
    if not path.exists(req_path):
        raise RuntimeError(listdir(path.dirname(__file__)))
    with open(req_path) as f:
        return [
            req.strip() for req in f.readlines() if req.strip()
        ]


core_reqs = read_reqs("requirements.txt")
util_reqs = read_reqs("requirements-utils.txt")
test_reqs = read_reqs("requirements-test.txt")
all_reqs = core_reqs + util_reqs + test_reqs

extras_require = {
    "all": all_reqs,
    "test": test_reqs,
    "util": util_reqs
}

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="odp_sdk",
    version="0.3.9",  # Update this for every new version
    #py_modules = ['client'],
    author="ocean-kristian",
    author_email="kristian.authen@oceandata.earth",
    description='Python SDK for the Ocean Data Platform',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=core_reqs,
    extras_require=extras_require,
    url='https://github.com/C4IROcean/odp-sdk-python',
    packages=setuptools.find_packages(),
    classifiers=(
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ),
)

# python setup.py sdist bdist_wheel
# twine upload --repository testpypi dist/*

