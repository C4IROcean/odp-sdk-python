# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
from os import path
import sys
sys.path.insert(0, path.abspath(path.join(path.dirname(__file__), "../../")))
sys.path.insert(0, path.abspath(path.join(path.dirname(__file__), "../../Examples")))


# -- Project information -----------------------------------------------------

project = 'ODP Python SDK'
copyright = '2020, C4IR/Ocean Data Foundation'
author = 'C4IR/Ocean Data Foundation'

version = '0.3.9'

# The full version, including alpha/beta/rc tags
release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_rtd_theme"
]

autosummary_generate = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

html_favicon = "img/odp-favicon-rgb-blueandwhite.png"
html_logo = "img/odp-logo-rgb-blueandwhite.png"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# html_context = {
#     "css_files": ["_static/style.css"]
# }

master_doc = "index"

def setup(app):
    app.add_css_file("style.css")
