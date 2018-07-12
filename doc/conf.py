#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# GWCelery documentation build configuration file, created by
# sphinx-quickstart on Tue Dec  5 14:59:24 2017.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import importlib
from subprocess import check_output
import sys

sys.path.insert(0, os.path.abspath('..'))


def get_setup_output(*args):
    return check_output((sys.executable, 'setup.py') + args, cwd='..').decode()


# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'celery.contrib.sphinx',
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx_celery.setting_crossref']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
source_suffix = ['.rst', '.md']
source_parsers = {
   '.md': 'recommonmark.parser.CommonMarkParser'
}

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = get_setup_output('--name')
author = get_setup_output('--author')

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = get_setup_output('--version')
# The full version, including alpha/beta/rc tags.
release = version

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

autodoc_mock_imports = []
for mod in ['astropy',
            'gcn',
            'glue',
            'gwpy',
            'lal',
            'lalapps',
            'lalburst',
            'laldetchar',
            'lalframe',
            'lalinference',
            'lalinspiral',
            'lalmetaio',
            'lalpulsar',
            'lalsimulation',
            'lalstochastic',
            'lalxml',
            'ligo',
            'ligo.followup_advocate',
            'ligo.p_astro'
            'ligo.raven',
            'ligo.skymap',
            'lxml',
            'lxml.etree',
            'numpy',
            'sleek_lvalert']:
    try:
        importlib.import_module(mod)
    except ImportError:
        autodoc_mock_imports.append(mod)
autodoc_default_flags = ['members', 'show-inheritance']
autosummary_generate = True


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
html_theme_options = {
    'description': get_setup_output('--description'),
    'github_button': False,
    'logo': 'logo.png',
    'logo_name': True
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# This is required for the alabaster theme
# refs: http://alabaster.readthedocs.io/en/latest/installation.html#sidebars
html_sidebars = {
    'index': [
        'about.html',
        'searchbox.html',
    ],
    '**': [
        'about.html',
        'navigation.html',
        'searchbox.html',
    ]
}


# -- Options for HTMLHelp output ------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = project + 'doc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    # 'papersize': 'letterpaper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    # 'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',

    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, project + '.tex', 'GWCelery Documentation',
     author, 'manual'),
]


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, project.lower(), project + ' Documentation',
     [author], 1)
]


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, project, project + ' Documentation',
     author, project, 'One line description of project.',
     'Miscellaneous'),
]


# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'celery': ('http://celery.readthedocs.org/en/latest/', None),
    'celery_eternal': ('http://celery-eternal.readthedocs.io/en/latest/',
                       None),
    'gwpy': ('https://gwpy.github.io/docs/stable/', None)
}


# -- Options for viewcode extension ---------------------------------------

# celery.contrib.sphinx does not show source links for tasks if this is set
# to the default value of True
viewcode_import = False
