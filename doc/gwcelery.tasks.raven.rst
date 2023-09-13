gwcelery.tasks.raven module
---------------------------

This module use functions from the `ligo-raven`_ package to launch coincidence
searches and determine whether a joint search should be published.

Flow chart
~~~~~~~~~~

.. digraph:: exttrig

    compound = true
    nodesep = 0.1
    ranksep = 0.1

    node [
        fillcolor = white
        shape = box
        style = filled
        target = "_top"
    ]

    graph [
        labeljust = "left"
        style = filled
        target = "_top"
    ]

    NEW_EVENT [
        style="rounded"
        label="New superevent\nor external event"
    ]

    RELAUNCH_PIPELINE [
        style="rounded"
        label="Re-launch due to\nnew sky map(s)"
    ]

    subgraph cluster_raven_pipeline {
        href = "../gwcelery.tasks.raven.html#gwcelery.tasks.raven.coincidence_search"
        label = <<B><FONT face="monospace">coincidence_search</FONT></B>>

        Query_gracedb [
            label="Query GraceDB\nto search for coincidence"
        ]

        Query_results [
            shape=diamond
            label="Any joint\ncandidates found?"
        ]

        Add_to_superevent [
            label="Add external event(s)\nto superevent"
        ]

        Calculate_coincidence_far [
            label="Calculate\ncoincidence FAR"
        ]

        Apply_emcoinc [
            label="Apply EM_COINC\nlabels to events"
        ]
  
        Check_publishing [
            shape=diamond
            label="Does this\ncoincidence pass\npublishing conditions?"
        ]

        Launch_alert [
            label="Launch preliminary alert\nor add coincidence info\nto next alert"
        ]

        Update_preferred_external [
            label="Update preferred\nexternal event"
        ]

    }

    NEW_EVENT -> Query_gracedb [
        lhead = cluster_raven_pipeline
    ]

    RELAUNCH_PIPELINE -> Calculate_coincidence_far

    Query_gracedb -> Query_results
    Query_results -> Add_to_superevent[label="yes"]
    Add_to_superevent -> Calculate_coincidence_far
    Calculate_coincidence_far -> Apply_emcoinc
    Calculate_coincidence_far -> Check_publishing
    Check_publishing -> Launch_alert[label="yes"]
    Check_publishing -> Update_preferred_external[label="no"]
    Launch_alert -> Update_preferred_external

Tasks
~~~~~

.. automodule:: gwcelery.tasks.raven

.. _`ligo-raven`: https://ligo-raven.readthedocs.io