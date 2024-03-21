gwcelery.tasks.external_triggers module
---------------------------------------

This module listens to the `GCN` notices from `SNEWS`_ and the `Fermi`_,
`Swift`_, and `INTEGRAL`_ missions, as well as Kafka alerts from
`Fermi`_ and `Swift`_. It is also responsible for carrying out tasks related to
external trigger-gravitational wave coincidences, including looking for
temporal coincidences, creating combined GRB-GW sky localization probability
maps, and computing their joint temporal and spatio-temporal false alarm
rates.

There are two GCN, one Kafka, and two IGWN Alert message handlers in the
`gwcelery.tasks.external_triggers` module:

* :meth:`~gwcelery.tasks.external_triggers.handle_snews_gcn` is called for
  each `SNEWS`_ GCN.

* :meth:`~gwcelery.tasks.external_triggers.handle_grb_gcn` is called for
  each GRB GCN such as `Fermi`_, `Swift`_, and `INTEGRAL`_.

* :meth:`~gwcelery.tasks.external_triggers.handle_targeted_kafka_alert` is
  called for GRB missions involved with the targeted search, currently `Fermi`_
  and `Swift`_.
    
* :meth:`~gwcelery.tasks.external_triggers.handle_snews_igwn_alert` is called
  for each `SNEWS`_ external trigger and superevent IGWN Alert.

* :meth:`~gwcelery.tasks.external_triggers.handle_grb_igwn_alert` is called
  for each `Fermi`_ and `Swift`_ external trigger and superevent IGWN Alert.

Flow charts
~~~~~~~~~~~

GCN VOEvent Ingestion
^^^^^^^^^^^^^^^^^^^^^

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

    SNEWS_GCN [
        style="rounded"
        label="SNEWS GCN recieved"
    ]

    GRB_GCN [
        style="rounded"
        label="GRB\nGCN recieved"
    ]

    subgraph cluster_gcn_handle {
        href = "../gwcelery.tasks.external_triggers.html#gwcelery.tasks.external_triggers.handle_grb_gcn"
        label = <<B><FONT face="monospace">handle_gcn</FONT></B>>

        Ignore_gcn [
            label="Ignore"
        ]

        Likely_noise [
            shape=diamond
            label="Is the event\nlikely non-astrophysical?"
        ]

        Event_exists_in_Gracedb [
            shape=diamond
            label="Does the event already\nexist in GraceDB"
        ]

        Update_existing_event_in_gracedb [
            label="Update the existing\nevent in GraceDB"
        ]

        Create_new_event_in_gracedb [
            label="Create a new event\nin GraceDB"
        ]
  
        Grab_create_skymap [
            label="Grab and/or\ncreate external sky map"
        ]

       Launch_detchar_tasks [
            label="Launch detector\ncharacterization checks\naround data"
        ]
    }

    SNEWS_GCN -> Likely_noise [
        lhead = cluster_gcn_handle
    ]

    GRB_GCN -> Likely_noise [
        lhead = cluster_gcn_handle
    ]

    Likely_noise -> Event_exists_in_Gracedb[label="no"]
    Likely_noise -> Ignore_gcn[label="yes"]
    Event_exists_in_Gracedb -> Update_existing_event_in_gracedb[label="yes"]
    Event_exists_in_Gracedb -> Create_new_event_in_gracedb[label="no"]
    Update_existing_event_in_gracedb -> Grab_create_skymap
    Create_new_event_in_gracedb -> Grab_create_skymap
    Create_new_event_in_gracedb -> Launch_detchar_tasks


Kafka Alert Ingestion
^^^^^^^^^^^^^^^^^^^^^

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

    KAFKA_FERMI [
        style="rounded"
        label="Fermi Kafka\nalert recieved"
    ]

    KAFKA_SWIFT [
        style="rounded"
        label="Swift Kafka\nalert recieved"
    ]

    subgraph cluster_kafka_handle {
        href = "../gwcelery.tasks.external_triggers.html#gwcelery.tasks.external_triggers.handle_targeted_kafka_alert"
        label = <<B><FONT face="monospace">handle_targeted_kafka_alert</FONT></B>>

        Ignore_gcn [
            label="Mark with NOT_GRB label\nto prevent publication"
        ]

        Likely_noise [
            shape=diamond
            label="Is the GRB FAR too high\nor a retraction notice?"
        ]

        Event_exists_in_Gracedb [
            shape=diamond
            label="Does the superevent or\nexternal event already\nexist in GraceDB?"
        ]

        Update_existing_event_in_gracedb [
            label="Update the existing\nevent in GraceDB"
        ]

        Create_new_event_in_gracedb [
            label="Create a new event\nin GraceDB"
        ]
  
        Grab_create_skymap [
            label="Use provided and/or\ncreate external sky map"
        ]

       Launch_detchar_tasks [
            label="Launch detector\ncharacterization checks\naround data"
        ]
    }

    KAFKA_FERMI -> Likely_noise [
        lhead = cluster_kafka_handle
    ]

    KAFKA_SWIFT -> Likely_noise [
        lhead = cluster_kafka_handle
    ]

    Likely_noise -> Event_exists_in_Gracedb[label="no"]
    Likely_noise -> Ignore_gcn[label="yes"]
    Ignore_gcn -> Event_exists_in_Gracedb
    Event_exists_in_Gracedb -> Update_existing_event_in_gracedb[label="yes"]
    Event_exists_in_Gracedb -> Create_new_event_in_gracedb[label="no"]
    Update_existing_event_in_gracedb -> Grab_create_skymap
    Create_new_event_in_gracedb -> Grab_create_skymap
    Create_new_event_in_gracedb -> Launch_detchar_tasks


IGWN Alert Handling
^^^^^^^^^^^^^^^^^^^

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

    GRB_External_Trigger_or_Superevent_IGWN_Alert [
        style="rounded"
        label="GRB external trigger or\nSuperevent IGWN Alert received"
    ]

    subgraph cluster_grb_igwn_alert_handle {
        href = "../gwcelery.tasks.external_triggers.html#gwcelery.tasks.external_triggers.handle_grb_igwn_alert"
        label = <<B><FONT face="monospace">handle_grb_igwn_alert</FONT></B>>

        Is_New_IGWN_Alert [
            shape=diamond
            label="Is there\na new superevent or\nexternal event?"
        ]

        Is_Label_Exttrig_IGWN_Alert [
            shape=diamond
            label="Is there a new label\nin the external event?"
        ]

        Are_Labels_Exttrig_Complete [
            shape=diamond
            label=" Does this label\ncomplete a set indicating\nboth sky maps are available?"
        ]

        Is_File_Exttrig_IGWN_Alert [
            shape=diamond
            label="Is there a new file\n in the external event,\nupdating a sky map?"
        ]

        Perform_Raven_Search [
            label="Perform Raven\ncoincidence search(es)"
        ]

        Does_Label_Launch_Pipeline [
            shape=diamond
            label="Are the labels a\ncomplete set, indicating a\ncoincidence and both sky maps\nare available?"
        ]
 
        Launch_Raven_Pipeline [
            label="Relaunch Raven\nPipeline"
        ]

        Create_Combined_Skymap [
            label="Create combined GW-GRB\nsky map"
        ]

    }

    GRB_External_Trigger_or_Superevent_IGWN_Alert -> Is_New_IGWN_Alert [
        lhead = cluster_grb_igwn_alert_handle
    ]
    Is_New_IGWN_Alert -> Perform_Raven_Search[label="yes"]
    Is_New_IGWN_Alert -> Is_Label_Exttrig_IGWN_Alert[label="no"]
    Is_Label_Exttrig_IGWN_Alert -> Are_Labels_Exttrig_Complete[label="yes"]
    Are_Labels_Exttrig_Complete -> Launch_Raven_Pipeline[label="yes"]
    Is_Label_Exttrig_IGWN_Alert -> Is_File_Exttrig_IGWN_Alert[label="no"]
    Is_File_Exttrig_IGWN_Alert -> Does_Label_Launch_Pipeline[label="yes"]
    Does_Label_Launch_Pipeline -> Launch_Raven_Pipeline[label="yes"]
    Launch_Raven_Pipeline -> Create_Combined_Skymap

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

    SNEWS_External_Trigger_or_Superevent_IGWN_Alert [
        style="rounded"
        label="SNEWS external trigger or\nSuperevent IGWN Alert received"
    ]

    subgraph cluster_snews_igwn_alert_handle {
        href = "../gwcelery.tasks.external_triggers.html#gwcelery.tasks.external_triggers.handle_snews_igwn_alert"
        label = <<B><FONT face="monospace">handle_snews_igwn_alert</FONT></B>>

        ignore [
            label="Ignore"
        ]

        is_new_exttrig_igwn_alert [
            shape=diamond
            label="Is this a new type SNEWS\nexternal trigger IGWN Alert?"
        ]

        is_new_superevent_igwn_alert [
            shape=diamond
            label="Is this a new type\nsuperevent IGWN Alert?"
        ]

        perform_raven_search [
            label="Perform Raven\ncoincidence search"
        ]
    }

    SNEWS_External_Trigger_or_Superevent_IGWN_Alert -> is_new_exttrig_igwn_alert [
        lhead = cluster_snews_igwn_alert_handle
    ]
    is_new_exttrig_igwn_alert -> perform_raven_search[label="yes"]
    is_new_exttrig_igwn_alert -> is_new_superevent_igwn_alert[label="no"]
    is_new_superevent_igwn_alert -> perform_raven_search[label="yes"]
    is_new_superevent_igwn_alert -> ignore[label="no"]

Tasks
~~~~~
.. automodule:: gwcelery.tasks.external_triggers

.. _`Fermi`: https://fermi.gsfc.nasa.gov/
.. _`Swift`: https://swift.gsfc.nasa.gov/
.. _`INTEGRAL`: https://www.cosmos.esa.int/web/integral/science-grb
.. _`SNEWS`: https://snews2.org/