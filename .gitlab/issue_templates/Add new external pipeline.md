# Checklist

## Policy

- [ ] Determine whether the new experiment falls within an existing search, see this [documentation](https://gracedb.ligo.org/documentation/models.html#data-models) for a comprehensive list.
- [ ] Determine which (if any) joint FAR method to use, either [untargeted](https://ligo-raven.readthedocs.io/en/latest/joint_far.html#untargeted-search-method) or [targeted](https://ligo-raven.readthedocs.io/en/latest/joint_far.html#targeted-search-method). The untargeted method is often used with highly significant external events (enough to be published based on their own sigificance) while the targeted method is often used with less significant events that have their own FARs available.
- [ ] Determine what the relevant rates will be, either the *independent* rates of detection (i.e. not detected by any other experiment in the same search) or max FAR thresholds (both GW and external). See the following for examples: [adding subthreshold GRBs](https://dcc.ligo.org/cgi-bin/private/DocDB/ShowDocument?docid=T1900297); [Adding SVOM GRBs to the list of existing GRBs](https://dcc.ligo.org/LIGO-T2400116)
- [ ] Determine which coincidence searches will need to be set up, i.e. which type(s) of GW candidate(s) associations we are looking for. Examples include CBCs and unmodeled GW Bursts created from supernovae.
- [ ] If a new coincidence search, determine what the relevant new time window(s) should be. These should ideally be based on delay models. If the underlying systems that could produce such a coincidence are not as well understood, then a practical time window could be chosen that is roughly what we could expect (e.g. the CBC-FRB time windows are double that of CBC-GRB due to our ignorance of such systems). Otherwise use the time windows of the existing search.
- [ ] Determine whether sky maps or sky map information (RA/dec/error radius) will be available. Determine whether these will be flattened or Multi-Order Coverage (MOC).
- [ ] If a new search, determine whether there should be differences in considering which external event should be preferred (currently SNEWS > threshold > subthreshold, with tie breakers being whether the `NOT_GRB` label is present and the joint FAR).
- [ ] Determine whether there are any special conditions to publish this event beyond the current conditions (see `raven.trigger_raven_alert`)

## GraceDB

- [ ] If the `search` and/or `pipeline` names do not yet exist in GraceDB, open an issue on [GraceDB Server](https://git.ligo.org/computing/gracedb/server/-/issues/new) and work with the developers to add this. This can be checked via the [documentation](https://gracedb.ligo.org/documentation/models.html#data-models).
- [ ] If the VOEvent packet differs from previous examples and is not ingestible, open an issue on [GraceDB Server](https://git.ligo.org/computing/gracedb/server/-/issues/new) and work with the developers to fix this. *Note* we currently convert JSON packets to VOEvent in gwcelery, so until this is natively supported in GraceDB, an existing function in gwcelery [_kafka_to_voevent](https://git.ligo.org/emfollow/gwcelery/-/blob/218d378e665e5397677854b43f5e6719c42e0036/gwcelery/tasks/external_triggers.py#L726) may need to be modified (or a new equivalent function written if too different).

## GWCelery

- [ ] Add/modify the policy-determined variables to the [configuration file](https://git.ligo.org/emfollow/gwcelery/-/blob/main/gwcelery/conf/__init__.py), next to each relevant existing variable if applicable. This also includes pipeline variables used in other applications like detchar omegascans, etc.
- [ ] If an existing search, add to the relevant listener. Add any unique differences compared to previous searches, especially if there are ways to either filter out events or mark them to not be considered in the search with the `NOT_GRB` label. If the search differs significantly from existing or different functionality is needed, create a new listener. Add logic to download sky maps if available or create sky maps if RA/dec/error is given by choosing the arguments for `external_triggers._create_replace_external_event_and_skymap`.
- [ ] Do the same for the IGWN alert listener. Add `raven.coincidence_search`s determined in the above policy section.
- [ ] If using Kafka, modify the credential map (at `~/.config/gwcelery/kafka_credential_map.json`) on each server (`production`, `playground`, `test`, `dev`) to include the key/credential pair for each new notice type.
- [ ] If using ligo-raven < 4.0, modify `raven.calculate_coincidence_far` to get the correct joint FAR method by overriding the `ext_search` if necessary. If using ligo-raven >= 4.0, map to the correct `joint_far_method`.
- [ ] If changing or adding time windows, modify `raven._time_window`.
- [ ] If changing how the preferred external event is calculated, modify `raven.update_coinc_far` accordingly.
- [ ] If change publishing conditions, modify `raven.trigger_raven_alert`.
- [ ] If creating sky maps from RA/dec/error, add to list of pipelines in `external_skymaps.py`.
- [ ] Add both O3 replay and MDC testing in [`first2years_external.py`](https://git.ligo.org/emfollow/gwcelery/-/blob/main/gwcelery/tasks/first2years_external.py).
- [ ] Add [tests](https://git.ligo.org/emfollow/gwcelery/-/tree/main/gwcelery/tests) for all of the changes made. Ensure 100% testing coverage moving forward.

/label ~External Triggers
