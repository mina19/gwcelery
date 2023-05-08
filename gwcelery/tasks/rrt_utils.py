from . import gracedb
import json
from ..util.tempfile import NamedTemporaryFile
from ..import app
from ligo.skymap.io import read_sky_map
from ligo.skymap.postprocess.crossmatch import crossmatch


@app.task(shared=False)
def check_high_profile(skymap, em_bright,
                       p_astro, superevent):
    superevent_id = superevent['superevent_id']
    # conditions are defined in L2100046
    # RAVEN_ALERT HIGH_PROFILE is implemented in raven.py
    # Checking if the label is applied beforehand
    if 'HIGH_PROFILE' in superevent['labels']:
        return "HIGH_PROFILE already applied"

    # low-far unmodelled burst condition
    far_list = []
    gw_events = superevent["gw_events"]
    for event in gw_events:
        events_dict = gracedb.get_event(event)
        far_list.append({"group": events_dict["group"],
                         "search": events_dict["search"],
                         "far": events_dict["far"]})
    far_list_sorted = sorted(far_list, key=lambda k: k["far"])

    if far_list_sorted[0]["group"] == "Burst" and \
       far_list_sorted[0]["search"] != "BBH":
        gracedb.create_label.si(
            'HIGH_PROFILE', superevent_id).delay()
        return "Significant Burst event. Applying label"

    # annotation number condition
    preferred_event = superevent['preferred_event_data']
    if preferred_event["group"] == "CBC":
        em_bright_dict = json.loads(em_bright)
        has_remnant = em_bright_dict['HasRemnant']

        pastro_dict = json.loads(p_astro)
        p_bns = pastro_dict['BNS']
        p_terr = pastro_dict['Terrestrial']
        p_nsbh = pastro_dict['NSBH']

        with NamedTemporaryFile(content=skymap) as skymap_file:
            gw_skymap = read_sky_map(skymap_file.name, moc=True)
            cl = 90
            result = crossmatch(gw_skymap, contours=[cl / 100])
            sky_area = result.contour_areas[0]

        if p_terr < 0.5:
            if (p_bns > 0.1 or p_nsbh > 0.1 or has_remnant > 0.1 or sky_area < 100):  # noqa: E501
                gracedb.create_label.si(
                    'HIGH_PROFILE', superevent_id).delay()
                return "Annotations condition satisfied. Applying label"
    return "No conditions satisfied. Skipping"
