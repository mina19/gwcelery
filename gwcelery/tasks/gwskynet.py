"""GWSkyNet annotation with GWSkyNet model"""
import json

from GWSkyNet import GWSkyNet

from . import gracedb, igwn_alert
from ..import app
from ..util.tempfile import NamedTemporaryFile
from ..util import PromiseProxy

GWSkyNet_model = PromiseProxy(GWSkyNet.load_GWSkyNet_model)


@app.task(shared=False)
def gwskynet_annotation(filecontents):
    """Perform the series of tasks necessary for GWSkyNet to

    Parameters
    ----------
    filecontents : bytes
            the sky map downloaded from gracedb
    GWSkyNet_model : keras.engine.functional.Functional object
            the GWSkyNet model used to annotate the events
    """
    with NamedTemporaryFile(content=filecontents) as fitsfile:
        GWSkyNet_input = GWSkyNet.prepare_data(fitsfile.name)
    class_score = GWSkyNet.predict(GWSkyNet_model, GWSkyNet_input)
    FAR, FNR = GWSkyNet.get_rates(class_score)
    far = FAR[0]
    fnr = FNR[0]
    cs = class_score[0]
    gwskynet_output = {'class_score': cs, 'FAR': far, 'FNR': fnr}
    return json.dumps(gwskynet_output)


@igwn_alert.handler('cbc_gstlal',
                    'cbc_spiir',
                    'cbc_pycbc',
                    'cbc_mbta',
                    shared=False)
def handle_cbc_event(alert):
    """"Annotate CBC events using GWSkyNet
    """
    graceid = alert['uid']
    filename = 'gwskynet.json'
    if alert['alert_type'] == 'label_added':
        label_name = alert['data']['name']
        if label_name == 'SKYMAP_READY':
            skymap_filename = 'bayestar.multiorder.fits'
            (
                gracedb.download.s(skymap_filename, graceid)
                |
                gwskynet_annotation.s()
                |
                gracedb.upload.s(filename,
                                 graceid,
                                 message=('GWSkyNet annotation from '
                                          '<a href='
                                          '"/api/events/{graceid}/files/'
                                          '{skymap_filename}">'
                                          '{skymap_filename}</a>').format(
                                     graceid=graceid,
                                     skymap_filename=skymap_filename), tags=())
            ).apply_async()
