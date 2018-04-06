"""Rapid sky localization with BAYESTAR."""
import io
import logging
import os

from celery import group

from ..celery import app
from .gracedb import download, upload

# Workaround for testing in an environment that lacks LALInference
try:
    from lalinference.io.events import DetectorDisabledError
except ImportError:  # pragma: no cover
    DetectorDisabledError = ValueError

log = logging.getLogger('BAYESTAR')


def bayestar(graceid, service):
    """Peform end-to-end rapid sky localization with BAYESTAR, including
    GraceDB downloads and uploads.

    This function downloads all of the required inputs for BAYESTAR,
    runs rapid sky localization with a few different detector settings,
    and uploads the resulting sky map FITS files to GraceDB.

    Internally, all of the heavy lifting is done by :meth:`localize`.
    """
    coinc = download('coinc.xml', graceid, service)
    psd = download('psd.xml.gz', graceid, service)
    coinc_psd = (coinc, psd)
    return group(
        localize.s(coinc_psd, graceid, service) |
        upload.s('bayestar.fits.gz', graceid, service,
                 'sky localization complete', 'sky_loc'),
        localize.s(coinc_psd, graceid, service,
                   disabled_detectors=['V1'],
                   filename='bayestar_no_virgo.fits.gz') |
        upload.s('bayestar_no_virgo.fits.gz', graceid, service,
                 'sky localization complete', 'sky_loc'))


# FIXME: should be `throws=events.DetectorDisabledError, but that would add
# a real on lalinference.
@app.task(queue='openmp', shared=False, throws=DetectorDisabledError)
def localize(coinc_psd, graceid, service, filename='bayestar.fits.gz',
             disabled_detectors=None):
    """Do the heavy lifting of generating a rapid localization using BAYESTAR.

    This function runs the computationally-intensive part of BAYESTAR.
    The `coinc.xml` and `psd.xml.gz` files should already have been downloaded
    by :func:`bayestar`.

    This task should execute in a special queue for computationally intensive
    OpenMP parallel tasks.
    """
    from lalinference.io import events
    from lalinference.io import fits
    try:
        # FIXME: class moved
        from lalinference.bayestar.command import TemporaryDirectory
    except ImportError:
        from lalinference.util.file import TemporaryDirectory
    from lalinference.bayestar.sky_map import localize, rasterize
    from ligo.gracedb.logging import GraceDbLogHandler
    from ligo.gracedb.rest import GraceDb

    handler = GraceDbLogHandler(GraceDb(service), graceid)
    handler.setLevel(logging.INFO)
    log.addHandler(handler)

    try:
        # A little bit of Cylon humor
        log.info('by your command...')

        # Parse event
        coinc, psd = coinc_psd
        coinc = io.BytesIO(coinc)
        psd = io.BytesIO(psd)
        event_source = events.ligolw.open(coinc, psd_file=psd, coinc_def=None)
        if disabled_detectors:
            event_source = events.detector_disabled.open(
                event_source, disabled_detectors)
        event, = event_source.values()

        # Run BAYESTAR
        log.info("starting sky localization")
        skymap = rasterize(localize(event))
        skymap.meta['objid'] = str(graceid)
        log.info("sky localization complete")

        with TemporaryDirectory() as tmpdir:
            fitspath = os.path.join(tmpdir, filename)
            fits.write_sky_map(fitspath, skymap, nest=True)
            return open(fitspath, 'rb').read()
    except:
        # Produce log message for any otherwise uncaught exception
        log.exception("sky localization failed")
        raise
    finally:
        log.removeHandler(handler)
        del handler
