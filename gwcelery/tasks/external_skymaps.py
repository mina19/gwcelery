"""Create and upload external sky maps."""
from astropy import units as u
from astropy.coordinates import ICRS, SkyCoord
import astropy_healpix as ah
from astropy_healpix import HEALPix, pixel_resolution_to_nside
from base64 import b64decode
from celery import group
#  import astropy.utils.data
import numpy as np
from ligo.skymap.io import fits
from ligo.skymap.distance import parameters_to_marginal_moments
from ligo.skymap.tool import ligo_skymap_combine
import gcn
import healpy as hp
import io
import lxml.etree
import re
import ssl
import urllib
from urllib.error import HTTPError

from ..import app
from . import gracedb
from . import skymaps
from ..util.cmdline import handling_system_exit
from ..util.tempfile import NamedTemporaryFile
from ..import _version


COMBINED_SKYMAP_FILENAME_MULTIORDER = 'combined-ext.multiorder.fits'
"""Filename of combined sky map in a multiordered format"""

COMBINED_SKYMAP_FILENAME_FLAT = 'combined-ext.fits.gz'
"""Filename of combined sky map in a flattened format"""

COMBINED_SKYMAP_FILENAME_PNG = 'combined-ext.png'
"""Filename of combined sky map plot"""


@app.task(shared=False)
def create_combined_skymap(se_id, ext_id, preferred_event=None):
    """Creates and uploads the combined LVK-external skymap, uploading to the
    external trigger GraceDB page. The filename used for the combined sky map
    will be 'combined-ext.multiorder.fits' if the GW sky map is multi-ordered
    or 'combined-ext.fits.gz' if the GW sky map is not. Will use the GW sky map
    in from the preferred event if given.

    Parameters
    ----------
    se_id : str
        Superevent GraceDB ID
    ext_id: str
        External event GraceDB ID
    preferred_event: str
        Preferred event GraceDB ID. If given, use sky map from preferred event

    """
    # gw_id is either superevent or preferred event graceid
    gw_id = preferred_event if preferred_event else se_id
    gw_skymap_filename = get_skymap_filename(gw_id, is_gw=True)
    ext_skymap_filename = get_skymap_filename(ext_id, is_gw=False)
    # Determine whether GW sky map is multiordered or flat
    gw_moc = '.multiorder.fits' in gw_skymap_filename

    new_filename = \
        (COMBINED_SKYMAP_FILENAME_MULTIORDER if gw_moc else
         COMBINED_SKYMAP_FILENAME_FLAT)

    message = \
        ('Combined LVK-external sky map using {0} and {1}, with {2} and '
         '{3}'.format(gw_id, ext_id, gw_skymap_filename, ext_skymap_filename))
    message_png = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=ext_id,
            filename=new_filename)

    (
        _download_skymaps.si(
            gw_skymap_filename, ext_skymap_filename, gw_id, ext_id
        )
        |
        combine_skymaps.s(gw_moc=gw_moc)
        |
        group(
            gracedb.upload.s(
                new_filename, ext_id, message, ['sky_loc', 'ext_coinc']
            )
            |
            gracedb.create_label.si('COMBINEDSKYMAP_READY', ext_id),

            skymaps.plot_allsky.s()
            |
            gracedb.upload.s(COMBINED_SKYMAP_FILENAME_PNG, ext_id,
                             message_png, ['sky_loc', 'ext_coinc'])
        )
    ).delay()


@app.task(autoretry_for=(ValueError,), retry_backoff=10,
          retry_backoff_max=600)
def get_skymap_filename(graceid, is_gw):
    """Get the skymap fits filename.

    If not available, will try again 10 seconds later, then 20, then 40, etc.
    until up to 10 minutes after initial attempt.

    Parameters
    ----------
    graceid : str
        GraceDB ID
    is_gw: bool
        If True, uses method for superevent or preferred event. Otherwise uses
        method for external event.

    """
    gracedb_log = gracedb.get_log(graceid)
    if is_gw:
        # Try first to get a multiordered sky map
        for message in reversed(gracedb_log):
            filename = message['filename']
            v = message['file_version']
            fv = '{},{}'.format(filename, v)
            if filename.endswith('.multiorder.fits') and \
                    "combined-ext." not in filename:
                return fv
        # Try next to get a flattened sky map
        for message in reversed(gracedb_log):
            filename = message['filename']
            v = message['file_version']
            fv = '{},{}'.format(filename, v)
            if filename.endswith('.fits.gz') and \
                    "combined-ext." not in filename:
                return fv
    else:
        for message in reversed(gracedb_log):
            filename = message['filename']
            v = message['file_version']
            fv = '{},{}'.format(filename, v)
            if (filename.endswith('.fits') or filename.endswith('.fit') or
                    filename.endswith('.fits.gz')) and \
                    "combined-ext." not in filename:
                return fv
    raise ValueError('No skymap available for {0} yet.'.format(graceid))


@app.task(shared=False)
def _download_skymaps(gw_filename, ext_filename, gw_id, ext_id):
    """Download both superevent and external sky map to be combined."""
    gw_skymap = gracedb.download(gw_filename, gw_id)
    ext_skymap = gracedb.download(ext_filename, ext_id)
    return gw_skymap, ext_skymap


def combine_skymaps_moc_flat(gw_sky, ext_sky, ext_header):
    """This function combines a multiordered (MOC) GW sky map with a flattened
    external one by reweighting the MOC sky map using the values of the
    flattened one.

    Header info is generally inherited from the GW sky map or recalculated
    using the combined sky map values.
    """
    #  Find ra/dec of each GW pixel
    level, ipix = ah.uniq_to_level_ipix(gw_sky["UNIQ"])
    nsides = ah.level_to_nside(level)
    areas = ah.nside_to_pixel_area(nsides)
    ra_gw, dec_gw = ah.healpix_to_lonlat(ipix, nsides, order='nested')
    #  Find corresponding external sky map indicies
    ext_nside = ah.npix_to_nside(len(ext_sky))
    ext_ind = ah.lonlat_to_healpix(
        ra_gw, dec_gw, ext_nside,
        order='nested' if ext_header['nest'] else 'ring')
    #  Reweight GW prob density by external sky map probabilities
    gw_sky['PROBDENSITY'] *= ext_sky[ext_ind]
    gw_sky['PROBDENSITY'] /= \
        np.sum(gw_sky['PROBDENSITY'] * areas).value
    #  Modify GW sky map with new data, ensuring they exist first
    if 'DISTMU' in gw_sky.keys() and 'DISTSIGMA' in gw_sky.keys():
        distmean, diststd = parameters_to_marginal_moments(
            gw_sky['PROBDENSITY'] * areas.value,
            gw_sky['DISTMU'], gw_sky['DISTSIGMA'])
        gw_sky.meta['distmean'], gw_sky.meta['diststd'] = distmean, diststd
    if 'instruments' not in ext_header:
        ext_header.update({'instruments': {'external instrument'}})
    if 'instruments' in gw_sky.meta:
        gw_sky.meta['instruments'].update(ext_header['instruments'])
    if 'HISTORY' in gw_sky.meta:
        ext_instrument = list(ext_header['instruments'])[0]
        gw_sky.meta['HISTORY'].extend([
            '', 'The values were reweighted by using data from {0}{1}'.format(
                ('an ' if ext_instrument == 'external instrument'
                 else ''),
                ext_instrument)])
    return gw_sky


@app.task(shared=False)
def combine_skymaps(skymapsbytes, gw_moc=True):
    """This task combines the two input skymaps, in this case the external
    trigger skymap and the LVK skymap and writes to a temporary output file. It
    then returns the contents of the file as a byte array.

    There are separate methods in case the GW sky map is multiordered (we just
    reweight using the external sky map) or flattened (use standard
    ligo.skymap.combine method).
    """
    gw_skymap_bytes, ext_skymap_bytes = skymapsbytes
    suffix = ".fits" if gw_moc else ".fits.gz"
    with NamedTemporaryFile(mode='rb', suffix=suffix) as combinedskymap, \
            NamedTemporaryFile(content=gw_skymap_bytes) as gw_skymap_file, \
            NamedTemporaryFile(content=ext_skymap_bytes) as ext_skymap_file, \
            handling_system_exit():

        # If GW sky map is multiordered, use reweighting method
        if gw_moc:
            #  FIXME: Use method that regrids the combined sky map e.g. mhealpy
            #  once this method is quicker and preserves the header
            #  Load sky maps
            gw_skymap = fits.read_sky_map(gw_skymap_file.name, moc=True)
            ext_skymap, ext_header = fits.read_sky_map(ext_skymap_file.name,
                                                       moc=False)
            #  Create and write combined sky map
            combined_skymap = combine_skymaps_moc_flat(gw_skymap, ext_skymap,
                                                       ext_header)
            fits.write_sky_map(combinedskymap.name, combined_skymap, moc=True)
        # If GW sky map is flattened, use older method
        else:
            ligo_skymap_combine.main([gw_skymap_file.name,
                                      ext_skymap_file.name,
                                      combinedskymap.name])
        #  FIXME: Add method for MOC-MOC if there is a switch to MOC external
        #  skymaps
        return combinedskymap.read()


@app.task(shared=False)
def external_trigger(graceid):
    """Returns the associated external trigger GraceDB ID."""
    em_events = gracedb.get_superevent(graceid)['em_events']
    if len(em_events):
        for exttrig in em_events:
            if gracedb.get_event(exttrig)['search'] == 'GRB':
                return exttrig
    raise ValueError('No associated GRB EM event(s) for {0}.'.format(graceid))


@app.task(shared=False)
def external_trigger_heasarc(external_id):
    """Returns the HEASARC fits file link."""
    gracedb_log = gracedb.get_log(external_id)
    for message in gracedb_log:
        if 'Original Data' in message['comment']:
            filename = message['filename']
            xmlfile = gracedb.download(urllib.parse.quote(filename),
                                       external_id)
            root = lxml.etree.fromstring(xmlfile)
            heasarc_url = root.find('./What/Param[@name="LightCurve_URL"]'
                                    ).attrib['value']
            return re.sub(r'quicklook(.*)', 'current/', heasarc_url)
    raise ValueError('Not able to retrieve HEASARC link for {0}.'.format(
        external_id))


@app.task(autoretry_for=(gracedb.RetryableHTTPError,), retry_backoff=10,
          max_retries=14)
def get_external_skymap(link, search):
    """Download the Fermi sky map fits file and return the contents as a byte
    array. If GRB, will construct a HEASARC url, while if SubGRB, will use the
    link directly.

    If not available, will try again 10 seconds later, then 20, then 40, etc.
    until up to 15 minutes after initial attempt.
    """
    if search == 'GRB':
        # if Fermi GRB, determine final HEASARC link
        trigger_id = re.sub(r'.*\/(\D+?)(\d+)(\D+)\/.*', r'\2', link)
        skymap_name = 'glg_healpix_all_bn{0}_v00.fit'.format(trigger_id)
        skymap_link = link + skymap_name
    elif search in {'SubGRB', 'FromURL'}:
        skymap_link = link
    #  FIXME: Under Anaconda on the LIGO Caltech computing cluster, Python
    #  (and curl, for that matter) fail to negotiate TLSv1.2 with
    #  heasarc.gsfc.nasa.gov
    context = ssl.create_default_context()
    context.options |= ssl.OP_NO_TLSv1_3
    #  return astropy.utils.data.get_file_contents(
    #      (skymap_link), encoding='binary', cache=False)
    try:
        response = urllib.request.urlopen(skymap_link, context=context)
        return response.read()
    except HTTPError as e:
        if e.code == 404:
            raise gracedb.RetryableHTTPError("Failed to download the sky map."
                                             "Retrying...")
        else:
            raise


@app.task(shared=False)
def read_upload_skymap_from_base64(event, skymap_str):
    """Decode and upload 64base encoded sky maps from Kafka alerts"""

    graceid = event['graceid']
    ra = event['extra_attributes']['GRB']['ra']
    dec = event['extra_attributes']['GRB']['dec']
    skymap_filename = event['pipeline'].lower() + '_skymap.fits.gz'

    # Decode base64 encoded string to bytes string
    skymap_data = b64decode(skymap_str)

    message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=graceid, filename=skymap_filename)

    (
        group(
            gracedb.upload.si(skymap_data,
                              skymap_filename,
                              graceid,
                              'Sky map uploaded from {}'.format(
                                  event['pipeline']),
                              ['sky_loc']),

            skymaps.plot_allsky.si(skymap_data, ra=ra, dec=dec)
            |
            gracedb.upload.s(event['pipeline'].lower() + '_skymap.png',
                             graceid,
                             message,
                             ['sky_loc'])
        )
        |
        gracedb.create_label.si('EXT_SKYMAP_READY', graceid)
    ).delay()


@app.task(autoretry_for=(urllib.error.HTTPError, urllib.error.URLError,),
          retry_backoff=10, retry_backoff_max=1200)
def get_upload_external_skymap(event, skymap_link=None):
    """If a Fermi sky map is not uploaded yet, tries to download one and upload
    to external event. If sky map is not available, passes so that this can be
    re-run the next time an update GCN notice is received. If GRB, will
    construct a HEASARC url, while if SubGRB, will use the link directly.
    If SubGRB or FromURL, downloads a skymap using the provided URL rather
    than construct one.
    """
    graceid = event['graceid']
    search = event['search']

    if search == 'GRB':
        external_skymap_canvas = (
            external_trigger_heasarc.si(graceid)
            |
            get_external_skymap.s(search)
        )
    elif search in {'SubGRB', 'SubGRBTargeted', 'FromURL'}:
        external_skymap_canvas = get_external_skymap.si(skymap_link, search)

    skymap_filename = \
        ('external_from_url' if search == 'FromURL'
         else 'glg_healpix_all_bn_v00')

    fits_message = \
        ('Downloaded from {}.'.format(skymap_link) if search == 'FromURL'
         else 'Official sky map from Fermi analysis.')
    png_message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=graceid, filename=skymap_filename + '.fits')

    (
        external_skymap_canvas
        |
        group(
            gracedb.upload.s(
                skymap_filename + '.fits',
                graceid,
                fits_message,
                ['sky_loc']),

            skymaps.plot_allsky.s()
            |
            gracedb.upload.s(skymap_filename + '.png',
                             graceid,
                             png_message,
                             ['sky_loc'])
        )
        |
        gracedb.create_label.si('EXT_SKYMAP_READY', graceid)
    ).delay()


def create_external_skymap(ra, dec, error, pipeline, notice_type=111):
    """Create a sky map, either a gaussian or a single
    pixel sky map, given an RA, dec, and error radius.

    If from Fermi, convolves the sky map with both a core and
    tail Gaussian and then sums these to account for systematic
    effects as measured in :doi:`10.1088/0067-0049/216/2/32`

    If from Swift, converts the error radius from that containing 90% of the
    credible region to ~68% (see description of Swift error
    here:`https://gcn.gsfc.nasa.gov/swift.html#tc7`)

    Parameters
    ----------
    ra : float
        right ascension in deg
    dec: float
        declination in deg
    error: float
        error radius in deg

    Returns
    -------
    skymap : numpy array
        sky map array

    """
    max_nside = 2048
    if error:
        # Correct 90% containment to 1-sigma for Swift
        if pipeline == 'Swift':
            error /= np.sqrt(-2 * np.log1p(-.9))
        error_radius = error * u.deg
        # Use minimum nside of 256
        nside = max(pixel_resolution_to_nside(error_radius, round='up'), 256)
    else:
        nside = np.inf
    if nside >= max_nside:
        nside = max_nside

        #  Find the one pixel the event can localized to
        hpx = HEALPix(nside, 'ring', frame=ICRS())
        skymap = np.zeros(hpx.npix)
        ind = hpx.lonlat_to_healpix(ra * u.deg, dec * u.deg)
        skymap[ind] = 1.
    else:
        #  If larger error, create gaussian sky map
        hpx = HEALPix(nside, 'ring', frame=ICRS())
        ipix = np.arange(hpx.npix)

        #  Evaluate Gaussian.
        center = SkyCoord(ra * u.deg, dec * u.deg)
        distance = hpx.healpix_to_skycoord(ipix).separation(center)
        skymap = np.exp(-0.5 * np.square(distance / error_radius).to_value(
            u.dimensionless_unscaled))
        skymap /= skymap.sum()
    if pipeline == 'Fermi':
        # Correct for Fermi systematics based on recommendations from GBM team
        # Convolve with both a narrow core and wide tail Gaussian with error
        # radius determined by the scales respectively, each comprising a
        # fraction determined by the weights respectively
        if notice_type == gcn.NoticeType.FERMI_GBM_FLT_POS:
            # Flight notice
            # Values from first row of Table 7
            weights = [0.897, 0.103]
            scales = [7.52, 55.6]
        elif notice_type == gcn.NoticeType.FERMI_GBM_GND_POS:
            # Ground notice
            # Values from first row of Table 3
            weights = [0.804, 0.196]
            scales = [3.72, 13.7]
        elif notice_type == gcn.NoticeType.FERMI_GBM_FIN_POS:
            # Final notice
            # Values from second row of Table 3
            weights = [0.900, 0.100]
            scales = [3.71, 14.3]
        else:
            raise AssertionError(
                'Need to provide a supported Fermi notice type')
        skymap = sum(
            weight * hp.sphtfunc.smoothing(skymap, sigma=np.radians(scale))
            for weight, scale in zip(weights, scales))

    # Renormalize due to possible lack of precision
    # Enforce the skymap to be non-negative
    return np.abs(skymap) / np.abs(skymap).sum()


def write_to_fits(skymap, event, notice_type, notice_date):
    """Write external sky map fits file, populating the
    header with relevant info.

    Parameters
    ----------
    skymap : numpy array
        sky map array
    event : dict
        Dictionary of Swift external event

    Returns
    -------
    skymap fits : bytes array
        bytes array of sky map

    """
    notice_type_dict = {
        '53': 'INTEGRAL_WAKEUP',
        '54': 'INTEGRAL_REFINED',
        '55': 'INTEGRAL_OFFLINE',
        '60': 'SWIFT_BAT_GRB_ALERT',
        '61': 'SWIFT_BAT_GRB_POSITION',
        '105': 'AGILE_MCAL_ALERT',
        '110': 'FERMI_GBM_ALERT',
        '111': 'FERMI_GBM_FLT_POS',
        '112': 'FERMI_GBM_GND_POS',
        '115': 'FERMI_GBM_FINAL_POS',
        '131': 'FERMI_GBM_SUBTHRESHOLD'}

    if notice_type is None:
        msgtype = event['pipeline'] + '_LVK_TARGETED_SEARCH'
    else:
        msgtype = notice_type_dict[str(notice_type)]

    gcn_id = event['extra_attributes']['GRB']['trigger_id']
    with NamedTemporaryFile(suffix='.fits.gz') as f:
        fits.write_sky_map(f.name, skymap,
                           objid=gcn_id,
                           url=event['links']['self'],
                           instruments=event['pipeline'],
                           gps_time=event['gpstime'],
                           msgtype=msgtype,
                           msgdate=notice_date,
                           creator='gwcelery',
                           origin='LIGO-VIRGO-KAGRA',
                           vcs_version=_version.get_versions()['version'],
                           history='file only for internal use')
        with open(f.name, 'rb') as file:
            return file.read()


@app.task(shared=False)
def create_upload_external_skymap(event, notice_type, notice_date):
    """Create and upload external sky map using
    RA, dec, and error radius information.

    Parameters
    ----------
    event : dict
        Dictionary of Swift external event

    """
    graceid = event['graceid']
    skymap_filename = event['pipeline'].lower() + '_skymap.fits.gz'

    ra = event['extra_attributes']['GRB']['ra']
    dec = event['extra_attributes']['GRB']['dec']
    error = event['extra_attributes']['GRB']['error_radius']
    pipeline = event['pipeline']

    if not (ra or dec or error):
        # Don't create sky map if notice only contains zeros, lacking info
        return
    skymap = create_external_skymap(ra, dec, error, pipeline, notice_type)

    skymap_data = write_to_fits(skymap, event, notice_type, notice_date)

    message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=graceid, filename=skymap_filename)

    (
        gracedb.upload.si(skymap_data,
                          skymap_filename,
                          graceid,
                          'Sky map created from GCN RA, dec, and error.',
                          ['sky_loc'])
        |
        skymaps.plot_allsky.si(skymap_data, ra=ra, dec=dec)
        |
        gracedb.upload.s(event['pipeline'].lower() + '_skymap.png',
                         graceid,
                         message,
                         ['sky_loc'])
        |
        gracedb.create_label.si('EXT_SKYMAP_READY', graceid)
    ).delay()


@app.task(shared=False)
def plot_overlap_integral(coinc_far_dict, superevent, ext_event,
                          var_label=r"\mathcal{I}_{\Omega}"):
    """Plot and upload visualization of the sky map overlap integral computed
    by ligo.search.overlap_integral.

    Parameters
    ----------
    coinc_far_dict : dict
        Dictionary containing coincidence false alarm rate results from
        RAVEN
    superevent : dict
        superevent dictionary
    ext_event : dict
        external event dictionary
    var_label : str
        The variable symbol used in plotting

    """
    if coinc_far_dict.get('skymap_overlap') is None:
        return
    if superevent['em_type'] != ext_event['graceid'] and \
            'RAVEN_ALERT' in superevent['labels']:
        return

    superevent_id = superevent['superevent_id']
    ext_id = ext_event['graceid']

    log_overlap = np.log(coinc_far_dict['skymap_overlap'])
    logI_string = np.format_float_positional(log_overlap, 1, trim='0',
                                             sign=True)
    # Create plot
    fig, _ = skymaps.plot_bayes_factor(
        log_overlap, values=(1, 3, 5), xlim=7, var_label=var_label,
        title=(r'Sky Map Overlap between %s and %s [$\ln\,%s = %s$]' %
               (superevent_id, ext_id, var_label, logI_string)))
    # Convert to bytes
    outfile = io.BytesIO()
    fig.savefig(outfile, format='png')
    # Upload file
    gracedb.upload.si(
        outfile.getvalue(),
        'overlap_integral.png',
        superevent_id,
        message='Sky map overlap integral between {0} and {1}'.format(
            superevent_id, ext_id),
        tags=['ext_coinc']
    ).delay()
