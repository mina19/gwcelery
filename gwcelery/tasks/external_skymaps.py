"""Create and upload external sky maps."""
import io
import re
import ssl
import urllib
from base64 import b64decode
from urllib.error import HTTPError

import astropy_healpix as ah
import gcn
import lxml.etree
#  import astropy.utils.data
import numpy as np
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.io.fits import getheader
from celery import group
from hpmoc import PartialUniqSkymap
from hpmoc.utils import reraster, uniq_intersection
from ligo.skymap.distance import parameters_to_marginal_moments
from ligo.skymap.io import fits
from ligo.skymap.moc import bayestar_adaptive_grid
from ligo.skymap.plot.bayes_factor import plot_bayes_factor

from .. import _version, app
from ..util import closing_figures
from ..util.cmdline import handling_system_exit
from ..util.tempfile import NamedTemporaryFile
from . import gracedb, skymaps

COMBINED_SKYMAP_FILENAME_MULTIORDER = 'combined-ext.multiorder.fits'
"""Filename of combined sky map in a multiordered format"""

COMBINED_SKYMAP_FILENAME_PNG = 'combined-ext.png'
"""Filename of combined sky map plot"""

FERMI_OFFICIAL_SKYMAP_FILENAME = 'glg_healpix_all_bn_v00'
"""Filename of sky map from official Fermi GBM analysis"""

NOTICE_TYPE_DICT = {
        '53': 'INTEGRAL_WAKEUP',
        '54': 'INTEGRAL_REFINED',
        '55': 'INTEGRAL_OFFLINE',
        '60': 'SWIFT_BAT_GRB_ALERT',
        '61': 'SWIFT_BAT_GRB_POSITION',
        '110': 'FERMI_GBM_ALERT',
        '111': 'FERMI_GBM_FLT_POS',
        '112': 'FERMI_GBM_GND_POS',
        '115': 'FERMI_GBM_FINAL_POS',
        '131': 'FERMI_GBM_SUBTHRESHOLD'
}


@app.task(shared=False)
def create_combined_skymap(se_id, ext_id, preferred_event=None):
    """Creates and uploads a combined LVK-external skymap, uploading to the
    external trigger GraceDB page. The filename used for the combined sky map
    will be 'combined-ext.multiorder.fits' if the external sky map is
    multi-ordered or 'combined-ext.fits.gz' if the external sky map is not.
    Will use the GW sky map in from the preferred event if given.

    Parameters
    ----------
    se_id : str
        Superevent GraceDB ID
    ext_id : str
        External event GraceDB ID
    preferred_event : str
        Preferred event GraceDB ID. If given, use sky map from preferred event

    """
    # gw_id is either superevent or preferred event graceid
    gw_id = preferred_event if preferred_event else se_id
    gw_skymap_filename = get_skymap_filename(gw_id, is_gw=True)
    ext_skymap_filename = get_skymap_filename(ext_id, is_gw=False)
    # Determine whether external sky map is multiordered or flat
    ext_moc = '.multiorder.fits' in ext_skymap_filename

    message = \
        ('Combined LVK-external sky map using {0} and {1}, with {2} and '
         '{3}'.format(gw_id, ext_id, gw_skymap_filename, ext_skymap_filename))
    message_png = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=ext_id,
            filename=COMBINED_SKYMAP_FILENAME_MULTIORDER)

    (
        _download_skymaps.si(
            gw_skymap_filename, ext_skymap_filename, gw_id, ext_id
        )
        |
        combine_skymaps.s(ext_moc=ext_moc)
        |
        group(
            gracedb.upload.s(
                COMBINED_SKYMAP_FILENAME_MULTIORDER,
                ext_id, message, ['sky_loc', 'ext_coinc']
            ),

            skymaps.plot_allsky.s()
            |
            gracedb.upload.s(COMBINED_SKYMAP_FILENAME_PNG, ext_id,
                             message_png, ['sky_loc', 'ext_coinc'])
        )
        |
        gracedb.create_label.si('COMBINEDSKYMAP_READY', ext_id)
    ).delay()


@app.task(autoretry_for=(ValueError,), retry_backoff=10,
          retry_backoff_max=600)
def get_skymap_filename(graceid, is_gw):
    """Get the skymap FITS filename.

    If not available, will try again 10 seconds later, then 20, then 40, etc.
    up to a max 10 minutes retry delay.

    Parameters
    ----------
    graceid : str
        GraceDB ID
    is_gw : bool
        If True, uses method for superevent or preferred event. Otherwise uses
        method for external event.

    Returns
    -------
    filename : str
        Filename of latest sky map

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


def is_skymap_moc(skymap_bytes):
    with NamedTemporaryFile(content=skymap_bytes) as skymap_file:
        # If ordering is EXPLICIT, should be multiordered (MOC)
        return getheader(skymap_file.name, ext=1)['INDXSCHM'] == 'EXPLICIT'


@app.task(shared=False)
def _download_skymaps(gw_filename, ext_filename, gw_id, ext_id):
    """Download both superevent and external sky map to be combined.

    Parameters
    ----------
    gw_filename : str
        GW sky map filename
    ext_filename : str
        External sky map filename
    gw_id : str
        GraceDB ID of GW candidate, either superevent or preferred event
    ext_id : str
        GraceDB ID of external candidate

    Returns
    -------
    gw_skymap, ext_skymap : tuple
        Tuple of gw_skymap and ext_skymap bytes

    """
    gw_skymap = gracedb.download(gw_filename, gw_id)
    ext_skymap = gracedb.download(ext_filename, ext_id)
    return gw_skymap, ext_skymap


def combine_skymaps_moc_moc(gw_sky, ext_sky):
    """This function combines a multi-ordered (MOC) GW sky map with a MOC
    external skymap.
    """
    gw_sky_hpmoc = PartialUniqSkymap(gw_sky["PROBDENSITY"], gw_sky["UNIQ"],
                                     name="PROBDENSITY", meta=gw_sky.meta)
    # Determine the column name in ext_sky and rename it as PROBDENSITY.
    ext_sky_hpmoc = PartialUniqSkymap(ext_sky["PROBDENSITY"], ext_sky["UNIQ"],
                                      name="PROBDENSITY", meta=ext_sky.meta)

    comb_sky_hpmoc = gw_sky_hpmoc * ext_sky_hpmoc
    comb_sky_hpmoc /= np.sum(comb_sky_hpmoc.s * comb_sky_hpmoc.area())
    comb_sky = comb_sky_hpmoc.to_table(name='PROBDENSITY')

    #  Modify GW sky map with new data, ensuring they exist first
    if 'DISTMU' in gw_sky.keys() and 'DISTSIGMA' in gw_sky.keys():
        UNIQ = comb_sky['UNIQ']
        UNIQ_ORIG = gw_sky['UNIQ']
        intersection = uniq_intersection(UNIQ_ORIG, UNIQ)
        DIST_MU = reraster(UNIQ_ORIG,
                           gw_sky["DISTMU"],
                           UNIQ,
                           method='copy',
                           intersection=intersection)
        DIST_SIGMA = reraster(UNIQ_ORIG,
                              gw_sky["DISTSIGMA"],
                              UNIQ,
                              method='copy',
                              intersection=intersection)
        DIST_NORM = reraster(UNIQ_ORIG,
                             gw_sky["DISTNORM"],
                             UNIQ,
                             method='copy',
                             intersection=intersection)
        comb_sky.add_columns([DIST_MU, DIST_SIGMA, DIST_NORM],
                             names=['DISTMU', 'DISTSIGMA', 'DISTNORM'])

        distmean, diststd = parameters_to_marginal_moments(
            comb_sky['PROBDENSITY'] * comb_sky_hpmoc.area().value,
            comb_sky['DISTMU'], comb_sky['DISTSIGMA'])
        comb_sky.meta['distmean'], comb_sky.meta['diststd'] = distmean, diststd
    if 'instruments' not in ext_sky.meta:
        ext_sky.meta.update({'instruments': {'external instrument'}})
    if 'instruments' in comb_sky.meta:
        comb_sky.meta['instruments'].update(ext_sky.meta['instruments'])
    if 'HISTORY' in comb_sky.meta:
        ext_instrument = list(ext_sky.meta['instruments'])[0]
        comb_sky.meta['HISTORY'].extend([
            '', 'The values were reweighted by using data from {0}{1}'.format(
                ('an ' if ext_instrument == 'external instrument'
                 else ''),
                ext_instrument)])

    # Remove redundant field
    if 'ORDERING' in comb_sky.meta:
        del comb_sky.meta['ORDERING']
    return comb_sky


def combine_skymaps_moc_flat(gw_sky, ext_sky, ext_header):
    """This function combines a multi-ordered (MOC) GW sky map with a flattened
    external one by re-weighting the MOC sky map using the values of the
    flattened one.

    Header info is generally inherited from the GW sky map or recalculated
    using the combined sky map values.

    Parameters
    ----------
    gw_sky : Table
        GW sky map astropy Table
    ext_sky : array
        External sky map array
    ext_header : dict
        Header of external sky map

    Returns
    -------
    comb_sky : Table
        Table of combined sky map

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
    #  Fix any negative values and normalize
    ext_sky[ext_sky < 0.] = 0.
    ext_sky /= np.sum(ext_sky)
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
def combine_skymaps(skymapsbytes, ext_moc=True):
    """This task combines the two input sky maps, in this case the external
    trigger skymap and the LVK skymap and writes to a temporary output file. It
    then returns the contents of the file as a byte array.

    There are separate methods in case the GW sky map is multiordered (we just
    reweight using the external sky map) or flattened (use standard
    ligo.skymap.combine method).

    Parameters
    ----------
    skymapbytes : tuple
        Tuple of gw_skymap and ext_skymap bytes
    gw_moc : bool
        If True, assumes the GW sky map is a multi-ordered format

    Returns
    -------
    combinedskymap : bytes
        Bytes of combined sky map
    """
    gw_skymap_bytes, ext_skymap_bytes = skymapsbytes
    with NamedTemporaryFile(mode='rb', suffix=".fits") as combinedskymap, \
            NamedTemporaryFile(content=gw_skymap_bytes) as gw_skymap_file, \
            NamedTemporaryFile(content=ext_skymap_bytes) as ext_skymap_file, \
            handling_system_exit():

        gw_skymap = fits.read_sky_map(gw_skymap_file.name, moc=True)
        # If GW sky map is multiordered, use reweighting method
        if ext_moc:
            #  Load external sky map
            ext_skymap = fits.read_sky_map(ext_skymap_file.name, moc=True)
            #  Create and write combined sky map
            combined_skymap = combine_skymaps_moc_moc(gw_skymap,
                                                      ext_skymap)
        # If GW sky map is flattened, use older method
        else:
            #  Load external sky map
            ext_skymap, ext_header = fits.read_sky_map(ext_skymap_file.name,
                                                       moc=False, nest=True)
            #  Create and write combined sky map
            combined_skymap = combine_skymaps_moc_flat(gw_skymap, ext_skymap,
                                                       ext_header)
        fits.write_sky_map(combinedskymap.name, combined_skymap, moc=True)
        return combinedskymap.read()


@app.task(shared=False)
def external_trigger_heasarc(external_id):
    """Returns the HEASARC FITS file link.

    Parameters
    ----------
    external_id : str
        GraceDB ID of external event

    Returns
    -------
    heasarc_link : str
        Guessed HEASARC URL link

    """
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


@app.task(autoretry_for=(gracedb.RetryableHTTPError,), retry_backoff=30,
          max_retries=8)
def get_external_skymap(link, search):
    """Download the Fermi sky map FITS file and return the contents as a byte
    array. If GRB, will construct a HEASARC url, while if SubGRB, will use the
    link directly.

    If not available, will try again 10 seconds later, then 20, then 40, etc.
    until up to 15 minutes after initial attempt.

    Parameters
    ----------
    link : str
        HEASARC URL link
    search : str
        Search field of external event

    Returns
    -------
    external_skymap : bytes
        Bytes of external sky map

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
    """Decode and upload 64base encoded sky maps from Kafka alerts.

    Parameters
    ----------
    event : dict
        External event dictionary
    skymap_str : str
        Base 64 encoded sky map string

    """

    graceid = event['graceid']

    # Decode base64 encoded string to bytes string
    skymap_data = b64decode(skymap_str)

    # Determine filename based on whether is multiordered or flattened
    skymap_filename = event['pipeline'].lower() + '_skymap.'
    skymap_filename += ('multiorder.fits' if is_skymap_moc(skymap_data)
                        else 'fits.gz')

    message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=graceid, filename=skymap_filename)

    (
        group(
            gracedb.upload.si(
                skymap_data,
                skymap_filename,
                graceid,
                'Sky map uploaded from {} via a kafka notice'.format(
                    event['pipeline']),
                ['sky_loc']),

            skymaps.plot_allsky.si(skymap_data)
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

    Parameters
    ----------
    event : dict
        External event dictionary
    skymap_link : str
        HEASARC URL link

    """
    graceid = event['graceid']
    search = event['search']

    if search == 'GRB':
        skymap_data = \
            get_external_skymap(external_trigger_heasarc(graceid), search)
    elif search in {'SubGRB', 'SubGRBTargeted', 'FromURL'}:
        skymap_data = get_external_skymap(skymap_link, search)

    skymap_filename_base = \
        ('external_from_url' if search == 'FromURL'
         else FERMI_OFFICIAL_SKYMAP_FILENAME)
    skymap_filename = skymap_filename_base + \
        ('.multiorder.fits' if is_skymap_moc(skymap_data) else '.fits.gz')

    fits_message = \
        ('Downloaded from {}.'.format(skymap_link) if search == 'FromURL'
         else 'Official sky map from Fermi analysis.')
    png_message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=graceid, filename=skymap_filename)

    (
        group(
            gracedb.upload.si(
                skymap_data,
                skymap_filename,
                graceid,
                fits_message,
                ['sky_loc']),

            skymaps.plot_allsky.si(skymap_data)
            |
            gracedb.upload.s(skymap_filename_base + '.png',
                             graceid,
                             png_message,
                             ['sky_loc'])
        )
        |
        gracedb.create_label.si('EXT_SKYMAP_READY', graceid)
    ).delay()


def from_cone(pts, ra, dec, error):
    """
    Based on the given RA, DEC, and error radius of the center points,
    it calculates the gaussian pdf.
    """
    ras, decs = pts.T
    center = SkyCoord(ra * u.deg, dec * u.deg)
    error_radius = error * u.deg

    pts_loc = SkyCoord(np.rad2deg(ras) * u.deg, np.rad2deg(decs) * u.deg)

    distance = pts_loc.separation(center)
    skymap = np.exp(-0.5 * np.square(distance / error_radius).to_value(
        u.dimensionless_unscaled))
    return skymap


def _fisher(distance, error_stat, error_sys):
    """
    Calculates the Fisher distribution from Eq. 1 and Eq. 2 of Connaughton
    et al. 2015 (doi: 10.1088/0067-0049/216/2/32).
    """

    error_tot2 = (
            np.square(np.radians(error_stat)) +
            np.square(np.radians(error_sys))
    )
    kappa = 1 / (0.4356 * error_tot2)
    return kappa / (2 * np.pi * (np.exp(kappa) - np.exp(-kappa))) \
        * np.exp(kappa * np.cos(distance))


def fermi_error_model(pts, ra, dec, error, core, tail, core_weight):
    """
    Calculate the Fermi GBM error model from Connaughton et al. 2015 based
    on the given RA, Dec and error radius of the center point using the model
    parameters of core radii, tail radii, and core proportion (f in the paper).
    """
    ras, decs = pts.T
    center = SkyCoord(ra * u.deg, dec * u.deg)

    pts_loc = SkyCoord(np.rad2deg(ras) * u.deg, np.rad2deg(decs) * u.deg)
    distance = pts_loc.separation(center).rad  # Ensure the output is in radian

    core_component = core_weight * _fisher(distance, error, core)
    tail_component = (1 - core_weight) * _fisher(distance, error, tail)

    return core_component + tail_component


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
        Right ascension in deg
    dec : float
        Declination in deg
    error : float
        Error radius in deg
    pipeline : str
        External trigger pipeline name
    notice_type : int
        GCN notice type integer

    Returns
    -------
    skymap : array
        Sky map array

    """
    # Dictionary definitions for core_weight, core, and tail values
    # for different notice types.
    # Flight notice: Values from first row of Table 7
    # Ground notice: Values from first row of Table 3
    # Final notice: Values from second row of Table 3
    fermi_params = {
        gcn.NoticeType.FERMI_GBM_FLT_POS: {"core_weight": 0.897,
                                           "core_width": 7.52,
                                           "tail_width": 55.6},
        gcn.NoticeType.FERMI_GBM_GND_POS: {"core_weight": 0.804,
                                           "core_width": 3.72,
                                           "tail_width": 13.7},
        gcn.NoticeType.FERMI_GBM_FIN_POS: {"core_weight": 0.900,
                                           "core_width": 3.71,
                                           "tail_width": 14.3},
    }

    # Correct 90% containment to 1-sigma for Swift
    if pipeline == 'Swift':
        error /= np.sqrt(-2 * np.log1p(-.9))
    # Set minimum error radius so function does not return void
    # FIXME: Lower this when fixes are made to ligo.skymap to fix nans when
    #        the error radius is too low
    error = max(error, .08)
    # This function adaptively refines the grid based on the given gaussian
    # pdf to create the multi-ordered skymap.
    if pipeline == 'Fermi' and notice_type is not None:
        # Correct for Fermi systematics based on recommendations from GBM team
        # Convolve with both a narrow core and wide tail Gaussian with error
        # radius determined by the scales respectively, each comprising a
        # fraction determined by the weights respectively.
        if notice_type not in fermi_params:
            raise AssertionError('Provide a supported Fermi notice type')
        core_weight = fermi_params[notice_type]["core_weight"]
        # Note that tail weight = 1 - core_weight
        core_width = fermi_params[notice_type]["core_width"]
        tail_width = fermi_params[notice_type]["tail_width"]

        # Integrate the fermi_error_model using bayestar_adaptive_grid
        skymap = bayestar_adaptive_grid(fermi_error_model, ra, dec, error,
                                        core_width, tail_width, core_weight,
                                        rounds=8)
    else:
        # Use generic cone method for Swift, INTEGRAL, etc.
        skymap = bayestar_adaptive_grid(from_cone, ra, dec, error, rounds=8)

    return skymap


def write_to_fits(skymap, event, notice_type, notice_date):
    """Write external sky map FITS file, populating the
    header with relevant info.

    Parameters
    ----------
    skymap : array
        Sky map array
    event : dict
        Dictionary of external event
    notice_type : int
        GCN notice type integer
    notice_date : str
        External event trigger time in ISO format

    Returns
    -------
    skymap_fits : str
        Bytes string of sky map

    """

    if notice_type is None:
        msgtype = event['pipeline'] + '_LVK_TARGETED_SEARCH'
    else:
        msgtype = NOTICE_TYPE_DICT[str(notice_type)]

    gcn_id = event['extra_attributes']['GRB']['trigger_id']
    with NamedTemporaryFile(suffix='.fits') as f:
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
        Dictionary of external event
    notice_type : int
        GCN notice type integer
    notice_date : str
        External event trigger time in ISO format

    """
    graceid = event['graceid']
    skymap_filename = event['pipeline'].lower() + '_skymap.multiorder.fits'

    ra = event['extra_attributes']['GRB']['ra']
    dec = event['extra_attributes']['GRB']['dec']
    error = event['extra_attributes']['GRB']['error_radius']
    pipeline = event['pipeline']

    if not (ra or dec or error):
        # Don't create sky map if notice only contains zeros, lacking info
        return
    skymap = create_external_skymap(ra, dec, error, pipeline, notice_type)

    skymap_data = write_to_fits(skymap, event, notice_type, notice_date)

    if notice_type is None:
        extra_sentence = ' from {} via our joint targeted search'.format(
            pipeline)
    else:
        msgtype = NOTICE_TYPE_DICT[str(notice_type)]
        extra_sentence = ' from a {} type GCN notice'.format(msgtype)

    message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>{extra_sentence}').format(
            graceid=graceid, filename=skymap_filename,
            extra_sentence=extra_sentence)

    (
        gracedb.upload.si(
            skymap_data,
            skymap_filename,
            graceid,
            'Sky map created from GCN RA, dec, and error{}.'.format(
                extra_sentence),
            ['sky_loc'])
        |
        skymaps.plot_allsky.si(skymap_data)
        |
        gracedb.upload.s(event['pipeline'].lower() + '_skymap.png',
                         graceid,
                         message,
                         ['sky_loc'])
        |
        gracedb.create_label.si('EXT_SKYMAP_READY', graceid)
    ).delay()


@app.task(shared=False)
@closing_figures()
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
        Superevent dictionary
    ext_event : dict
        External event dictionary
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
    fig, _ = plot_bayes_factor(
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
