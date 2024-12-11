"""Annotations for sky maps."""
import os
import tempfile

from astropy import table
from astropy.io import fits
from celery import group
from celery.exceptions import Ignore
from ligo.skymap.tool import (ligo_skymap_flatten, ligo_skymap_from_samples,
                              ligo_skymap_plot, ligo_skymap_plot_coherence,
                              ligo_skymap_plot_volume, ligo_skymap_unflatten)
from matplotlib import pyplot as plt

from .. import app
from ..jinja import env
from ..util.cmdline import handling_system_exit
from ..util.matplotlib import closing_figures
from ..util.tempfile import NamedTemporaryFile
from . import external_skymaps, gracedb, igwn_alert


@app.task(ignore_result=True, shared=False)
def annotate_fits_tuple(filecontents_versioned_filename, graceid, tags):
    filecontents, versioned_filename = filecontents_versioned_filename
    annotate_fits(filecontents, versioned_filename, graceid, tags)


@app.task(ignore_result=True, shared=False)
def annotate_fits(filecontents, versioned_filename, graceid, tags):
    """Perform annotations on a sky map.

    This function downloads a FITS file and then generates and uploads all
    derived images as well as an HTML dump of the FITS header.
    """
    multiorder_extension = '.multiorder.fits'
    flat_extension = '.fits'

    if multiorder_extension in versioned_filename:
        extension = multiorder_extension
        multiorder = True
    else:
        extension = flat_extension
        multiorder = False

    filebase, _, _ = versioned_filename.partition(extension)

    header_msg = (
        'FITS headers for <a href="/api/superevents/{graceid}/files/'
        '{versioned_filename}">{versioned_filename}</a>').format(
            graceid=graceid, versioned_filename=versioned_filename)
    allsky_msg = (
        'Mollweide projection of <a href="/api/superevents/{graceid}/files/'
        '{versioned_filename}">{versioned_filename}</a>').format(
            graceid=graceid, versioned_filename=versioned_filename)
    volume_msg = (
        'Volume rendering of <a href="/api/superevents/{graceid}/files/'
        '{versioned_filename}">{versioned_filename}</a>').format(
            graceid=graceid, versioned_filename=versioned_filename)
    flatten_msg = (
        'Flat-resolution FITS file created from '
        '<a href="/api/superevents/{graceid}/files/'
        '{versioned_filename}">{versioned_filename}</a>').format(
            graceid=graceid, versioned_filename=versioned_filename)

    group(
        fits_header.s(versioned_filename)
        |
        gracedb.upload.s(
            filebase + '.html', graceid, header_msg, tags),

        plot_allsky.s()
        |
        gracedb.upload.s(
            filebase + '.png', graceid, allsky_msg, tags),

        annotate_fits_volume.s(
            filebase + '.volume.png', graceid, volume_msg, tags),

        *(
            [
                flatten.s(f'{filebase}.fits.gz')
                |
                gracedb.upload.s(
                    f'{filebase}.fits.gz', graceid, flatten_msg, tags)
            ] if multiorder else []
        )
    ).delay(filecontents)


def is_3d_fits_file(filecontents):
    """Determine if a FITS file has distance information."""
    with NamedTemporaryFile(content=filecontents) as fitsfile:
        return 'DISTNORM' in table.Table.read(fitsfile.name).colnames


@app.task(ignore_result=True, shared=False)
def annotate_fits_volume(filecontents, *args):
    """Perform annotations that are specific to 3D sky maps."""
    if is_3d_fits_file(filecontents):
        (
            plot_volume.s(filecontents)
            |
            gracedb.upload.s(*args)
        ).apply_async()


@app.task(shared=False)
def fits_header(filecontents, filename):
    """Dump FITS header to HTML."""
    template = env.get_template('fits_header.jinja2')
    with NamedTemporaryFile(content=filecontents) as fitsfile, \
            fits.open(fitsfile.name) as hdus:
        return template.render(filename=filename, hdus=hdus)


@app.task(shared=False)
@closing_figures()
def plot_allsky(filecontents):
    """Plot a Mollweide projection of a sky map using the command-line tool
    :doc:`ligo-skymap-plot <ligo.skymap:tool/ligo_skymap_plot>`.
    """
    # Explicitly use a non-interactive Matplotlib backend.
    plt.switch_backend('agg')

    with NamedTemporaryFile(mode='rb', suffix='.png') as pngfile, \
            NamedTemporaryFile(content=filecontents) as fitsfile, \
            handling_system_exit():
        ligo_skymap_plot.main([fitsfile.name, '-o', pngfile.name,
                               '--annotate', '--contour', '50', '90'])
        return pngfile.read()


@app.task(priority=1, queue='openmp', shared=False)
@closing_figures()
def plot_volume(filecontents):
    """Plot a 3D volume rendering of a sky map using the command-line tool
    :doc:`ligo-skymap-plot-volume <ligo.skymap:tool/ligo_skymap_plot_volume>`.
    """
    # Explicitly use a non-interactive Matplotlib backend.
    plt.switch_backend('agg')

    with NamedTemporaryFile(mode='rb', suffix='.png') as pngfile, \
            NamedTemporaryFile(content=filecontents) as fitsfile, \
            handling_system_exit():
        ligo_skymap_plot_volume.main([fitsfile.name, '-o',
                                      pngfile.name, '--annotate'])
        return pngfile.read()


@app.task(shared=False, queue='highmem')
def flatten(filecontents, filename):
    """Convert a HEALPix FITS file from multi-resolution UNIQ indexing to the
    more common IMPLICIT indexing using the command-line tool
    :doc:`ligo-skymap-flatten <ligo.skymap:tool/ligo_skymap_flatten>`.
    """
    with NamedTemporaryFile(content=filecontents) as infile, \
            tempfile.TemporaryDirectory() as tmpdir, \
            handling_system_exit():
        outfilename = os.path.join(tmpdir, filename)
        ligo_skymap_flatten.main([infile.name, outfilename])
        return open(outfilename, 'rb').read()


@app.task(shared=False, queue='highmem')
def unflatten(filecontents, filename):
    """Convert a HEALPix FITS file to multi-resolution UNIQ indexing from
    the more common IMPLICIT indexing using the command-line tool
    :doc:`ligo-skymap-unflatten <ligo.skymap:tool/ligo_skymap_unflatten>`.
    """
    with NamedTemporaryFile(content=filecontents) as infile, \
            tempfile.TemporaryDirectory() as tmpdir, \
            handling_system_exit():
        outfilename = os.path.join(tmpdir, filename)
        ligo_skymap_unflatten.main([infile.name, outfilename])
        return open(outfilename, 'rb').read()


@app.task(shared=False, queue='multiprocessing')
def skymap_from_samples(samplefilecontents, superevent_id, instruments):
    """Generate multi-resolution FITS file from samples."""
    with NamedTemporaryFile(content=samplefilecontents) as samplefile, \
            tempfile.TemporaryDirectory() as tmpdir, \
            handling_system_exit():
        ligo_skymap_from_samples.main([
            '-j', '--seed', '150914', '--maxpts', '5000', '--objid',
            superevent_id, '--instruments', *instruments, '-o', tmpdir,
            samplefile.name])
        with open(os.path.join(tmpdir, 'skymap.fits'), 'rb') as f:
            return f.read()


@app.task(shared=False)
@closing_figures()
def plot_coherence(filecontents):
    """IGWN alert handler to plot the coherence Bayes factor.

    Parameters
    ----------
    contents : str, bytes
        The contents of the FITS file.

    Returns
    -------
    png : bytes
        The contents of a PNG file.

    Notes
    -----
    Under the hood, this just calls :meth:`plot_bayes_factor`.

    """
    # Explicitly use a non-interactive Matplotlib backend.
    plt.switch_backend('agg')

    with NamedTemporaryFile(mode='rb', suffix='.png') as pngfile, \
            NamedTemporaryFile(content=filecontents) as fitsfile:
        header = fits.getheader(fitsfile, 1)
        try:
            header['LOGBCI']
        except KeyError:
            raise Ignore('FITS file does not have a LOGBCI field')
        ligo_skymap_plot_coherence.main([fitsfile.name, '-o', pngfile.name])
        return pngfile.read()


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    shared=False)
def handle_plot_coherence(alert):
    """IGWN alert handler to plot and upload a visualization of the coherence
    Bayes factor.

    Notes
    -----
    Under the hood, this just calls :meth:`plot_coherence`.

    """
    if alert['alert_type'] != 'log':
        return  # not for us
    if not alert['data']['filename'].endswith('.fits') or \
            (alert['data']['filename'] ==
             external_skymaps.COMBINED_SKYMAP_FILENAME_MULTIORDER):
        return  # not for us

    graceid = alert['uid']
    f = alert['data']['filename']
    v = alert['data']['file_version']
    fv = '{},{}'.format(f, v)

    (
        gracedb.download.s(fv, graceid)
        |
        plot_coherence.s()
        |
        gracedb.upload.s(
            f.replace('.fits', '.coherence.png'), graceid,
            message=(
                f'Bayes factor for coherence vs. incoherence of '
                f'<a href="/api/superevents/{graceid}/files/{fv}">'
                f'{fv}</a>'),
            tags=['sky_loc']
        )
    ).delay()
