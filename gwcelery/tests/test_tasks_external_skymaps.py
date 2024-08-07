from unittest.mock import patch
from urllib.error import HTTPError

import astropy_healpix as ah
import numpy as np
import pytest
from astropy.table import Table

from ..tasks import external_skymaps, gcn, gracedb
from ..util import read_binary, read_json
from . import data
from .test_tasks_skymaps import toy_3d_fits_filecontents  # noqa: F401
from .test_tasks_skymaps import toy_fits_filecontents  # noqa: F401

true_heasarc_link = ('http://heasarc.gsfc.nasa.gov/FTP/fermi/data/gbm/'
                     + 'triggers/2017/bn170817529/current/')
true_skymap_link = true_heasarc_link + 'glg_healpix_all_bn170817529_v00.fit'


def mock_get_event(exttrig):
    return {'search': 'GRB'}


def mock_get_superevent(graceid):
    return read_json(data, 'mock_superevent_object.json')


def mock_get_log(graceid):
    if 'S' in graceid:
        logs = read_json(data, 'gracedb_setrigger_log.json')
        if graceid == 'S23456':
            logs[0]['filename'] = 'bayestar.fits.gz'
        return logs
    elif graceid == 'E12345':
        return read_json(data, 'gracedb_externaltrigger_log.json')
    else:
        return {}


@pytest.fixture  # noqa: F811
def mock_download(monkeypatch, toy_3d_fits_filecontents):  # noqa: F811

    def download(filename, graceid):
        """Mocks GraceDB download functionality"""
        if graceid == 'S12345' and filename == 'bayestar.fits.gz,0':
            return toy_3d_fits_filecontents
        elif (graceid == 'E12345' and
              filename == ('nasa.gsfc.gcn_Fermi%23GBM_Gnd_Pos_2017-08-17'
                           + 'T12%3A41%3A06.47_524666471_57-431.xml')):
            return read_binary(data, 'externaltrigger_original_data.xml')
        else:
            raise ValueError

    monkeypatch.setattr('gwcelery.tasks.gracedb.download.run', download)


def mock_get_file_contents(monkeypatch, toy_fits_filecontents):  # noqa: F811
    """Mocks astropy get_file_contents functionality"""
    def get_file_contents(heasarc_link):
        assert heasarc_link == true_heasarc_link
        return toy_fits_filecontents

    monkeypatch.setattr(
        'astropy.utils.data.get_file_contents', get_file_contents)


def get_gw_moc_skymap():
    array = [np.arange(12, dtype=np.float64)] * 5
    # Normalize
    array[0] = array[0] / sum(array[0]) / (4 * np.pi) * len(array[0])
    #  Modify UNIQ table to be allowable values
    array[4] = np.asarray(array[4] + 4, dtype=int)
    table = Table(
        array,
        names=['PROBDENSITY', 'DISTMU', 'DISTSIGMA', 'DISTNORM', 'UNIQ'])
    table.meta['comment'] = 'This is a comment.'
    table.meta['HISTORY'] = \
        ['This is a history line. <This should be escaped.>']
    table.meta['OBJECT'] = 'T12345'
    table.meta['LOGBCI'] = 3.5
    table.meta['ORDERING'] = 'NESTED'
    table.meta['instruments'] = {'L1', 'H1', 'V1'}
    return table


@patch('gwcelery.tasks.skymaps.plot_allsky.run')
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.external_skymaps.combine_skymaps.run')
@patch('gwcelery.tasks.gracedb.download')
@patch('gwcelery.tasks.external_skymaps.get_skymap_filename',
       return_value='fermi_skymap.fits.gz,0')
def test_create_combined_skymap(mock_get_skymap_filename,
                                mock_download,
                                mock_combine_skymaps, mock_upload,
                                mock_plot_allsky):
    """Test creating combined LVC and Fermi skymap"""
    # Run function under test
    external_skymaps.create_combined_skymap('S12345', 'E12345')
    mock_combine_skymaps.assert_called_once()
    mock_upload.assert_called()


def _mock_read_sky_map(filename, moc=True, nest=True):
    if moc:
        return get_gw_moc_skymap()
    else:
        ext_sky = np.full(12, 1 / 12)
        ext_header = {'instruments': set({'Fermi'}), 'nest': nest}
        return ext_sky, ext_header


@pytest.mark.parametrize('ext_moc',
                         [True, False])
@patch('gwcelery.tasks.external_skymaps.combine_skymaps_moc_moc')
@patch('gwcelery.tasks.external_skymaps.combine_skymaps_moc_flat')
@patch('ligo.skymap.io.fits.read_sky_map', side_effect=_mock_read_sky_map)
@patch('ligo.skymap.io.fits.write_sky_map')
def test_combine_skymaps(mock_write_sky_map,
                         mock_read_sky_map,
                         mock_skymap_combine_moc_flat,
                         mock_skymap_combine_moc_moc,
                         ext_moc):
    """Test using our internal MOC-flat sky map combination gives back the
    input using a uniform sky map, ensuring the test is giving a sane result
    and is at least running to completion.
    """
    external_skymaps.combine_skymaps((b'', b''), ext_moc=ext_moc)
    mock_read_sky_map.assert_called()
    if ext_moc:
        mock_skymap_combine_moc_moc.assert_called_once()
    else:
        mock_skymap_combine_moc_flat.assert_called_once()
    mock_write_sky_map.assert_called_once()


@pytest.mark.parametrize('missing_header_values,instrument',
                         [[False, 'Fermi'],
                          [True, 'Fermi'],
                          [False, None],
                          [True, None]])
def test_create_combined_skymap_moc_flat(missing_header_values, instrument):
    """Test using our internal MOC-flat sky map combination gives back the
    input using a uniform sky map, ensuring the test is giving a sane result
    and is at least running to completion.
    """
    # Run function under test
    gw_sky = get_gw_moc_skymap()
    if missing_header_values:
        del gw_sky['DISTMU']
        del gw_sky['DISTSIGMA']
        gw_sky.meta.pop('instruments')
        gw_sky.meta.pop('HISTORY')
    # Combining the sky map overwrites original, keep copy for comparison
    gw_sky_original = gw_sky.copy()
    ext_sky = np.full(12, 1 / 12)
    if instrument:
        ext_header = {'instruments': set({instrument}), 'nest': True}
    else:
        ext_header = {'nest': True}
    combined_sky = external_skymaps.combine_skymaps_moc_flat(gw_sky, ext_sky,
                                                             ext_header)
    assert all(combined_sky['PROBDENSITY'] == gw_sky_original['PROBDENSITY'])
    if missing_header_values:
        assert 'instruments' not in combined_sky.meta
    else:
        assert ('Fermi' in combined_sky.meta['instruments'] if instrument else
                'external instrument' in combined_sky.meta['instruments'])


def test_create_combined_skymap_moc_flat_negative_values():
    """Test using our internal MOC-flat sky map combination using an external
    skymap with a negative value. This value is removed and the sky map
    normalized without this.
    """
    # Run function under test
    gw_sky = get_gw_moc_skymap()
    # Combining the sky map overwrites original, keep copy for comparison
    gw_sky_original = gw_sky.copy()
    ext_sky = np.full(12, 1 / 12)
    # Turn one non-zero pixel negative to check we catch this later
    ext_sky[-1] = -1 / 12
    ext_header = {'instruments': set({'Fermi'}), 'nest': True}
    combined_sky = external_skymaps.combine_skymaps_moc_flat(gw_sky, ext_sky,
                                                             ext_header)
    # Assert missing pixel has no probability
    assert combined_sky['PROBDENSITY'][-1] == 0.
    # Since one pixel is removed from external sky map, correct normalization
    # of other pixels: sum(0 .. 11) / sum(0 .. 10) = 66/55 = 6/5
    assert all(np.isclose(combined_sky['PROBDENSITY'][:-1],
                          6 / 5 * gw_sky_original['PROBDENSITY'][:-1]))
    assert ('Fermi' in combined_sky.meta['instruments'])


@pytest.mark.parametrize('missing_header_values,instrument',
                         [[False, 'Fermi'],
                          [True, 'Fermi'],
                          [False, None],
                          [True, None]])
def test_create_combined_skymap_moc_moc(monkeypatch,
                                        missing_header_values, instrument):
    """Test using our internal MOC-MOC sky map combination gives back the
    input using a uniform sky map, ensuring the test is giving a sane result
    and is at least running to completion.
    """
    # Run function under test
    gw_sky = get_gw_moc_skymap()
    if missing_header_values:
        del gw_sky['DISTMU']
        del gw_sky['DISTSIGMA']
        gw_sky.meta.pop('instruments')
        gw_sky.meta.pop('HISTORY')
    if instrument:
        ext_header = {'instruments': set({instrument}), 'nest': True}
    else:
        ext_header = {'nest': True}
    # Create new uniform sky map so nothing is changed
    ext_sky = Table([np.arange(4, 16, dtype=int), np.full(12, 1 / 12)],
                    names=['UNIQ', 'PROBDENSITY'],
                    meta=ext_header)
    # Load sky maps into temporary files to be read by combine_skymaps_moc_moc
    combined_sky = external_skymaps.combine_skymaps_moc_moc(gw_sky, ext_sky)
    assert all(combined_sky['PROBDENSITY'].value ==
               gw_sky['PROBDENSITY'].value)
    if missing_header_values:
        assert 'instruments' not in combined_sky.meta
    else:
        assert ('Fermi' in combined_sky.meta['instruments'] if instrument else
                'external instrument' in combined_sky.meta['instruments'])


@pytest.mark.parametrize('graceid',
                         ['S12345', 'S23456', 'E12345'])
@patch('gwcelery.tasks.gracedb.get_log', side_effect=mock_get_log)
def test_get_skymap_filename(mock_get_logs, graceid):
    """Test getting the LVC skymap FITS filename"""
    filename = external_skymaps.get_skymap_filename(graceid,
                                                    is_gw='S' in graceid)
    if graceid == 'S12345':
        assert filename == 'bayestar.multiorder.fits,0'
    if graceid == 'S23456':
        assert filename == 'bayestar.fits.gz,0'
    elif 'E' in graceid:
        assert filename == 'fermi_skymap.fits.gz,0'


@patch('gwcelery.tasks.gracedb.get_log', side_effect=mock_get_log)
def test_get_skymap_filename_404(mock_get_logs):
    with pytest.raises(ValueError):
        external_skymaps.get_skymap_filename(
            'E23456', is_gw=False)


@patch('gwcelery.tasks.gracedb.get_log', mock_get_log)
def test_external_trigger_heasarc(mock_download):
    """Test retrieving HEASARC FITS file link from GCN"""
    heasarc_link = external_skymaps.external_trigger_heasarc('E12345')
    assert heasarc_link == true_heasarc_link


@patch('gwcelery.tasks.gracedb.get_log', mock_get_log)
def test_external_trigger_heasarc_404(mock_download):
    """Test retrieving HEASARC FITS file link from GCN"""
    with pytest.raises(ValueError):
        external_skymaps.external_trigger_heasarc('E23456')


@pytest.mark.parametrize('search', ['GRB', 'SubGRB', 'FromURL'])
@patch('urllib.request.urlopen')
def test_get_external_skymap(mock_urlopen, search):
    """Assert that the correct call to astropy.get_file_contents is used"""
    external_skymaps.get_external_skymap(true_heasarc_link, search)
    mock_urlopen.assert_called_once()


class File(object):
    def close(self):
        pass


class HTTPError_403(object):
    def __init__(self, *args, **kwargs):
        pass

    def read(self):
        raise HTTPError('', 403, '', '', File)


class HTTPError_404(object):
    def __init__(self, *args, **kwargs):
        pass

    def read(self):
        raise HTTPError('', 404, '', '', File)


@pytest.mark.parametrize('search', ['GRB', 'SubGRB', 'FromURL'])
@patch('urllib.request.urlopen', HTTPError_403)
def test_get_external_skymap_403(search):
    """Assert that when urllib.request raises 403 error, we raise
    that type of error."""
    with pytest.raises(HTTPError):
        external_skymaps.get_external_skymap(true_heasarc_link, search)


@pytest.mark.parametrize('search', ['GRB', 'SubGRB', 'FromURL'])
@patch('urllib.request.urlopen', HTTPError_404)
def test_get_external_skymap_404(search):
    """Assert that when urllib.request raises 404 error, we raise
    gracedb.RetryableHTTPError."""
    with pytest.raises(gracedb.RetryableHTTPError):
        external_skymaps.get_external_skymap(true_heasarc_link, search)


@pytest.mark.parametrize('search', ['GRB', 'SubGRB', 'FromURL'])
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
@patch('gwcelery.tasks.external_skymaps.get_external_skymap',
       return_value=read_binary(data, 'swift_skymap.multiorder.fits'))
@patch('gwcelery.tasks.external_skymaps.external_trigger_heasarc',
       return_value='https:/foo.bar')
@patch('gwcelery.tasks.external_skymaps.is_skymap_moc', return_value=True)
def test_get_upload_external_skymap(mock_is_skymap_moc,
                                    mock_external_trigger_heasarc,
                                    mock_get_external_skymap,
                                    mock_plot_allsky,
                                    mock_upload,
                                    search):
    """Test that an external sky map is grabbed and uploaded."""
    event = {'graceid': 'E12345', 'search': search}
    filename_base = \
        ('external_from_url' if search == 'FromURL'
         else external_skymaps.FERMI_OFFICIAL_SKYMAP_FILENAME)
    filename = filename_base + '.multiorder.fits'
    external_skymaps.get_upload_external_skymap(event)
    if search == 'GRB':
        mock_external_trigger_heasarc.assert_called_once()
    mock_get_external_skymap.assert_called_once()
    mock_upload.assert_called()
    assert filename == mock_upload.call_args_list[0][0][1]


@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
@patch('gwcelery.tasks.external_skymaps.get_external_skymap',
       return_value=read_binary(data, 'fermi_skymap.fits.gz'))
@patch('gwcelery.tasks.external_skymaps.is_skymap_moc', return_value=False)
def test_get_upload_external_skymap_subgrb(mock_is_skymap_moc,
                                           mock_get_external_skymap,
                                           mock_plot_allsky,
                                           mock_upload):
    """Test that an external sky map is grabbed and uploaded."""
    event = {'graceid': 'E12345', 'search': 'SubGRB'}
    external_skymaps.get_upload_external_skymap(
        event,
        ('https://gcn.gsfc.nasa.gov/notices_gbm_sub/' +
         'gbm_subthresh_604671025.728000_healpix.fits'))
    mock_get_external_skymap.assert_called_once()
    assert 'glg_healpix_all_bn_v00.fits.gz' == \
        mock_upload.call_args_list[0][0][1]


@pytest.mark.parametrize('is_moc', [True, False])
def test_is_moc_skymap(is_moc):
    # If moc, grab multiordered sky map
    if is_moc:
        skymapbytes = read_binary(data, 'swift_skymap.multiorder.fits')
    # If not, grab flattened sky map
    else:
        skymapbytes = read_binary(data, 'fermi_skymap.fits.gz')
    result = external_skymaps.is_skymap_moc(skymapbytes)
    assert result == is_moc


@pytest.mark.parametrize('ra,dec,error',
                         [[0, 90, 0],
                          [270, -90, .1]])
def test_create_swift_skymap(ra, dec, error):
    """Test created single pixel sky maps for Swift localization."""
    skymap = external_skymaps.create_external_skymap(ra, dec, error, 'Swift')

    level, ipix = ah.uniq_to_level_ipix(skymap['UNIQ'])
    nsides = ah.level_to_nside(level)
    areas = ah.nside_to_pixel_area(nsides)
    # Ensure new map is generated and normalized
    assert np.sum((skymap['PROBDENSITY'] * areas).value) == \
        pytest.approx(1.0, 1.e-9)


@pytest.mark.parametrize('error,notice_type',
                         [[20., gcn.NoticeType.FERMI_GBM_FLT_POS],
                          [10., gcn.NoticeType.FERMI_GBM_GND_POS],
                          [3.0, gcn.NoticeType.FERMI_GBM_FIN_POS]])
def test_create_fermi_skymap(error, notice_type):
    """Test designed to validate the creation of sky maps for Fermi
    localizations."""
    skymap = external_skymaps.create_external_skymap(
                 0, 90, error, 'Fermi', notice_type=notice_type)

    level, ipix = ah.uniq_to_level_ipix(skymap['UNIQ'])
    nsides = ah.level_to_nside(level)
    areas = ah.nside_to_pixel_area(nsides)
    # Ensure new map is generated and normalized
    assert np.sum((skymap['PROBDENSITY'] * areas).value) == \
        pytest.approx(1.0, 1.e-9)


def test_create_fermi_skymap_wrong_notice_type():
    with pytest.raises(AssertionError):
        external_skymaps.create_external_skymap(
            0., 0., 10., 'Fermi', notice_type=gcn.NoticeType.FERMI_GBM_ALERT)


@pytest.mark.parametrize('notice_type', ['61', None])
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
def test_create_upload_swift_skymap(mock_plot_allsky,
                                    mock_upload, notice_type):
    """Test the creation and upload of sky maps for Swift localization."""
    event = {'graceid': 'E1234',
             'pipeline': 'Swift',
             'gpstime': 1259790538.77,
             'extra_attributes': {
                 'GRB': {
                     'trigger_id': 1234567,
                     'ra': 1.,
                     'dec': 1.,
                     'error_radius': 0}},
             'links': {
                 'self': 'https://gracedb.ligo.org/api/events/E356793'}}
    external_skymaps.create_upload_external_skymap(event, notice_type,
                                                   '2020-01-09T01:47:09')
    mock_upload.assert_called()
    mock_plot_allsky.assert_called_once()


@patch('gwcelery.tasks.gracedb.upload.run')
def test_create_upload_skymap_filter(mock_upload):
    """Test that empty notices don't create sky maps."""
    event = {'graceid': 'E1234',
             'pipeline': 'Swift',
             'gpstime': 1259790538.77,
             'extra_attributes': {
                 'GRB': {
                     'trigger_id': 1234567,
                     'ra': 0.,
                     'dec': 0.,
                     'error_radius': 0.}},
             'links': {
                 'self': 'https://gracedb.ligo.org/api/events/E356793'}}
    external_skymaps.create_upload_external_skymap(event, '111',
                                                   '2020-01-09T01:47:09')
    mock_upload.assert_not_called()


@pytest.mark.parametrize(
    'em_type,graceid,labels,expected_result',
    [[None, 'E1', [], True],
     ['E1', 'E1', [], True],
     ['E1', 'E2', [], True],
     ['E1', 'E1', ['RAVEN_ALERT'], True],
     ['E1', 'E2', ['RAVEN_ALERT'], False],
     [None, 'E3', [], False]]
)
@patch('gwcelery.tasks.gracedb.upload.run')
def test_plot_overlap_integral(mock_upload,
                               em_type, graceid, labels, expected_result):

    coinc_far_dict = {'skymap_overlap': 1e2} if graceid != 'E3' else {}
    superevent = {'superevent_id': 'S1', 'em_type': em_type, 'labels': labels}
    ext_event = {'graceid': graceid}
    external_skymaps.plot_overlap_integral(coinc_far_dict, superevent,
                                           ext_event)
    if expected_result:
        mock_upload.assert_called_once()
    else:
        mock_upload.assert_not_called()


@pytest.mark.parametrize('kafka_packet', ['kafka_alert_fermi.json',
                                          'kafka_alert_swift_wskymap.json'])
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
@patch('gwcelery.tasks.gracedb.create_label.run')
def test_read_upload_skymap_from_base64(mock_create_label, mock_plot_allsky,
                                        mock_gracedb_upload, kafka_packet):
    pipeline = ('Fermi' if 'fermi' in kafka_packet else 'Swift')
    skymapb64 = read_json(
                    data, kafka_packet
                )['healpix_file']
    event = {'graceid': 'E1234',
             'pipeline': pipeline,
             'extra_attributes': {
                 'GRB': {
                     'ra': 14.5,
                     'dec': -40.1}}}
    external_skymaps.read_upload_skymap_from_base64(event, skymapb64)

    mock_gracedb_upload.assert_called()
    mock_plot_allsky.assert_called_once()
    mock_create_label.assert_called_with('EXT_SKYMAP_READY', event['graceid'])

    filename = '{}_skymap.multiorder.fits'.format(pipeline.lower())
    assert filename == mock_gracedb_upload.call_args_list[0][0][1]
