from importlib import resources
from unittest.mock import patch

from astropy.table import Table
import numpy as np
import pytest
from urllib.error import HTTPError

from . import data
from ..util import read_json
from .test_tasks_skymaps import toy_fits_filecontents  # noqa: F401
from .test_tasks_skymaps import toy_3d_fits_filecontents  # noqa: F401
from ..tasks import external_skymaps
from ..tasks import gracedb


true_heasarc_link = ('http://heasarc.gsfc.nasa.gov/FTP/fermi/data/gbm/'
                     + 'triggers/2017/bn170817529/current/')
true_skymap_link = true_heasarc_link + 'glg_healpix_all_bn170817529_v00.fit'


def mock_get_event(exttrig):
    return {'search': 'GRB'}


def mock_get_superevent(graceid):
    return read_json(data, 'mock_superevent_object.json')


def mock_get_log(graceid):
    if graceid == 'S12345':
        return read_json(data, 'gracedb_setrigger_log.json')
    elif graceid == 'E12345':
        return read_json(data, 'gracedb_externaltrigger_log.json')
    else:
        raise ValueError


@pytest.fixture  # noqa: F811
def mock_download(monkeypatch, toy_3d_fits_filecontents):  # noqa: F811

    def download(filename, graceid):
        """Mocks GraceDB download functionality"""
        if graceid == 'S12345' and filename == 'bayestar.fits.gz,0':
            return toy_3d_fits_filecontents
        elif (graceid == 'E12345' and
              filename == ('nasa.gsfc.gcn_Fermi%23GBM_Gnd_Pos_2017-08-17'
                           + 'T12%3A41%3A06.47_524666471_57-431.xml')):
            return resources.read_binary(
                data, 'externaltrigger_original_data.xml')
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
    #  Modify UNIQ table to be allowable values
    array[4] = array[4] + 4
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


def _mock_read_sky_map(filename, moc=True):
    if moc:
        return get_gw_moc_skymap()
    else:
        ext_sky = np.full(12, 1 / 12)
        ext_header = {'instruments': set({'Fermi'}), 'nest': True}
        return ext_sky, ext_header


@pytest.mark.parametrize('gw_moc',
                         [True, False])
@patch('ligo.skymap.tool.ligo_skymap_combine.main')
@patch('gwcelery.tasks.external_skymaps.combine_skymaps_moc_flat')
@patch('ligo.skymap.io.fits.read_sky_map', side_effect=_mock_read_sky_map)
@patch('ligo.skymap.io.fits.write_sky_map')
def test_combine_skymaps(mock_write_sky_map,
                         mock_read_sky_map,
                         mock_skymap_combine_moc_flat,
                         mock_skymap_combine_flat_flat,
                         gw_moc):
    """Test using our internal MOC-flat sky map combination gives back the
    input using a uniform sky map, ensuring the test is giving a sane result
    and is at least running to completion.
    """
    external_skymaps.combine_skymaps((b'', b''), gw_moc=gw_moc)
    if gw_moc:
        mock_read_sky_map.assert_called()
        mock_skymap_combine_moc_flat.assert_called_once()
        mock_write_sky_map.assert_called_once()
    else:
        mock_skymap_combine_flat_flat.assert_called()


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
    ext_sky = np.full(12, 1 / 12)
    if instrument:
        ext_header = {'instruments': set({instrument}), 'nest': True}
    else:
        ext_header = {'nest': True}
    combined_sky = external_skymaps.combine_skymaps_moc_flat(gw_sky, ext_sky,
                                                             ext_header)
    assert all(combined_sky['PROBDENSITY'] == gw_sky['PROBDENSITY'])
    if missing_header_values:
        assert 'instruments' not in combined_sky.meta
    else:
        assert ('Fermi' in combined_sky.meta['instruments'] if instrument else
                'external instrument' in combined_sky.meta['instruments'])


@pytest.mark.parametrize('graceid',
                         ['S12345', 'E12345'])
@patch('gwcelery.tasks.gracedb.get_log', side_effect=mock_get_log)
def test_get_skymap_filename(mock_get_logs, graceid):
    """Test getting the LVC skymap fits filename"""
    filename = external_skymaps.get_skymap_filename(graceid,
                                                    is_gw='S' in graceid)
    if 'S' in graceid:
        assert filename == 'bayestar.multiorder.fits,0'
    elif 'E' in graceid:
        assert filename == 'fermi_skymap.fits.gz,0'


@patch('gwcelery.tasks.gracedb.get_event', mock_get_event)
@patch('gwcelery.tasks.gracedb.get_superevent',
       return_value={'em_events': ['E12345']})
def test_external_trigger(mock_get_superevent, mock_download):
    """Test getting related em event for superevent"""
    assert external_skymaps.external_trigger('S12345') == 'E12345'


@patch('gwcelery.tasks.gracedb.get_log', mock_get_log)
def test_external_trigger_heasarc(mock_download):
    """Test retrieving HEASARC fits file link from GCN"""
    heasarc_link = external_skymaps.external_trigger_heasarc('E12345')
    assert heasarc_link == true_heasarc_link


@pytest.mark.parametrize('search', ['GRB', 'SubGRB', 'FromURL'])
@patch('urllib.request.urlopen')
def test_get_external_skymap(mock_urlopen, search):
    """Assert that the correct call to astropy.get_file_contents is used"""
    external_skymaps.get_external_skymap(true_heasarc_link, search)
    mock_urlopen.assert_called_once()


class File(object):
    def close(self):
        pass


class HTTPError_404(object):
    def __init__(self, *args, **kwargs):
        pass

    def read(self):
        raise HTTPError('', 404, '', '', File)


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
@patch('gwcelery.tasks.external_skymaps.get_external_skymap.run')
@patch('gwcelery.tasks.external_skymaps.external_trigger_heasarc.run')
def test_get_upload_external_skymap(mock_external_trigger_heasarc,
                                    mock_get_external_skymap,
                                    mock_plot_allsky,
                                    mock_upload,
                                    search):
    """Test that an external sky map is grabbed and uploaded."""
    event = {'graceid': 'E12345', 'search': search}
    external_skymaps.get_upload_external_skymap(event)
    if search == 'GRB':
        mock_external_trigger_heasarc.assert_called_once()
    mock_get_external_skymap.assert_called_once()
    mock_upload.assert_called()


@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
@patch('gwcelery.tasks.external_skymaps.get_external_skymap.run')
def test_get_upload_external_skymap_subgrb(mock_get_external_skymap,
                                           mock_plot_allsky,
                                           mock_upload):
    """Test that an external sky map is grabbed and uploaded."""
    event = {'graceid': 'E12345', 'search': 'SubGRB'}
    external_skymaps.get_upload_external_skymap(
        event,
        ('https://gcn.gsfc.nasa.gov/notices_gbm_sub/' +
         'gbm_subthresh_604671025.728000_healpix.fits'))
    mock_get_external_skymap.assert_called_once()
    mock_upload.assert_called()


@pytest.mark.parametrize('ra,dec,error,pix',
                         [[0, 90, 0, 0],
                          [270, -90, .01, -1]])
def test_create_swift_skymap(ra, dec, error, pix):
    """Test created single pixel sky maps for Swift localization."""
    skymap = external_skymaps.create_external_skymap(ra, dec, error, 'Swift')
    assert skymap[pix] == 1


def test_create_fermi_skymap():
    """Test created single pixel sky maps for Swift localization."""
    ra, dec, error = 0, 90, 10
    assert (np.sum(external_skymaps.create_external_skymap(
               ra, dec, error, 'Fermi')) ==
           pytest.approx(1.0, 1.e-9))


@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
def test_create_upload_swift_skymap(mock_plot_allsky,
                                    mock_upload):
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
    external_skymaps.create_upload_external_skymap(event, '111',
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


@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
@patch('gwcelery.tasks.gracedb.create_label.run')
def test_read_upload_skymap_from_base64(mock_create_label, mock_plot_allsky,
                                        mock_gracedb_upload):
    skymapb64 = read_json(
                    data, 'kafka_alert_fermi.json'
                )['healpix_file']
    event = {'graceid': 'E1234',
             'pipeline': 'Fermi',
             'extra_attributes': {
                 'GRB': {
                     'ra': 14.5,
                     'dec': -40.1}}}
    external_skymaps.read_upload_skymap_from_base64(event, skymapb64)

    mock_gracedb_upload.assert_called()
    mock_plot_allsky.assert_called_once()
    mock_create_label.assert_called_with('EXT_SKYMAP_READY', event['graceid'])
