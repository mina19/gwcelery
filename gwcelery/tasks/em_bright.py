"""Qualitative source properties for CBC events."""
import io
import json

from celery.utils.log import get_task_logger
from matplotlib import pyplot as plt

from .. import app
from ..util import NamedTemporaryFile, closing_figures
from . import gracedb, igwn_alert
from .p_astro import _format_prob

log = get_task_logger(__name__)


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    shared=False)
def handle(alert):
    """IGWN alert handler to plot and upload a visualization
    of every ``em_bright.json``.
    """
    filename = 'em_bright.json'
    graceid = alert['uid']
    if alert['alert_type'] == 'log' and alert['data']['filename'] == filename:
        (
            gracedb.download.si(filename, graceid)
            |
            plot.s()
            |
            gracedb.upload.s(
                filename.replace('.json', '.png'),
                graceid,
                message=(
                    'Source properties visualization from '
                    '<a href="/api/superevents/{graceid}/files/{filename}">'
                    '{filename}</a>').format(
                        graceid=graceid, filename=filename),
                tags=['em_follow', 'em_bright', 'public']
            )
        ).delay()


@app.task(shared=False)
@closing_figures()
def plot(contents):
    """Make a visualization of the source properties.

    Examples
    --------
    .. plot::
        :include-source:

        >>> from gwcelery.tasks import em_bright
        >>> contents = '{"HasNS": 0.9137, "HasRemnant": 0.0, "HasMassGap": 0.0}'  # noqa E501
        >>> em_bright.plot(contents)
    """
    # Explicitly use a non-interactive Matplotlib backend.
    plt.switch_backend('agg')

    properties = json.loads(contents)
    outfile = io.BytesIO()

    properties = dict(sorted(properties.items(), reverse=True))
    probs, names = list(properties.values()), list(properties.keys())

    with plt.style.context('seaborn-v0_8-white'):
        fig, ax = plt.subplots(figsize=(3, 1))
        ax.barh(names, probs)
        ax.barh(names, [1.0 - p for p in probs],
                color='lightgray', left=probs)
        for i, prob in enumerate(probs):
            ax.annotate(_format_prob(prob), (0, i), (4, 0),
                        textcoords='offset points', ha='left', va='center')
        ax.set_xlim(0, 1)
        ax.set_xticks([])
        ax.tick_params(left=False)
        for side in ['top', 'bottom', 'right']:
            ax.spines[side].set_visible(False)
        fig.tight_layout()
        fig.savefig(outfile, format='png')
    return outfile.getvalue()


@app.task(shared=False, queue='em-bright')
def em_bright_posterior_samples(posterior_file_content):
    """Returns the probability of having a NS component and remnant
    using Bilby posterior samples.

    Parameters
    ----------
    posterior_file_content : hdf5 posterior file content

    Returns
    -------
    str
        JSON formatted string storing ``HasNS``, ``HasRemnant``,
        and ``HasMassGap`` probabilities

    Examples
    --------
    >>> em_bright_posterior_samples(GraceDb().files('S190930s',
    ... 'Bilby.posterior_samples.hdf5').read())
    {"HasNS": 0.014904901243599122, "HasRemnant": 0.0, "HasMassGap": 0.0}

    """
    from ligo.em_bright import em_bright
    with NamedTemporaryFile(content=posterior_file_content) as samplefile:
        filename = samplefile.name
        r = em_bright.source_classification_pe(
            filename, num_eos_draws=10000, eos_seed=0
        )
        has_ssm, has_ns, has_remnant, has_massgap = r
    data = json.dumps({
        'HasNS': has_ns,
        'HasRemnant': has_remnant,
        'HasMassGap': has_massgap,
        'HasSSM': has_ssm
    })
    return data


@app.task(shared=False, queue='em-bright')
def source_properties(mass1, mass2, spin1z, spin2z, snr,
                      pipeline='gstlal', search='allsky'):
    """Returns the probability of having a NS component, the probability of
    having non-zero disk mass, and the probability of any component being the
    lower mass gap for the detected event.

    Parameters
    ----------
    mass1 : float
        Primary mass in solar masses
    mass2 : float
        Secondary mass in solar masses
    spin1z : float
         Dimensionless primary aligned spin component
    spin2z : float
         Dimensionless secondary aligned spin component
    snr : float
        Signal to noise ratio
    pipeline_search : tuple
        The pipeline and the search as a tuple. This is used
        to select the appropriate classifiers in ``ligo.em-bright``
        for ``SSM`` search only. This is unused for ``AllSky``
        searches.

    Returns
    -------
    str
        JSON formatted string storing ``HasNS``, ``HasRemnant``,
        ``HasMassGap`` probabilities for ``AllSky`` searches, and
        ``HasSSM``, ``HasNS``, ``HasMassGap`` probabilities for
        ``SSM`` searches.

    Examples
    --------
    >>> em_bright.source_properties(2.0, 1.0, 0.0, 0.0, 10.)
    '{"HasNS": 1.0, "HasRemnant": 1.0, "HasMassGap": 0.0}'
    >>> em_bright.source_properties(2.0, 1.0, 0.0, 0.0, 10.,
    ... pipeline='gstlal', search='ssm')
    '{"HasSSM": 0.52, "HasNS": 0.9199999999999999, "HasMassGap": 0.08}'
    """
    from ligo.em_bright import em_bright
    if search == 'ssm':
        chirp_mass = (mass1 * mass2) ** (3. / 5.)
        chirp_mass /= (mass1 + mass2) ** (1. / 5.)
        p_ssm, p_ns, p_mg = em_bright.source_classification_ssm(
            mass1, mass2, spin1z, spin2z, chirp_mass,
            snr, pipeline
        )
        data = json.dumps({
            'HasSSM': p_ssm,
            'HasNS': p_ns,
            'HasMassGap': p_mg,
        })
    else:
        p_ns, p_em, p_mg = em_bright.source_classification(
            mass1, mass2, spin1z, spin2z, snr
        )
        data = json.dumps({
            'HasNS': p_ns,
            'HasRemnant': p_em,
            'HasMassGap': p_mg,
        })
    return data
