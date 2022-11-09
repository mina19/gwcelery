"""Qualitative source properties for CBC events."""
import io
import json
from matplotlib import pyplot as plt

from ligo.em_bright import em_bright

from celery.utils.log import get_task_logger

from ..import app
from . import gracedb, igwn_alert
from .p_astro import _format_prob
from ..util import closing_figures, NamedTemporaryFile


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

    with plt.style.context('seaborn-white'):
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


@app.task(shared=False)
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
    with NamedTemporaryFile(content=posterior_file_content) as samplefile:
        filename = samplefile.name
        has_ns, has_remnant, has_massgap = em_bright.source_classification_pe(
            filename, num_eos_draws=100, eos_seed=0
        )
    data = json.dumps({
        'HasNS': has_ns,
        'HasRemnant': has_remnant,
        'HasMassGap': has_massgap
    })
    return data


@app.task(shared=False)
def source_properties(mass1, mass2, spin1z, spin2z, snr):
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

    Returns
    -------
    str
        JSON formatted string storing ``HasNS``, ``HasRemnant``,
        and `HasMassGap`` probabilities

    Examples
    --------
    >>> em_bright.source_properties(2.0, 1.0, 0.0, 0.0, 10.)
    '{"HasNS": 1.0, "HasRemnant": 1.0, "HasMassGap"}'
    """
    p_ns, p_em, p_mg = em_bright.source_classification(
        mass1, mass2, spin1z, spin2z, snr
    )

    data = json.dumps({
        'HasNS': p_ns,
        'HasRemnant': p_em,
        'HasMassGap': p_mg
    })
    return data
