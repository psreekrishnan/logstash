from pathlib import Path
from charmhelpers import fetch
from subprocess import check_output, STDOUT
from charmhelpers.core import hookenv, host
from charmhelpers.core.templating import render
from charms.reactive import (
    endpoint_from_flag,
    is_flag_set,
    set_flag,
    clear_flag,
    when,
    when_file_changed,
    when_not
)


CONF_DIR = Path("/etc/logstash/conf.d")
BEATS_CONF = CONF_DIR / "beats.conf"


def fail_on_java_unavailable():
    try:
        check_output(['java', '-version'], stderr=STDOUT).decode()
    except Exception as e:
        hookenv.log('Failed to find Java binary')
        hookenv.status_set('BLOCKED', 'Failed to find Java binary')
        print(e.output)
        exit(1)


def logstash_version():
    app_version = ''
    try:
        app_version = check_output(['/usr/share/logstash/bin/logstash', '--version']).decode().strip("logstash").strip()
    except Exception as e:
        hookenv.log('Failed to get Logstash version')
        hookenv.status_set('BLOCKED', 'Failed to find Logstash binary')
        print(e.output)
        exit(1)
    return app_version


@when_not('logstash.installed')
def install_logstash():
    fetch.configure_sources()
    fetch.apt_update()
    fetch.apt_install('openjdk-8-jre-headless')
    fail_on_java_unavailable()
    fetch.apt_install('logstash')
    set_flag('logstash.installed')


@when('logstash.installed')
def set_logstash_version():
    hookenv.application_version_set(logstash_version())
    host.service_start('logstash')
    set_flag('logstash.available')


@when('logstash.available')
@when_not('logstash.beats.conf.available')
def render_beat_conf():
    """Create context and render beat conf.
    """

    context = {
        'es_nodes': [],
        'beats_port': hookenv.config('beats_port'),
    }

    if is_flag_set('endpoint.elasticsearch.available'):
        endpoint = hookenv.endpoint_from_flag('endpoint.elasticsearch.available')
        [context['es_nodes'].append("{}:{}".format(unit['host'], unit['port']))
         for unit in endpoint.list_unit_data()]

    if BEATS_CONF.exists():
        BEATS_CONF.unlink()

    render('beats.conf', str(BEATS_CONF), context)
    set_flag('logstash.beats.conf.available')


@when('endpoint.elasticsearch.available')
@when_not('endpoint.elasticsearch.available')
def re_render_conf():
    clear_flag('logstash.beats.conf.available')


@when_file_changed('/etc/logstash/conf.d/beats.conf')
def recycle_logstash_service():
    host.service_restart('logstash')


@when('logstash.available')
def set_logstash_version_in_unit_data():
    hookenv.status_set('active', 'Logstash running - version {}'.format(logstash_version()))


@when('client.connected')
def configure_logstash_input():
    '''Configure the legacy logstash clients.'''
    endpoint = endpoint_from_flag('client.connected')
    # Send the port data to the clients.
    endpoint.provide_data(hookenv.config('tcp_port'), hookenv.config('udp_port'))


@when('beat.connected')
def configure_filebeat_input():
    '''Configure the logstash beat clients.'''
    endpoint = endpoint_from_flag('beat.connected')
    endpoint.provide_data(hookenv.config('beats_port'))
    clear_flag('logstash.beats.conf.available')
