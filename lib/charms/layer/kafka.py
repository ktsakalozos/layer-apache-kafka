import os
import signal
from subprocess import Popen

import jujuresources
from charmhelpers.core import unitdata, hookenv
from jujubigdata import utils


class Kafka(object):
    def __init__(self, dist_config):
        self.dist_config = dist_config
        self.resources = {
            'kafka': 'kafka-%s' % utils.cpu_arch(),
        }
        self.verify_resources = utils.verify_resources(*self.resources.values())

    def is_installed(self):
        return unitdata.kv().get('kafka.installed')

    def install(self, force=False):
        if not force and self.is_installed():
            return
        self.dist_config.add_users()
        self.dist_config.add_dirs()
        self.dist_config.add_packages()
        jujuresources.install(self.resources['kafka'],
                              destination=self.dist_config.path('kafka'),
                              skip_top_level=True)
        self.setup_kafka_config()
        unitdata.kv().set('kafka.installed', True)

    def setup_kafka_config(self):
        '''
        copy the default configuration files to kafka_conf property
        defined in dist.yaml
        '''
        default_conf = self.dist_config.path('kafka') / 'config'
        kafka_conf = self.dist_config.path('kafka_conf')
        kafka_conf.rmtree_p()
        default_conf.copytree(kafka_conf)
        # Now remove the conf included in the tarball and symlink our real conf
        # dir. we've seen issues where kafka still looks for config in
        # KAFKA_HOME/config.
        default_conf.rmtree_p()
        kafka_conf.symlink(default_conf)

        # Configure immutable bits
        kafka_bin = self.dist_config.path('kafka') / 'bin'
        with utils.environment_edit_in_place('/etc/environment') as env:
            if kafka_bin not in env['PATH']:
                env['PATH'] = ':'.join([env['PATH'], kafka_bin])
            env['LOG_DIR'] = self.dist_config.path('kafka_app_logs')

        # note: we set the advertised.host.name below to the public_address
        # to ensure that external (non-Juju) clients can connect to Kafka
        public_address = hookenv.unit_get('public-address')
        private_ip = utils.resolve_private_address(hookenv.unit_get('private-address'))
        kafka_server_conf = self.dist_config.path('kafka_conf') / 'server.properties'
        service, unit_num = os.environ['JUJU_UNIT_NAME'].split('/', 1)
        utils.re_edit_in_place(kafka_server_conf, {
            r'^broker.id=.*': 'broker.id=%s' % unit_num,
            r'^port=.*': 'port=%s' % self.dist_config.port('kafka'),
            r'^log.dirs=.*': 'log.dirs=%s' % self.dist_config.path('kafka_data_logs'),
            r'^#?advertised.host.name=.*': 'advertised.host.name=%s' % public_address,
        })

        kafka_log4j = self.dist_config.path('kafka_conf') / 'log4j.properties'
        utils.re_edit_in_place(kafka_log4j, {
            r'^kafka.logs.dir=.*': 'kafka.logs.dir=%s' % self.dist_config.path('kafka_app_logs'),
        })

        # fix for lxc containers and some corner cases in manual provider
        # ensure that public_address is resolvable internally by mapping it to the private IP
        utils.update_kv_host(private_ip, public_address)
        utils.manage_etc_hosts()

    def open_ports(self):
        for port in self.dist_config.exposed_ports('kafka'):
            hookenv.open_port(port)

    def configure_kafka(self, zk_units):
        # Get ip:port data from our connected zookeepers
        if not zk_units:
            # if we have no zookeepers, make sure kafka is stopped
            self.stop()
        else:
            zks = []
            for remote_address, port in zk_units:
                ip = utils.resolve_private_address(remote_address)
                zks.append("%s:%s" % (ip, port))
            zks.sort()
            zk_connect = ",".join(zks)

            # update consumer props
            cfg = self.dist_config.path('kafka_conf') / 'consumer.properties'
            utils.re_edit_in_place(cfg, {
                r'^zookeeper.connect=.*': 'zookeeper.connect=%s' % zk_connect,
            })

            # update server props
            cfg = self.dist_config.path('kafka_conf') / 'server.properties'
            utils.re_edit_in_place(cfg, {
                r'^zookeeper.connect=.*': 'zookeeper.connect=%s' % zk_connect,
            })

    def run_bg(self, user, command, *args):
        """
        Run a Kafka command as the `kafka` user in the background.

        :param str command: Command to run
        :param list args: Additional args to pass to the command
        """
        parts = [command] + list(args)
        quoted = ' '.join("'%s'" % p for p in parts)
        e = utils.read_etc_env()
        Popen(['su', user, '-c', quoted], env=e)

    def restart(self):
        self.stop()
        self.start()

    def start(self):
        kafka_conf = self.dist_config.path('kafka_conf')
        kafka_bin = self.dist_config.path('kafka') / 'bin'
        self.stop()
        self.run_bg('kafka',
                    kafka_bin / 'kafka-server-start.sh',
                    kafka_conf / 'server.properties')

    def stop(self):
        kafka_pids = utils.jps('kafka.Kafka')
        for pid in kafka_pids:
            os.kill(int(pid), signal.SIGKILL)

    def cleanup(self):
        self.dist_config.remove_users()
        self.dist_config.remove_dirs()
