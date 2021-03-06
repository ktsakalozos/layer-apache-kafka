from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state

from charmhelpers.core import hookenv

from charms.layer.kafka import Kafka

from jujubigdata.utils import DistConfig


@when_not('kafka.installed')
def install_kafka(*args):

    kafka = Kafka(DistConfig())
    if kafka.verify_resources():
        hookenv.status_set('maintenance', 'Installing Kafka')
        kafka.install()
        kafka.open_ports()
        set_state('kafka.installed')


@when('kafka.installed')
@when_not('zookeeper.connected')
def waiting_for_zookeeper_connection():
    hookenv.status_set('blocked', 'Waiting for connection to Zookeeper')


@when('kafka.installed', 'zookeeper.connected')
@when_not('zookeeper.available')
def waiting_for_zookeeper_available(zk):
    hookenv.status_set('waiting', 'Waiting for Zookeeper to become ready')


@when('kafka.installed', 'zookeeper.joining', 'zookeeper.available')
@when_not('kafka.started')
def configure_kafka(zkjoining, zkavailable):
    try:
        zk_units = zkavailable.get_zookeeper_units()
        hookenv.status_set('maintenance', 'Setting up Kafka')
        kafka = Kafka(DistConfig())
        kafka.configure_kafka(zk_units)
        kafka.start()
        zkjoining.dismiss_joining()
        hookenv.status_set('active', 'Ready')
        set_state('kafka.started')
    except:
        hookenv.log("Relation with Zookeeper not established")


@when('kafka.started', 'zookeeper.joining', 'zookeeper.available')
def reconfigure_kafka_new_zk_instances(zkjoining, zkavailable):
    try:
        zk_units = zkavailable.get_zookeeper_units()
        hookenv.status_set('maintenance', 'Updating Kafka with new Zookeeper instances')
        kafka = Kafka(DistConfig())
        kafka.configure_kafka(zk_units)
        kafka.restart()
        zkjoining.dismiss_joining()
        hookenv.status_set('active', 'Ready')
    except:
        hookenv.log("Relation with Zookeeper not established")


@when('kafka.started', 'zookeeper.departing', 'zookeeper.available')
def reconfigure_kafka_zk_instances_leaving(zkdeparting, zkavailable):
    try:
        zk_units = zkavailable.get_zookeeper_units()
        hookenv.status_set('maintenance', 'Updating Kafka with departing Zookeeper instances ')
        kafka = Kafka(DistConfig())
        kafka.configure_kafka(zk_units)
        kafka.restart()
        zkdeparting.dismiss_departing()
        hookenv.status_set('active', 'Ready')
    except:
        hookenv.log("Relation with Zookeeper not established. Stopping Kafka.")
        kafka = Kafka(DistConfig())
        kafka.stop()
        remove_state('kafka.started')
        hookenv.status_set('blocked', 'Waiting for connection to Zookeeper')


@when('kafka.connected', 'zookeeper.available')
def serve_client(kafka_client, zookeeper):
    kafka_port = DistConfig().port('kafka')
    kafka_client.send_port(kafka_port)
    kafka_client.send_zookeepers(zookeeper.get_zookeeper_units())
    hookenv.log('Sending configuration to client')
