import re
import yaml
import subprocess


def get_zookeepers():
    with open("dist.yaml", 'r') as distconf:
        config = yaml.load(distconf)

    cfg = '/'.join((config["dirs"]["kafka_conf"]["path"], 'consumer.properties'))
    print(cfg)
    file = open(cfg, "r")

    for line in file:
        if re.search('^zookeeper.connect=.*', line):
            zks = line.split("=")[1].strip('\n')
            return zks

    return None

def kafka_is_running():
    s = subprocess.Popen(["ps", "axw"],stdout=subprocess.PIPE)
    for line in s.stdout:
        if re.search(b'/usr/lib/kafka/bin/kafka-server-start.sh', line):
            return True

    return False
