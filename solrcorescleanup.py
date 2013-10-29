#!/usr/bin/env python

import os
import logging, logging.handlers
from xml.dom import minidom
from urllib2 import urlopen
from shutil import move
from datetime import datetime

###VARIABLES
SOLR_ADDR = "127.0.0.1"
SOLR_INSTANCES = (("solr1", 8983), )
SOLR_BASE_PATH = "/opt"
BACKUP_DIR_BASE = "/opt/cores_backup"
CORE_PREFIX = "s4"
ZABBIX_CONF = "/etc/zabbix/zabbix_agentd.conf"
LOG_FILE = "/var/log/solrcorescleanup.log"
ADMIN_EMAIL = "admin@email"


def sendZabbix(zabbixKey, zabbixValue):
    if os.system("zabbix_sender -c %s -s %s -k %s -o %s" % (ZABBIX_CONF, hostname, zabbixKey, zabbixValue)) == 0:
        log.info("Sent data to Zabbix")
    else:
        log.error("Error sending data to Zabbix, exiting ...")
        os._exit(1)


class solrInstance:
    def __init__(self, name, port):
        self.name = name
        self.port = port
        self.solrcores = []
        self.diskcores = []
        self.inactivecores = []
        self.inactivecoreslen = 0
        self.backupdir = "%s-%s" % (self.name, now.strftime("%Y-%m-%d"))

    def getSolrCores(self):
        doc = urlopen("http://%s:%s/solr/admin/cores?action=STATUS" % (SOLR_ADDR, self.port))
        xmldoc = minidom.parseString(doc.read())
        lst_tags = xmldoc.getElementsByTagName("lst")
        for i in lst_tags:
            if i.attributes["name"].value.startswith(CORE_PREFIX):
                self.solrcores.append(i.attributes["name"].value)

    def getDiskCores(self):
        for d in os.listdir(os.path.join(SOLR_BASE_PATH, self.name, "example/multicore")):
            if d.startswith(CORE_PREFIX) and os.path.isdir(os.path.join(SOLR_BASE_PATH, self.name, "example/multicore", d)):
                self.diskcores.append(d)

    def getInactiveCores(self):
        self.inactivecores = list(set(self.diskcores) - set(self.solrcores))
        self.inactivecoreslen = self.inactivecores.__len__()

    def moveCores(self):
        try:
            os.mkdir(os.path.join(BACKUP_DIR_BASE, self.backupdir))
        except OSError:
            pass
        self.getSolrCores()
        self.getDiskCores()
        self.getInactiveCores()
        for d in self.inactivecores:
            try:
                move(os.path.join(SOLR_BASE_PATH, self.name, "example/multicore", d), os.path.join(BACKUP_DIR_BASE, self.backupdir, d))
            except:
                log.error("Error moving cores for %s" % self.name)
                os._exit(1)


solrs = []
now = datetime.now()
hostname = os.uname()[1]

x = logging.getLogger("log")
x.setLevel(logging.INFO)

h1 = logging.FileHandler(LOG_FILE)
f = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
h1.setFormatter(f)
h1.setLevel(logging.INFO)
x.addHandler(h1)

h2 = logging.handlers.SMTPHandler('127.0.0.1', "root <root@%s>" % hostname, ADMIN_EMAIL, 'ERROR: Solr cores cleanup failed on %s' % hostname)
h2.setLevel(logging.ERROR)
h2.setFormatter(f)
x.addHandler(h2)
log = logging.getLogger("log")


try:
    os.stat(BACKUP_DIR_BASE)
except OSError:
    os.mkdir(BACKUP_DIR_BASE, 0700)
    log.info("Created %s" % BACKUP_DIR_BASE)


try:
    os.stat(ZABBIX_CONF)
except OSError:
    log.error("Zabbix config is missing, exiting ...")
    os._exit(1)

if not os.access(BACKUP_DIR_BASE, os.R_OK | os.W_OK | os.X_OK):
    log.error("Insufficient permissions on backup dir, exiting ...")
    os._exit(1)


for i in SOLR_INSTANCES:
    solrs.append(solrInstance(i[0], i[1]))


for s in solrs:
    s.moveCores()
    log.info("Moved %s cores for %s instance" % (s.inactivecoreslen, s.name))
    sendZabbix(s.name, s.inactivecoreslen)
