#!/usr/bin/env python

from sys import exit
from json import load
from urllib import urlopen
from argparse import ArgumentParser
from time import sleep


status = 'http://%s:%s/solr/admin/cores?wt=json'
create = 'http://%(host)s:%(port)s/solr/admin/cores?action=CREATE&name=%(core)s&instanceDir=default_merge&config=solrconfig.xml&schema=schema.xml&dataDir=../%(core)s'

def parseargs():
    p = ArgumentParser()
    p.add_argument('-o', '--old', metavar = 'Old solr server', required = True, help = 'Old solr server')
    p.add_argument('-n', '--new', metavar = 'New solr server', required = True, help = 'New solr server')
    p.add_argument('-p', '--port', metavar = 'Solr port', required = True, type = int, choices = [ 8983, 8984, 8985 ], help = 'Solr port')
    return vars(p.parse_args())

def getcores(host, port):
    doc = load(urlopen(status % (host, port)))['status']
    cores = []
    for core in doc:
        cores.append(core)
    return cores

def createcore(host, port, core):
    try:
        res = urlopen(create % {'host': host, 'port': port, 'core': core})
        if res.code != 200:
            print res.readlines()[2]
    except IOError, e:
        print e
        exit(1)


if __name__ == '__main__':
    params = parseargs()
    for core in getcores(params['old'], params['port']):
        createcore(params['new'], params['port'], core)
        sleep(1/10)
    exit(0)
