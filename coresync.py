#!/usr/bin/env python

import signal
from sys import exit
from json import load
from urllib import urlopen
from argparse import ArgumentParser
from time import sleep

verbose = False
status = 'http://%s:%s/solr/admin/cores?wt=json'
create = 'http://%(host)s:%(port)s/solr/admin/cores?action=CREATE&name=%(core)s&instanceDir=default_merge&config=solrconfig.xml&schema=schema.xml&dataDir=../%(core)s'
sleeptime = 0.1


def signal_handler(signal, frame):
    print '\nExiting ...'
    exit(1)

def parseargs():
    p = ArgumentParser()
    p.add_argument('-o', '--old', metavar = 'Old solr server', required = True, help = 'Old solr server')
    p.add_argument('-n', '--new', metavar = 'New solr server', required = True, help = 'New solr server')
    p.add_argument('-p', '--port', metavar = 'Solr port', required = True, type = int, choices = [ 8983, 8984, 8985 ], help = 'Solr port')
    p.add_argument('-v', '--verbose', action = 'store_true')
    return vars(p.parse_args())

def getcores(host, port):
    try:
        doc = load(urlopen(status % (host, port)))['status']
    except IOError, e:
        if verbose == True: print e
        exit(1)
    cores = []
    for core in doc:
        cores.append(core)
    return cores

def createcore(host, port, core):
    try:
        res = urlopen(create % {'host': host, 'port': port, 'core': core})
        if res.code != 200 and verbose == True:
            print res.readlines()[2]
    except IOError, e:
        if verbose == True: print e
        exit(1)

def main():
    cores = list(set(getcores(params['old'], params['port']) - set(getcores(params['new'], params['port']))))
    for core in cores:
        createcore(params['new'], params['port'], core)
        sleep(sleeptime)
    exit(0)


if __name__ == '__main__':
    params = parseargs()
    verbose = params['verbose']
    signal.signal(signal.SIGINT, signal_handler)
    main()
