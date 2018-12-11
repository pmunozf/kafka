#!/usr/bin/env python
#
# Pablo Munoz (C) 2018
#
# Server manager abstraction
import os
import sys
import time
import tempfile
import subprocess
import zerorpc
from threading import Thread
from uuid import uuid4
from argparse import ArgumentParser
from mess import ZookeeperInstance
from mess.utils import dotdict



class ServerManager():
    '''RPC Server Manager'''
    __HELP ="""
    #################################################
    This is a RPC Service Manager log console.
    In order to issue a command to this service run

    ${KAFKA_REPO}/bin/rpcManager.py

    with the following commands:

    Commands:
        -d, --deploy SERVICE_NAME
        -s, --stop SERVICE_NAME
        -pr, --protocol (default TCP)
        -ho, --host (default localhost, 0.0.0.0)
        -po, --port (default 4242)

    #################################################

    """

    def __init__(self, protocol, host, port):
        # A registry uf running services
        self.services = {}

        # Config variables
        self.config = dotdict
        self.config.protocol = protocol
        self.config.host = host
        self.config.port = port

        # Setup rpc facility
        self.rpcServer = zerorpc.Server(self)
        self.rpcServer.bind('{}://{}:{}'.format(protocol, host, port))

    def runRpcServer(self):
        """Serve a log console UI."""
        print(ServerManager.__HELP)
        boot = Thread(target=self.serveConsole)
        boot.start()
        self.rpcServer.run()

    def serveConsole(self):
        while True:
            cmd = input('> ')
            if cmd == "quit":
                break
            else:
                print('Your command is ' + cmd)

    def hasServiceRunning(self, serviceName):
        return serviceName in self.services

    def startService(self, serviceName, serviceInstance):
        try:
            return self.services[serviceName].start()
        except KeyError:
            return 'Error: service {} not installed.'.format(serviceName)

    def stopService(self, serviceName):
        if self.hasServiceRunning(serviceName):
            self.services[serviceName].kill()
            del self.services[serviceName]
            return "Stopped service " + serviceName
        else:
            return "Service not running " + serviceName

    def getInfo(self, serviceName):
        print('GETTING INFO:: ' + serviceName)
        try:
            info = str(self.services[serviceName])
        except KeyError:
            info = "Not running service " + serviceName
        info = '\t' + info
        print(info)
        return info

    def deployZookeeper(self, args):
        print('DEPLOYING :: zookeeper')
        zkExecDir = os.path.join(args['execDir'], 'zookeeper/')
        zookeeper = ZookeeperInstance(zkExecDir,
                                      clientPort=args['zookeeperClientPort'],
                                      maxClientCnxns=args['zookeeperMaxClientCnxns'],
                                      tickTime=args['zookeeperTickTime'],
                                      verbose=args['verbose'])
        self.startService('zookeeper', zookeeper)
        print(str(zookeeper))
        return 'deployed zookeeper at ' + args['execDir']

