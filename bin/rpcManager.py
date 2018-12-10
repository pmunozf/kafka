#!/usr/bin/env python
#
# Pablo Munoz (c) 2018
#
# A RPC Server manager for administrating service
# deamons.
#
#
import os
import sys
import tempfile
import subprocess
import zerorpc
from uuid import uuid4
from argparse import ArgumentParser
from mess import ZookeeperInstance
from mess.utils import dotdict


def randomId():
    return str(uuid4())

def getClient(args):
    client = zerorpc.Client(timeout=10 * 2)
    client.connect('{}://{}:{}'.format(args.protocol, args.host, args.port))
    return client

def stop_zookeeper(args):
    client = getClient(args)
    result = client.stopService('zookeeper')
    print(result)

def info_zookeeper(args):
    client = getClient(args)
    result = client.getInfo('zookeeper')
    print(result)

def deploy_zookeeper(args):
    # Resolve and make execution dir
    if not  args.execDir:
        if args.temp: # Temporary file mode
            args.execDir = tempfile.NamedTemporaryFile(prefix='msgsys-')
        else: # Randomized id inside default exec folder
            defaultExecDir = os.path.join(os.environ['KAFKA_REPO'], 'exec', randomId())
            os.makedirs(defaultExecDir, exist_ok=True)
            args.execDir = defaultExecDir
    client = getClient(args)
    #Check if running service
    if client.hasServiceRunning('zookeeper'):
        print('A zookeeper instance is already running.')
        sys.exit(1)
    else:
        config = {
            'execDir' : args.execDir,
            'zookeeperClientPort' : args.zookeeperClientPort,
            'zookeeperMaxClientCnxns' : args.zookeeperMaxClientCnxns,
            'zookeeperTickTime' : args.zookeeperTickTime,
            'verbose' : args.verbose
        }
        ret = client.deploy_zookeeper(config)
        print(ret)
        sys.stdout.flush()
        sys.exit(0)

def main_loop():
    '''TODO This is curently in the bin script. Should be moved to
    '''
    manager = Manager(args.protocol, args.host, args.port)
    manager.run()
    sys.stdout.flush()



class Manager():
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
        self.services = {}
        self.config = dotdict
        self.config.protocol = protocol
        self.config.host = host
        self.config.port = port
        self.rpcServer = zerorpc.Server(self)
        self.rpcServer.bind('{}://{}:{}'.format(protocol, host, port))

    def run(self):
        print(Manager.__HELP)
        self.rpcServer.run()

    def hasServiceRunning(self, serviceName):
        return serviceName in self.services

    def startService(self, serviceName, serviceInstance):
        self.services[serviceName] = serviceInstance
        serviceInstance.start()

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



    def deploy_zookeeper(self, args):
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

    def stop_zookeeper(self):#TODO this assumes standalone
        self.stopService('zookeeper')

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-d', '--deploy', default=None)
    parser.add_argument('-s', '--stop', default=None)
    parser.add_argument('-i', '--info', default=None)
    parser.add_argument('-x', '--execDir', default=None)
    parser.add_argument('-t', '--temp', help='Execute in temporary mode', default=False)
    parser.add_argument('-v', '--verbose', default=True)
    parser.add_argument('-pr', '--protocol', default='tcp')
    parser.add_argument('-ho', '--host', default='0.0.0.0')
    parser.add_argument('-po', '--port', default='4242')
    parser.add_argument('-zcp', '--zookeeperClientPort', default=2181)
    parser.add_argument('-zmc', '--zookeeperMaxClientCnxns', default=0)
    parser.add_argument('-ztt', '--zookeeperTickTime', default=2000)
    args = parser.parse_args()

    commands = ['deploy', 'stop', 'info']
    command = None
    service = None
    for cmd in commands:
        if getattr(args, cmd):
            if command:
                print('ERROR: Only one instruction per command')
                sys.exit(1)
            else:
               service = getattr(args, cmd)
               command = cmd
    if command:
        try:
            eval(command + '_' + service + '(args)')
            sys.exit(0)
        except NameError:
            print('ERROR: Command not found: ' + command + ' ' + service)
            sys.exit(1)
    else:
        main_loop()
