#!/usr/bin/env python
#
# Pablo Munoz (c) 2018
#
# General service instance interface that encapsulates
# execution cage, logging, IO, config, deployment, cleaning up,
# and UI services.
#
import os
import sys
import logging
import time
import subprocess
from threading import Thread

class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

class ZookeeperInstance(Thread):

    def __init__(self, execDir, logFileMode='w', logLevel=logging.DEBUG,
                       clientPort=2181, maxClientCnxns=0, tickTime=2000, verbose=False):
        # Call thread super
        super(ZookeeperInstance, self).__init__()

        # Initialize instance config
        self.exit = False
        self.config = dotdict()
        self.config.execDir = execDir
        self.config.rootFolders = ['data', 'etc']
        self.config.configFilename = os.path.join(execDir, 'etc', 'zookeeper.properties')
        self.config.configTemplateFilename = os.path.join(os.environ['KAFKA_REPO'],
                                                          'etc/zookeeper.properties.template')
        self.config.logFile = os.path.join(execDir, 'log')
        self.config.logFileMode = logFileMode
        self.config.logLevel = logLevel
        self.config.logFormat = '[%(levelname)s:%(threadName)s:%(asctime)s] %(message)s'
        self.config.configTemplateValue = dotdict()
        self.config.configTemplateValue.dataDir = os.path.join(execDir, 'data')
        self.config.configTemplateValue.clientPort = 2181
        self.config.configTemplateValue.maxClientCnxns = 0
        self.config.configTemplateValue.tickTime = 2000
        self.config.bin = dotdict()
        self.config.bin.serviceHome = os.environ['KAFKA_HOME']
        self.config.bin.binaryRelativePath = 'bin/zookeeper-server-start.sh'
        self.config.bin.stopBinaryPath = 'bin/zookeeper-server-stop.sh'
        self.config.verbose = verbose
        self.serviceName = 'zookeeper'

        # Make execution dir
        os.makedirs(self.config.execDir)

    def __str__(self):
        strrpr="""
Zookeeper instance
    address             localhost:{}
    execution dir       {}
    binary              {}
        """.format(self.config.configTemplateValue.clientPort,
                   self.config.execDir,
                   os.path.join(self.config.bin.serviceHome,
                                self.config.bin.binaryRelativePath))
        return strrpr

    def setupExecDir(self):
        '''Prepare the execution dir'''
        self.log('debug', 'Preparing exec dir ' + self.config.execDir)
        for folder in self.config.rootFolders:
            folder_path = os.path.join(self.config.execDir, folder)
            self.log('debug', 'mkdir ' + folder_path)
            os.makedirs(folder_path)

    def setupConfig(self):
        """Prepare the config files."""
        self.log('debug', 'Preparing config file ' + self.config.configFilename)

        # Replace template with values
        configTemplate = ''
        with open(self.config.configTemplateFilename, 'r') as configTemplateFile:
            configTemplate = configTemplateFile.read()
            for configVariable, configValue in self.config.configTemplateValue.items():
                configTemplate = configTemplate.replace('%'+configVariable+'%', str(configValue))

        # Write resulting config file
        with open(self.config.configFilename, 'w') as configFile:
            configFile.write(configTemplate)

    def setupLogger(self):
        """Initialize logging facility."""
        logging.basicConfig(filename=self.config.logFile,
                            filemode=self.config.logFileMode,
                            level=self.config.logLevel,
                            format=self.config.logFormat)
        self.logger = logging.getLogger('mess.logger.' + self.serviceName)

    def log(self, mode, msg):
        """Log a message under the specified mode
           e.g. instance.log(debug, 'debug msg')."""
        getattr(self.logger, mode)(msg)

    def deploy(self): #TODO
        '''Deploy service'''
        cmd = [
            os.path.join(self.config.bin.serviceHome,
                         self.config.bin.binaryRelativePath),
            self.config.configFilename
               ]
        self.log('debug', 'Deploying ' + self.serviceName)
        self.log('debug', cmd)
        cmd = ' '.join(cmd)
        self.process = subprocess.Popen(cmd,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        shell=True)

    def getInfo(self):
        #self.config.pipe = self.process.communicate()
        return self.config

    def exit(self):
        self.exit = True

    def kill(self):
        self.log('debug', 'Killing zookeeper instance')
        cmd = os.path.join(self.config.bin.serviceHome,
                           'bin/zookeeper-server-stop.sh')
        print(cmd)
        sys.stdout.flush()
        exitCode = subprocess.call(cmd, shell=True)
        if exitCode == 1:
            self.log('debug', '[Bug] Tried to kill when no service is running')
        else:
            print('Killed the zookeeper instance.')
        sys.stdout.flush()

        return exitCode == 0


    def run(self):
        """Thread main"""
        self.setupLogger()
        self.setupExecDir()
        self.setupConfig()
        self.deploy()
        while True:
            if self.exit:
                self.kill()
                return


        #self.log('debug', str(self.process.communicate()))
        #print(self.process.communicate()[0].decode('utf-8'))

