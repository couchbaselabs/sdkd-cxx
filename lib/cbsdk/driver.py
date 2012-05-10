"""
This module defines the higher level API used by clients wishing to interact
with the sdkd
"""

import json
import socket
import cbsdk.message
import logging

from subprocess import Popen, PIPE

from cbsdk.message import SDKDriverMessage, SDKCreateHandle

class SDKDriverFlowException(Exception): pass
    
    
class SDKMessageConduit(object):
    def __init__(self, fp = None, rdfp = None, wrfp = None):
        if not rdfp or not wrfp:
            rdfp = fp
            wrfp = fp
        
        self.wrfp = wrfp
        self.rdfp = rdfp
        
        if not self.wrfp or not self.rdfp:
            raise SDKDriverFlowException("Invalid conduit specifier")
            
        self.log = logging.getLogger()
    
    def send_msg(self, msg):
        self.log.debug("> " + str(msg))
        
        self.wrfp.write(msg.encode() + "\n")
        
    def recv_msg(self):
        txt = self.rdfp.readline()
        msg = cbsdk.message.SDKDriverResponse.parse(txt)
        self.log.debug("< " + str(msg))
        return msg
        
    

class SDKDriverHandle(object):
    """
    This object represents a single handle/instance/connection to a Couchbase
    bucket.
    It has severl convenience methods (*_simple()), an ID (handle_id),
    and an I/O conduit for direct protocol access (.conduit)
    """
    
    def __init__(self, driver, conduit,
             host='127.0.0.1',
             port=8091,
             bucket='default',
             timeout = 30,
             username = "", password = ""):
        
        logger = logging.getLogger()
        logger.name = "driver"
        self.log = logger
        
        self.driver = driver
        self.handle_id = self.driver.mkhandleid()
        self.conduit = conduit
        
        regmsg = SDKCreateHandle(
            driver.mkreqid(), self.handle_id,
            
            host, port, bucket,
            
            Timeout = timeout,
            Username = username,
            Password = password
        )
        
        self.conduit.send_msg(regmsg)
        resp = self.conduit.recv_msg()
        
        if not resp.is_ok():
            raise SDKDriverFlowException(
                "Couldn't create new handle: " + str(resp)
            )
            
    def set_simple(self, key, value, **kwargs):
        msg = cbsdk.message.SDKDatasetMutation(
            self.driver.mkreqid(),
            self.handle_id,
            0,
            cbsdk.message.SDKDatasetMutation.MUTATE_SET,
            inline_dataset = { key : value })
        
        self.conduit.send_msg(msg)
        return self.conduit.recv_msg()
        
    
    def get_simple(self, key, **kwargs):        
        msg = cbsdk.message.SDKDatasetRetrieve(self.driver.mkreqid(),
                                               self.handle_id,
                                               0,
                                               inline_dataset = [key],
                                               Detailed = True)
        
        self.conduit.send_msg(msg)
        return self.conduit.recv_msg()        
    
class SDKDriver(object):
    """
    Base class for the SDK Driver, the new API for a couchbase abstraction.
    As a common format, the SDK driver will spawn a subprocess which will
    communicate over standard input/output with JSON messages (though the
    exact conduit may change).
    """
    def __init__(self, execargs, **options):
        self.log = logging.getLogger()
        
        self.log.debug("Executing: %s", execargs)
        self.po = Popen(execargs,
                        **self.exe_popen_args() )
        
        
        if options.has_key("spawn_on_demand"):
            self.spawn_on_demand = True
        else:
            self.spawn_on_demand = False
        
        
        self.handles = {}
        self.seedreq  = 1
        self.seedhand = 1
        self.execargs = execargs
        
        if (hasattr(self, 'postexec_hook')):
            self.postexec_hook()
        
    def create_handle(self, **kwargs):
        handle = SDKDriverHandle(self, self.io_new_handle_conduit(), **kwargs)
        self.handles[handle.handle_id] = handle
        return handle
        
    def io_new_handle_conduit(self):
        raise NotImplementedError("Not yet implemented!")
        
    def io_control_conduit(self):
        raise NotImplementedError("Control conduit must be established by subclass")
        
    def exe_popen_args(self):
        raise NotImplementedError("Not yet implemented")
        
    def mkreqid(self):
        ret = self.seedreq
        self.seedreq += 1
        return ret
    
    def mkhandleid(self):
        ret = self.seedhand
        self.seedhand+=1
        return ret
    
    def create_dataset(self, identifier, kvpairs):
        
        msg = cbsdk.message.SDKCreateDataset(self.mkreqid(),
                                             identifier,
                                             kvpairs)
        self.io_control_conduit().send_msg(msg)
        resp = self.io_control_conduit().recv_msg()
        return resp
    
class SDKDriverStdio(SDKDriver):
    """
    SDK driver whose children use a simple stdio conduit
    """
    
    def exe_popen_args(self):
        return { "stdin" : PIPE, "stdout" : PIPE, "stderr" : None }
    
    def postexec_hook(self):
        self._conduit = SDKMessageConduit(rdfp = self.po.stdout,
                                          wrfp = self.po.stdin)
     
    def io_new_handle_conduit(self):
        return self._conduit
    
    def io_control_conduit(self):
        return self._conduit