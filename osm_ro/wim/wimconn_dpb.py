# -*- coding: utf-8 -*-
##
# Copyright 2019 University of Lancaster - High Performance Networks Research
# Group
# All Rights Reserved.
#
# Contributors: Paul McCherry, Will Fantom
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# For those usages not covered by the Apache License, Version 2.0 please
# contact with: <highperformance-networks@bristol.ac.uk>
#
# Neither the name of the University of Bristol, University of Lancaster 
# nor the names of its contributors may be used to endorse or promote 
# products derived from this software without specific prior written permission.
#
# This work has been performed in the context of DCMS UK 5G Testbeds
# & Trials Programme and in the framework of the Metro-Haul project -
# funded by the European Commission under Grant number 761727 through the
# Horizon 2020 and 5G-PPP programmes.
##

import json
import struct
import logging
import random
import sys #FIXME: Used to print loggers to stdout
import operator
import requests
from enum import Enum
try:
    import paramiko
except:
    exit("Install Paramiko [pip install paramiko]")
from wimconn import WimConnector, WimConnectorError

# TODO: List
#  - Add correct HTTP error codes
#  - Add some comments....
#  - PEP8 it

class DpbSshInterface():
    """ Communicate with the DPB via SSH """

    __LOGGER_NAME_EXT = ".ssh"
    __FUNCTION_MAP_POS = 1

    def __init__(self, wim_account, wim_url, wim_port, network, auth_data, logger_name):
        self.logger = logging.getLogger(logger_name + self.__LOGGER_NAME_EXT)
        self.__account = wim_account
        self.__url = wim_url
        self.__port = wim_port
        self.__network = network
        self.__auth_data = auth_data
        self.__session_id = 1
        self.__ssh_client = self.__create_client()
        self.__stdin, self.__stdout = self.__connect()
        self.logger.info("SSH connection to DPB made OK")

    def post(self, function, url_params="", data=None, get_response=True):
        if data == None:
            data = {}
        url_ext_info = url_params.split('/')
        for i in range(0, len(url_ext_info)):
            if url_ext_info[i] == "service":
                data["service-id"] = int(url_ext_info[i+1])
        data["type"] = function[self.__FUNCTION_MAP_POS]
        data = {
            "session": self.__session_id,
            "content": data
        }
        self.__session_id += 1

        try:
            data = json.dumps(data).encode("utf-8")
            data_packed = struct.pack(">I" + str(len(data)) + "s", len(data), data)
            self.__stdin.write(data_packed)
            self.logger.debug("Data sent to DPB")
        except:
            raise WimConnectorError("Failed to write via SSH", 500)

        try:
            data_len = struct.unpack(">I", self.__stdout.read(4))[0]
            data = struct.unpack(str(data_len) + "s", self.__stdout.read(data_len))[0]
            return json.loads(data).get("content", {})
        except:
            raise WimConnectorError("Could not get response from WIM", 500)

    def get(self, function, url_params=""):
        raise WimConnectorError("SSH Get not implemented", 500)

    def __create_client(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return ssh_client
    
    def __connect(self):
        private_key = None
        password = None
        if self.__auth_data.get("auth_type", "PASS") == "KEY":
            private_key = self.__build_private_key_obj()
        if self.__auth_data.get("auth_type", "PASS") == "PASS":
            passsword = self.__account.get("passwd", None)

        try:
            self.__ssh_client.connect(hostname=self.__url,
                                      port=self.__port,
                                      username=self.__account.get("user"),
                                      password=password,
                                      pkey=private_key,
                                      look_for_keys=False,
                                      compress=False)
            stdin, stdout, stderr = self.__ssh_client.exec_command(command=self.__network)
        except paramiko.BadHostKeyException:
            raise WimConnectorError("Could not add SSH host key", 500)
        except paramiko.AuthenticationException:
            raise WimConnectorError("Could not authorize SSH connection", 400)
        except paramiko.SSHException:
            raise WimConnectorError("Could not establish the SSH connection", 500)
        except:
            raise WimConnectorError("Unknown error occured when connecting via SSH", 500)

        try:
            data_len = struct.unpack(">I", stdout.read(4))[0]
            data = json.loads(struct.unpack(str(data_len) + "s", stdout.read(data_len))[0])
        except:
            raise WimConnectorError("Failed to get response from DPB", 500)
        if "error" in data:
            raise WimConnectorError(data.get("msg", data.get("error", "ERROR")), 500)
        return stdin, stdout

    def __build_private_key_obj(self):
        try:
            with open(self.__auth_data.get("key_file"), 'r') as key_file:
                if self.__auth_data.get("key_type") == "RSA":
                    return paramiko.RSAKey.from_private_key(key_file,
                                                            password=self.__auth_data.get("key_pass", None))
                elif self.__auth_data.get("key_type") == "ECDSA":
                    return paramiko.ECDSAKey.from_private_key(key_file,
                                                              password=self.__auth_data.get("key_pass", None))
                else:
                    raise WimConnectorError("Key type not supported", 400)
        except:
            raise WimConnectorError("Could not load private SSH key", 500)


class DpbRestInterface():
    """ Communicate with the DPB via the REST API """

    __LOGGER_NAME_EXT = ".rest"
    __FUNCTION_MAP_POS = 0

    def __init__(self, wim_account, wim_url, wim_port, network, logger_name):
        self.logger = logging.getLogger(logger_name + self.__LOGGER_NAME_EXT)
        self.__account = wim_account
        self.__base_url = "http://{}:{}/network/{}".format(wim_url, str(wim_port), network)
        self.logger.info("REST OK")

    def post(self, function, url_params="", data=None, get_response=True):
        url = self.__base_url + url_params + "/" + function[self.__FUNCTION_MAP_POS]
        try:
            response = requests.post(url, json=data)
            '''if response.status_code != 200:
                raise WimConnectorError("REST request failed (non-200 status code)")'''
            if get_response:
                return response.json()
        except:
            raise WimConnectorError("REST request failed", 500)

    def get(self, function, url_params=""):
        url = self.__base_url + url_params + function[self.__FUNCTION_MAP_POS]
        try:
            return requests.get(url + url_ext)
        except:
            raise WimConnectorError("REST request failed", 500)


class DpbConnector(WimConnector):
    """ Use the DPB to establish multipoint connections """

    __LOGGER_NAME = "openmano.wimconn.dpb"
    __SUPPORTED_SERV_TYPES = ["ELAN (L2)", "ELINE (L2)"]
    __SUPPORTED_CONNECTION_TYPES = ["REST", "SSH"]
    __SUPPORTED_SSH_AUTH_TYPES = ["KEY", "PASS"]
    __SUPPORTED_SSH_KEY_TYPES = ["ECDSA", "RSA"]
    __STATUS_MAP = {
        "ACTIVE": "ACTIVE",
        "ACTIVATING": "BUILD",
        "FAILED": "ERROR"}
    __ACTIONS_MAP = {
        "CREATE": ("create-service", "new-service"),
        "DEFINE": ("define", "define-service"),
        "ACTIVATE": ("activate", "activate-service"),
        "RELEASE": ("release", "release-service"),
        "DEACTIVATE": ("deactivate", "deactivate-service"),
        "CHECK": ("await-status", "await-service-status"),
        "GET": ("services", "NOT IMPLEMENTED")
    }

    def __init__(self, wim, wim_account, config):
        self.logger = logging.getLogger(self.__LOGGER_NAME)
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG) #FIXME: Remove when testing complete

        self.__wim = wim
        self.__account = wim_account
        self.__config = config
        self.__cli_config = self.__account.pop("config", None)

        self.__url = self.__wim.get("wim_url", "")
        self.__password = self.__account.get("passwd", "")
        self.__username = self.__account.get("user", "")
        self.__network = self.__cli_config.get("network", "")
        self.__connection_type = self.__cli_config.get("connection_type", "REST")
        self.__port = self.__cli_config.get("port", (80 if self.__connection_type == "REST" else 22))
        self.__ssh_auth = self.__cli_config.get("ssh_auth", None)

        if self.__connection_type == "SSH":
            interface = DpbSshInterface(self.__account,
                                        self.__url,
                                        self.__port,
                                        self.__network,
                                        self.__ssh_auth,
                                        self.__LOGGER_NAME)
        elif self.__connection_type == "REST":
            interface = DpbRestInterface(self.__account,
                                         self.__url,
                                         self.__port,
                                         self.__network,
                                         self.__LOGGER_NAME)
        else:
            raise WimConnectorError("Connection type not supported", 400)
            exit(1)
        self.__post = interface.post
        self.__get = interface.get
        self.logger.info("DPB WimConn Init OK")

    def check_credentials(self):
        self.logger.debug("Credentials checked.....(lies)")
        
    def create_connectivity_service(self, service_type, connection_points, **kwargs):
        self.logger.info("CREATING CONNECTIVITY SERVICE")
        #self.__check_service(service_type, connection_points, kwargs)
        response = self.__post(self.__ACTIONS_MAP.get("CREATE"))
        if "service-id" in response:
            service_id = int(response.get("service-id"))
        else:
            raise WimConnectorError("Invalid create service response", 500)
        data = {"segment": []}
        for point in connection_points:
            data["segment"].append({
                "terminal-name": point.get("service_endpoint_id"),
                "label": int((point.get("service_endpoint_encapsulation_info")).get("vlan")),
                "ingress-bw": 10.0,
                "egress-bw": 10.0})
                #"ingress-bw": (bandwidth.get(point.get("service_endpoint_id"))).get("ingress"),
                #"egress-bw": (bandwidth.get(point.get("service_endpoint_id"))).get("egress")}
        self.__post(self.__ACTIONS_MAP.get("DEFINE"), "/service/"+str(service_id), data, get_response=False)
        self.__post(self.__ACTIONS_MAP.get("ACTIVATE"), "/service/"+str(service_id), get_response=False)
        self.logger.info("CREATED CONNECTIVITY SERVICE")
        return (str(service_id), None)

    def get_connectivity_service_status(self, service_uuid, conn_info=None):
        self.logger.info("CHECKING CONNECTIVITY SERVICE STATUS")
        data = {
            "timeout-millis": 10000,
            "acceptable": ["ACTIVE", "FAILED"]
        }
        response = self.__post(self.__ACTIONS_MAP.get("CHECK"), "/service/"+service_uuid, data)
        if "status" in response:
            status = response.get("status", None)
            self.logger.info("CHECKED CONNECTIVITY SERVICE STATUS")
            return {"wim_status": self.__STATUS_MAP.get(status)}
        else:
            raise WimConnectorError("Invalid status check response", 500)

    def delete_connectivity_service(self, service_uuid, conn_info=None):
        self.__post(self.__ACTIONS_MAP.get("DEACTIVATE"), "/service/"+service_id)
        self.__post(self.__ACTIONS_MAP.get("RELEASE"), "/service/"+service_id)

    def edit_connectivity_service(self, service_uuid, conn_info=None,
                                  connection_points=None, **kwargs):
        self.logger.debug("Can't edit service")

    def clear_all_connectivity_services(self):
        services = self.__get(self.__ACTIONS_MAP.get("GET"))
        self.logger.debug("Can't clear all services")

    def __check_service(self, serv_type, points, kwargs):
        if not serv_type in self.__SUPPORTED_SERV_TYPES:
            raise WimConnectorError("Service type no supported", 400)
        #
        # Check for bandwidth here
        #