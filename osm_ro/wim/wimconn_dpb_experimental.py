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
# Neither the name of the University of Bristol nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
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
import sys
import operator
from enum import Enum

from wimconn import WimConnector, WimConnectorError

try:
    import paramiko
except:
    exit("Install Paramiko [pip install paramiko]")


##
# TODO list
# - Meet PEP8 guidelines (mainly in line len + comments)
# - Fix HTTP error codes
# - Add bandwidth as an option somewhere (currently just 10.0)
##

class WimError(Enum):
    ''' Error definitions for the DPB Wim Connector '''

    UNSUPPORTED_FEATURE = "Unsupported feature"
    INVALID_WIM_RESPONSE = "WIM returned an invalid response"
    WIM_CONNECT_FAIL = "Failed to connect to the WIM"
    WIM_AUTH_ERROR = "Could not authorize WIM connection"
    SSH_BAD_HOST_KEY = "Failed to add SSH host key"
    NETWORK_SELECT_ERROR = "Could not modify given network name"
    SSH_KEY_ERROR = "Could not get SSH key"
    SSH_PORT = "Could not get SSH port"
    NETWORK_NAME = "Could not get network name"

    INVALID_RESPONSE = "Response from WIM is invalid"
    MALFORMED_RESPONSE = "Response from WIM does not contain session and/or content"
    WRONG_SESSION = "Response for incorrect session parsed"
    RESPONSE_ERROR = "WIM response contained an error (see log)"
    INVALID_RESPONSE_FIELD = "WIM response content did not contain desired fields"

    INVALID_SSH_RESPONSE = "Response from SSH could not be read"
    SSH_SEND_FAIL = "Failed to write on SSH channel"

    SERVICE_RELEASE_FAIL = "Failed to release the service on the DPB"
    SERVICE_ACTIVATE_FAIL = "Failed to activate service on the DPB"


class DpbConnector(WimConnector):
    ''' Connect points via the DPB '''

    __LOGGER_CHANNEL = "openmano.wimconn.dpb"
    __STATUS_MAP = {"ACTIVATED": "ACTIVE",
                    "ACTIVATING": "BUILD",
                    "FAILED": "ERROR"}

    def __init__(self, wim, wim_account, config=None, logger=None):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # FIXME: Remove when done with tests
        self.logger = logger or logging.getLogger(self.__LOGGER_CHANNEL)

        self.__wim = wim
        self.__wim_account = wim_account
        self.__config = config or {}
        self.__cli_config = self.__wim_account.get("config") or {}
        self.__ssh_pkey = None
        self.__unclaimed_responses = []
        self.__check_auth_data()

        self.logger.info("DPB Connector Initialized")

    def check_credentials(self):
        ''' Check if connector can connect to the DPB via SSH '''
        ssh_client = self.__create_ssh_client(".credcheck")
        try:
            self.__ssh_connect(ssh_client)
        except paramiko.BadHostKeyException:
            self.__exception(WimError.SSH_BAD_HOST_KEY, 500)
        except paramiko.AuthenticationException:
            self.__exception(WimError.WIM_AUTH_ERROR, 401)
        except:
            self.__exception(WimError.WIM_CONNECT_FAIL, 500)
        finally:
            ssh_client.close()

    def get_connectivity_service_status(self, service_uuid, conn_info=None):
        ''' Monitor the status of a service on the WIM (watch-service) '''
        ssh_client = self.__create_ssh_client(".statuscheck")
        try:
            stdin, stdout = self.__ssh_connect(ssh_client)
        except:
            ssh_client.close()
            self.__exception(WimError.WIM_CONNECT_FAIL, 500)

        try:
            session_id = random.randint(101, 200)
            content = {"type": "await-service-status",
                       "service-id": int(service_uuid),
                       "timeout-millis": 10000,
                       "acceptable": ["ACTIVATED", "ACTIVATING", "FAILED"]}
            request = self.__build_request(session_id, content)
            self.__ssh_request(stdin, request)
            response = self.__ssh_response(stdout, session_id)
            response = self.__parse_response(session_id, response, requirements=["status"])
            status = str(response.get("status"))
            self.logger.debug("Connectivity status checked | " + status)
            return {"wim_status": self.__STATUS_MAP.get(status)}
        except:
            raise
        finally:
            ssh_client.close()

    def create_connectivity_service(self, service_type, connection_points,
                                    **kwargs):
        ssh_client = self.__create_ssh_client(".createservice")
        try:
            stdin, stdout = self.__ssh_connect(ssh_client)
        except:
            ssh_client.close()
            raise

        try:
            session_id = random.randint(0, 100)
            content = {"type": "new-service"}
            request = self.__build_request(session_id, content)
            self.__ssh_request(stdin, request)
            response = self.__ssh_response(stdout, session_id)
            response = self.__parse_response(session_id, response, requirements=["service-id"])
            service_id = response.get("service-id")
            self.logger.debug("New service created | " + str(service_id))
        except:
            ssh_client.close()
            raise

        try:
            session_id += 1
            segments = []
            for point in connection_points:
                segments.append({"terminal-name": point.get("service_endpoint_id"),
                                 "label": int((point.get("service_endpoint_encapsulation_info")).get("vlan")),
                                 "ingress-bw": 10.0,
                                 "egress-bw": 10.0})
                # "ingress-bw": (bandwidth.get(point.get("service_endpoint_id"))).get("ingress"),
                # "egress-bw": (bandwidth.get(point.get("service_endpoint_id"))).get("egress")}
            content = {"type": "define-service",
                       "service-id": service_id,
                       "segment": segments}
            request = self.__build_request(session_id, content)
            self.__ssh_request(stdin, request)
            response = self.__ssh_response(stdout, session_id)
            response = self.__parse_response(session_id, response)
            self.logger.debug("New service defined | " + str(service_id))
        except:
            ssh_client.close()
            raise

        try:
            session_id += 1
            content = {"type": "activate-service",
                       "service-id": int(service_id)}
            request = self.__build_request(session_id, content)
            self.__ssh_request(stdin, request)
            response = self.__ssh_response(stdout, session_id)
            response = self.__parse_response(session_id, response)
            if not response == {}:
                self.__exception(WimError.SERVICE_ACTIVATE_FAIL, 500)
            self.logger.debug("Service activated | " + str(service_id))
        except:
            raise
        finally:
            ssh_client.close()
        return (str(service_id), None)

    def delete_connectivity_service(self, service_uuid, conn_info=None):
        ssh_client = self.__create_ssh_client(".deleteservice")
        try:
            stdin, stdout = self.__ssh_connect(ssh_client)
        except:
            ssh_client.close()
            self.__exception(WimError.WIM_CONNECT_FAIL, 500)

        try:
            session_id = random.randint(101, 200)
            content = {"type": "release-service",
                       "service-id": int(service_uuid)}
            request = self.__build_request(session_id, content)
            self.__ssh_request(stdin, request)
            response = self.__ssh_response(stdout, session_id)
            response = self.__parse_response(session_id, response)
            if not response == {}:
                self.__exception(WimError.SERVICE_RELEASE_FAIL, 500)
            self.logger.debug("Service released (deleted) | " + str(service_id))
        except:
            raise
        finally:
            ssh_client.close()

    ## Private Request Builds and Parsing
    def __build_request(self, session_id, content):
        ''' Build a request body for the wim '''
        if not isinstance(session_id, int) or not isinstance(content, dict):
            self.__exception(WimError.BAD_REQUEST_DATA, 400)
        body = {
            "session": session_id,
            "content": content
        }
        return body

    def __parse_response(self, session_id, response, requirements=[]):
        ''' Parse a response from DPB '''
        if not isinstance(response, dict):
            self.__exception(WimError.INVALID_RESPONSE, 500)
        if not "session" in response or not "content" in response:
            self.__exception(WimError.MALFORMED_RESPONSE, 500)
        if not isinstance(response.get("session"), int) or not isinstance(response.get("content"), dict):
            self.__exception(WimError.MALFORMED_RESPONSE, 500)
        if int(session_id) != int(response.get("session")):
            self.__unclaimed_responses.append(response)
            self.__exception(WimError.WRONG_SESSION, 500)
        response = response.get("content")
        if "error" in response:
            self.logger.error(response.get("error"))
            if "msg" in response:
                self.logger.error(response.get("msg"))
            self.__exception(WimError.RESPONSE_ERROR, 500)
        for req in requirements:
            if not req in response:
                self.__exception(WimError.INVALID_RESPONSE_FIELD, 500)
        return response

    ## Private SSH Methods
    def __create_key(self, pkey_file=None, pkey_str=None):
        ''' Create a private key object from a string or file '''
        try:
            if pkey_file:
                return paramiko.ECDSAKey.from_private_key_file(filename=pkey_file)
            if pkey_str:
                return paramiko.PKey(data=pkey_str)
        except IOError:
            self.__exception(WimError.SSH_KEY_ERROR, 500)
        except paramiko.SSHException:
            self.__exception(WimError.SSH_KEY_ERROR, 500)
        except paramiko.PasswordRequiredException:
            self.__exception(WimError.SSH_KEY_ERROR, 500)

    def __create_ssh_client(self, logger_name=None):
        ''' Create a paramiko SSH Client to connect to the WIM '''
        ssh_client = paramiko.SSHClient()
        ssh_client.set_log_channel(self.__LOGGER_CHANNEL
                                   + (logger_name or "paramiko"))
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return ssh_client

    def __ssh_connect(self, ssh_client):
        ''' Connect the client to the WIM via SSH '''
        ssh_client.connect(hostname=self.__wim.get("wim_url"),
                           port=self.__cli_config.get("ssh_port"),
                           username=self.__wim_account.get("user"),
                           pkey=self.__ssh_pkey,
                           look_for_keys=False,
                           compress=False)
        stdin, stdout, stderr = ssh_client.exec_command(command=self.__cli_config.get("network_name"))
        response = self.__ssh_response(stdout)
        if ("error" in response) or (not "network-name" in response):
            self.__exception(WimError.NETWORK_SELECT_ERROR, 404)
        if not response.get("network-name") == self.__cli_config.get("network_name"):
            self.__exception(WimError.INVALID_WIM_RESPONSE, 404)
        return stdin, stdout

    def __ssh_response(self, stdout, session_id=None):
        ''' Gets a response on the std channel or unclaimed list '''
        if session_id:
            for response in self.__unclaimed_responses:
                if int(response.get("session")) == int(session_id):
                    self.__unclaimed_responses.remove(response)
                    return response.get("content")
        try:
            response_head = struct.unpack(">I", stdout.read(4))[0]
            print(response_head)
            response_payload = struct.unpack(str(response_head) + "s", stdout.read(int(response_head)))[0]
            print(response_payload)
            response = json.loads(response_payload)
            return response
        except:
            self.__exception(WimError.INVALID_SSH_RESPONSE, 500)

    def __ssh_request(self, stdin, request_body):
        ''' Send a request to the WIM over SSH '''
        try:
            request_body = json.dumps(request_body).encode("utf-8")
            packed_request = struct.pack(">I" + str(len(request_body)) + "s", len(request_body), request_body)
            print(request_body)
            stdin.write(packed_request)
        except:
            self.__exception(WimError.SSH_SEND_FAIL, 500)

    ## Private Validation Methods
    def __exception(self, ex, code):
        if hasattr(ex, "value"):
            error = ex.value
        else:
            error = ex
        self.logger.error(error)
        raise WimConnectorError(error, http_code=code)

    def __get_from_response(self, response, requirement):
        try:
            return reduce(operator.getitem, requirement, response)
        except:
            self.__exception(WimError.INVALID_WIM_RESPONSE, 500)

    def __check_auth_data(self):
        if not "ssh_port" in self.__cli_config:
            self.__exception(WimError.SSH_PORT, 400)
        if not isinstance(self.__cli_config.get("ssh_port"), int):
            self.__exception(WimError.SSH_PORT, 400)
        if not "network_name" in self.__cli_config:
            self.__exception(WimError.NETWORK_NAME, 400)
        if not isinstance(self.__cli_config.get("network_name"), str):
            self.__exception(WimError.NETWORK_NAME, 400)
        if not "pkey_file" in self.__cli_config and not "pkey" in self.__cli_config:
            self.__exception(WimError.SSH_KEY_ERROR, 500)
        self.__ssh_pkey = self.__create_key(pkey_file=self.__cli_config.get("pkey_file"),
                                            pkey_str=self.__cli_config.get("pkey"))
