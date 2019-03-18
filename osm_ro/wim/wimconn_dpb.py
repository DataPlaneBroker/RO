# -*- coding: utf-8 -*-
##
# Copyright 2019 University of Lancaster - High Performance Networks Research
# Group
# All Rights Reserved.
#
# Contributors: Paul McCherry. Will Fantom
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

import requests
import json
import logging
from enum import Enum

from wimconn import WimConnector, WimConnectorError

class DpbConnector(WimConnector):
    __supported_service_types = ["ELAN (L2)"]
    __supported_encapsulation_types = ["dot1q"]
    __WIM_LOGGER = 'openmano.wimconn.dumb'
    __ENCAPSULATION_TYPE_PARAM = "service_endpoint_encapsulation_type"
    __ENCAPSULATION_INFO_PARAM = "service_endpoint_encapsulation_info"
    __BACKUP_PARAM = "backup"
    __BANDWIDTH_PARAM = "bandwidth"
    __SERVICE_ENDPOINT_PARAM = "service_endpoint_id"
    __WAN_SERVICE_ENDPOINT_PARAM = "wan_service_endpoint_id"
    __WAN_MAPPING_INFO_PARAM = "wan_service_mapping_info"
    __SW_ID_PARAM = "wan_switch_dpid"
    __SW_PORT_PARAM = "wan_switch_port"
    __VLAN_PARAM = "vlan"

    # Public functions exposed to the Resource Orchestrator
    def __init__(self, wim, wim_account, config):
        self.logger = logging.getLogger(self.__WIM_LOGGER)
        self.__wim = wim
        self.__wim_account = wim_account
        self.__config = config
        self.__wim_url = self.__wim.get("wim_url")
        self.__user = wim_account.get("user")
        self.__passwd = wim_account.get("passwd")
        self.logger.info("Initialized OK")

    def create_connectivity_service(self,
                                    service_type,
                                    connection_points,
                                    **kwargs):
        self.__check_service(service_type, connection_points, kwargs)
        self.logger.info("CCS - ST: %s, CP: %s, KWA: %s", service_type, connection_points, kwargs)
        return ("1234567890123456", None)

    def edit_connectivity_service(self, service_uuid,
                                  conn_info, connection_points,
                                  **kwargs):
        self.__exception(WimError.UNSUPPORTED_FEATURE, http_code=501)

    def get_connectivity_service_status(self, service_uuid):
        self.logger.info("SERVICE STATUS CHECKED | UUID: %s", service_uuid)
        return  {'wim_status': 'ACTIVE'}

    def delete_connectivity_service(self, service_uuid, conn_info):
        self.logger.info("Service with uuid: {} deleted".format(service_uuid))

    def clear_all_connectivity_services(self):
        self.logger.info("CLEAR ALL CON SERV")

    def check_connectivity(self):
        self.logger.info("Connectivity checked")

    def check_credentials(self):
        self.logger.info("Credentials checked")

    # Private functions
    def __exception(self, x, **kwargs):
        http_code = kwargs.get("http_code")
        if hasattr(x, "value"):
            error = x.value
        else:
            error = x
        self.logger.error(error)
        raise WimConnectorError(error, http_code=http_code)

    def __check_service(self, service_type, connection_points, kwargs):
        self.logger.info("SERVICED CHECKED")
