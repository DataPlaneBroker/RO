# -*- coding: utf-8 -*-

##
# Copyright 2015 Telefonica Investigacion y Desarrollo, S.A.U.
# This file is part of openvim
# All Rights Reserved.
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
# contact with: nfvlabs@tid.es
##

""""
This is thread that interacts with a VIM. It processes TASKs sequentially against a single VIM.
The tasks are stored at database in table vim_wim_actions
The task content is (M: stored at memory, D: stored at database):
    MD  instance_action_id:  reference a global action over an instance-scenario: database instance_actions
    MD  task_index:     index number of the task. This together with the previous forms a unique key identifier
    MD  datacenter_vim_id:  should contain the uuid of the VIM managed by this thread
    MD  vim_id:     id of the vm,net,etc at VIM
    MD  action:     CREATE, DELETE, FIND
    MD  item:       database table name, can be instance_vms, instance_nets, TODO: datacenter_flavors, datacenter_images
    MD  item_id:    uuid of the referenced entry in the previous table
    MD  status:     SCHEDULED,BUILD,DONE,FAILED,SUPERSEDED
    MD  extra:      text with yaml format at database, dict at memory with:
            params:     list with the params to be sent to the VIM for CREATE or FIND. For DELETE the vim_id is taken from other related tasks
            find:       (only for CREATE tasks) if present it should FIND before creating and use if existing. Contains the FIND params
            depends_on: list with the 'task_index'es of tasks that must be completed before. e.g. a vm creation depends on a net creation
                        can contain an int (single index on the same instance-action) or str (compete action ID)
            sdn_net_id: used for net.
            tries:
            interfaces: used for VMs. Each key is the uuid of the instance_interfaces entry at database
                iface_id: uuid of intance_interfaces
                sdn_port_id:
                sdn_net_id:
            created_items: dictionary with extra elements created that need to be deleted. e.g. ports, volumes,...
            created:    False if the VIM element is not created by other actions, and it should not be deleted
            vim_status: VIM status of the element. Stored also at database in the instance_XXX
    M   depends:    dict with task_index(from depends_on) to task class
    M   params:     same as extra[params] but with the resolved dependencies
    M   vim_interfaces: similar to extra[interfaces] but with VIM information. Stored at database in the instance_XXX but not at vim_wim_actions
    M   vim_info:   Detailed information of a vm,net from the VIM. Stored at database in the instance_XXX but not at vim_wim_actions
    MD  error_msg:  descriptive text upon an error.Stored also at database instance_XXX
    MD  created_at: task creation time
    MD  modified_at: last task update time. On refresh it contains when this task need to be refreshed

"""

import threading
import time
import Queue
import logging
import vimconn
import vimconn_openvim
import vimconn_aws
import vimconn_opennebula
import vimconn_openstack
import vimconn_vmware
import yaml
from db_base import db_base_Exception
from lib_osm_openvim.ovim import ovimException
from copy import deepcopy

__author__ = "Alfonso Tierno, Pablo Montes"
__date__ = "$28-Sep-2017 12:07:15$"

vim_module = {
    "openvim": vimconn_openvim,
    "aws": vimconn_aws,
    "opennebula": vimconn_opennebula,
    "openstack": vimconn_openstack,
    "vmware": vimconn_vmware,
}


def is_task_id(task_id):
    return task_id.startswith("TASK-")


class VimThreadException(Exception):
    pass


class VimThreadExceptionNotFound(VimThreadException):
    pass


class vim_thread(threading.Thread):
    REFRESH_BUILD = 5  # 5 seconds
    REFRESH_ACTIVE = 60  # 1 minute

    def __init__(self, task_lock, name=None, datacenter_name=None, datacenter_tenant_id=None,
                 db=None, db_lock=None, ovim=None):
        """Init a thread.
        Arguments:
            'id' number of thead
            'name' name of thread
            'host','user':  host ip or name to manage and user
            'db', 'db_lock': database class and lock to use it in exclusion
        """
        threading.Thread.__init__(self)
        self.vim = None
        self.error_status = None
        self.datacenter_name = datacenter_name
        self.datacenter_tenant_id = datacenter_tenant_id
        self.ovim = ovim
        if not name:
            self.name = vimconn["id"] + "." + vimconn["config"]["datacenter_tenant_id"]
        else:
            self.name = name
        self.vim_persistent_info = {}

        self.logger = logging.getLogger('openmano.vim.' + self.name)
        self.db = db
        self.db_lock = db_lock

        self.task_lock = task_lock
        self.task_queue = Queue.Queue(2000)

        self.refresh_tasks = []
        """Contains time ordered task list for refreshing the status of VIM VMs and nets"""

        self.pending_tasks = []
        """Contains time ordered task list for creation, deletion of VIM VMs and nets"""

        self.grouped_tasks = {}
        """ It contains all the creation/deletion pending tasks grouped by its concrete vm, net, etc
            <item><item_id>:
                -   <task1>  # e.g. CREATE task
                    <task2>  # e.g. DELETE task
        """

    def get_vimconnector(self):
        try:
            from_ = "datacenter_tenants as dt join datacenters as d on dt.datacenter_id=d.uuid"
            select_ = ('type', 'd.config as config', 'd.uuid as datacenter_id', 'vim_url', 'vim_url_admin',
                       'd.name as datacenter_name', 'dt.uuid as datacenter_tenant_id',
                       'dt.vim_tenant_name as vim_tenant_name', 'dt.vim_tenant_id as vim_tenant_id',
                       'user', 'passwd', 'dt.config as dt_config')
            where_ = {"dt.uuid": self.datacenter_tenant_id}
            vims = self.db.get_rows(FROM=from_, SELECT=select_, WHERE=where_)
            vim = vims[0]
            vim_config = {}
            if vim["config"]:
                vim_config.update(yaml.load(vim["config"]))
            if vim["dt_config"]:
                vim_config.update(yaml.load(vim["dt_config"]))
            vim_config['datacenter_tenant_id'] = vim.get('datacenter_tenant_id')
            vim_config['datacenter_id'] = vim.get('datacenter_id')

            # get port_mapping
            with self.db_lock:
                vim_config["wim_external_ports"] = self.ovim.get_of_port_mappings(
                    db_filter={"region": vim_config['datacenter_id'], "pci": None})

            self.vim = vim_module[vim["type"]].vimconnector(
                uuid=vim['datacenter_id'], name=vim['datacenter_name'],
                tenant_id=vim['vim_tenant_id'], tenant_name=vim['vim_tenant_name'],
                url=vim['vim_url'], url_admin=vim['vim_url_admin'],
                user=vim['user'], passwd=vim['passwd'],
                config=vim_config, persistent_info=self.vim_persistent_info
            )
            self.error_status = None
        except Exception as e:
            self.logger.error("Cannot load vimconnector for vim_account {}: {}".format(self.datacenter_tenant_id, e))
            self.vim = None
            self.error_status = "Error loading vimconnector: {}".format(e)

    def _reload_vim_actions(self):
        """
        Read actions from database and reload them at memory. Fill self.refresh_list, pending_list, vim_actions
        :return: None
        """
        try:
            action_completed = False
            task_list = []
            old_action_key = None

            old_item_id = ""
            old_item = ""
            old_created_at = 0.0
            database_limit = 200
            while True:
                # get 200 (database_limit) entries each time
                vim_actions = self.db.get_rows(FROM="vim_wim_actions",
                                                WHERE={"datacenter_vim_id": self.datacenter_tenant_id,
                                                        "item_id>=": old_item_id},
                                                ORDER_BY=("item_id", "item", "created_at",),
                                                LIMIT=database_limit)
                for task in vim_actions:
                    item = task["item"]
                    item_id = task["item_id"]

                    # skip the first entries that are already processed in the previous pool of 200
                    if old_item_id:
                        if item_id == old_item_id and item == old_item and task["created_at"] == old_created_at:
                            old_item_id = False  # next one will be a new un-processed task
                        continue

                    action_key = item + item_id
                    if old_action_key != action_key:
                        if not action_completed and task_list:
                            # This will fill needed task parameters into memory, and insert the task if needed in
                            # self.pending_tasks or self.refresh_tasks
                            try:
                                self._insert_pending_tasks(task_list)
                            except Exception as e:
                                self.logger.critical(
                                    "Unexpected exception at _reload_vim_actions:_insert_pending_tasks: " + str(e),
                                    exc_info=True)
                        task_list = []
                        old_action_key = action_key
                        action_completed = False
                    elif action_completed:
                        continue

                    if task["status"] == "SCHEDULED" or task["action"] == "CREATE" or task["action"] == "FIND":
                        task_list.append(task)
                    elif task["action"] == "DELETE":
                        # action completed because deleted and status is not SCHEDULED. Not needed anything
                        action_completed = True
                if len(vim_actions) == database_limit:
                    # update variables for get the next database iteration
                    old_item_id = item_id
                    old_item = item
                    old_created_at = task["created_at"]
                else:
                    break
            # Last actions group need to be inserted too
            if not action_completed and task_list:
                try:
                    self._insert_pending_tasks(task_list)
                except Exception as e:
                    self.logger.critical("Unexpected exception at _reload_vim_actions:_insert_pending_tasks: " + str(e),
                                         exc_info=True)
            self.logger.debug("reloaded vim actions pending:{} refresh:{}".format(
                len(self.pending_tasks), len(self.refresh_tasks)))
        except Exception as e:
            self.logger.critical("Unexpected exception at _reload_vim_actions: " + str(e), exc_info=True)

    def _refres_elements(self):
        """Call VIM to get VMs and networks status until 10 elements"""
        now = time.time()
        nb_processed = 0
        vm_to_refresh_list = []
        net_to_refresh_list = []
        vm_to_refresh_dict = {}
        net_to_refresh_dict = {}
        items_to_refresh = 0
        while self.refresh_tasks:
            task = self.refresh_tasks[0]
            with self.task_lock:
                if task['status'] == 'SUPERSEDED':
                    self.refresh_tasks.pop(0)
                    continue
                if task['modified_at'] > now:
                    break
                # task["status"] = "processing"
                nb_processed += 1
            self.refresh_tasks.pop(0)
            if task["item"] == 'instance_vms':
                if task["vim_id"] not in vm_to_refresh_dict:
                    vm_to_refresh_dict[task["vim_id"]] = [task]
                    vm_to_refresh_list.append(task["vim_id"])
                else:
                    vm_to_refresh_dict[task["vim_id"]].append(task)
            elif task["item"] == 'instance_nets':
                if task["vim_id"] not in net_to_refresh_dict:
                    net_to_refresh_dict[task["vim_id"]] = [task]
                    net_to_refresh_list.append(task["vim_id"])
                else:
                    net_to_refresh_dict[task["vim_id"]].append(task)
            else:
                task_id = task["instance_action_id"] + "." + str(task["task_index"])
                self.logger.critical("task={}: unknown task {}".format(task_id, task["item"]), exc_info=True)
            items_to_refresh += 1
            if items_to_refresh == 10:
                break

        if vm_to_refresh_list:
            now = time.time()
            try:
                vim_dict = self.vim.refresh_vms_status(vm_to_refresh_list)
            except vimconn.vimconnException as e:
                # Mark all tasks at VIM_ERROR status
                self.logger.error("task=several get-VM: vimconnException when trying to refresh vms " + str(e))
                vim_dict = {}
                for vim_id in vm_to_refresh_list:
                    vim_dict[vim_id] = {"status": "VIM_ERROR", "error_msg": str(e)}

            for vim_id, vim_info in vim_dict.items():

                # look for task
                for task in vm_to_refresh_dict[vim_id]:
                    task_need_update = False
                    task_id = task["instance_action_id"] + "." + str(task["task_index"])
                    self.logger.debug("task={} get-VM: vim_vm_id={} result={}".format(task_id, task["vim_id"], vim_info))

                    # check and update interfaces
                    task_warning_msg = ""
                    for interface in vim_info.get("interfaces", ()):
                        vim_interface_id = interface["vim_interface_id"]
                        if vim_interface_id not in task["extra"]["interfaces"]:
                            self.logger.critical("task={} get-VM: Interface not found {} on task info {}".format(
                                task_id, vim_interface_id, task["extra"]["interfaces"]), exc_info=True)
                            continue
                        task_interface = task["extra"]["interfaces"][vim_interface_id]
                        task_vim_interface = task["vim_interfaces"].get(vim_interface_id)
                        if task_vim_interface != interface:
                            # delete old port
                            if task_interface.get("sdn_port_id"):
                                try:
                                    with self.db_lock:
                                        self.ovim.delete_port(task_interface["sdn_port_id"], idempotent=True)
                                        task_interface["sdn_port_id"] = None
                                        task_need_update = True
                                except ovimException as e:
                                    error_text = "ovimException deleting external_port={}: {}".format(
                                        task_interface["sdn_port_id"], e)
                                    self.logger.error("task={} get-VM: {}".format(task_id, error_text), exc_info=True)
                                    task_warning_msg += error_text
                                    # TODO Set error_msg at instance_nets instead of instance VMs

                            # Create SDN port
                            sdn_net_id = task_interface.get("sdn_net_id")
                            if sdn_net_id and interface.get("compute_node") and interface.get("pci"):
                                sdn_port_name = sdn_net_id + "." + task["vim_id"]
                                sdn_port_name = sdn_port_name[:63]
                                try:
                                    with self.db_lock:
                                        sdn_port_id = self.ovim.new_external_port(
                                            {"compute_node": interface["compute_node"],
                                                "pci": interface["pci"],
                                                "vlan": interface.get("vlan"),
                                                "net_id": sdn_net_id,
                                                "region": self.vim["config"]["datacenter_id"],
                                                "name": sdn_port_name,
                                                "mac": interface.get("mac_address")})
                                        task_interface["sdn_port_id"] = sdn_port_id
                                        task_need_update = True
                                except (ovimException, Exception) as e:
                                    error_text = "ovimException creating new_external_port compute_node={}" \
                                                 " pci={} vlan={} {}".format(
                                        interface["compute_node"],
                                        interface["pci"],
                                        interface.get("vlan"), e)
                                    self.logger.error("task={} get-VM: {}".format(task_id, error_text), exc_info=True)
                                    task_warning_msg += error_text
                                    # TODO Set error_msg at instance_nets instead of instance VMs

                            self.db.update_rows(
                                'instance_interfaces',
                                UPDATE={"mac_address": interface.get("mac_address"),
                                        "ip_address": interface.get("ip_address"),
                                        "vim_interface_id": interface.get("vim_interface_id"),
                                        "vim_info": interface.get("vim_info"),
                                        "sdn_port_id": task_interface.get("sdn_port_id"),
                                        "compute_node": interface.get("compute_node"),
                                        "pci": interface.get("pci"),
                                        "vlan": interface.get("vlan")},
                                WHERE={'uuid': task_interface["iface_id"]})
                            task["vim_interfaces"][vim_interface_id] = interface

                    # check and update task and instance_vms database
                    vim_info_error_msg = None
                    if vim_info.get("error_msg"):
                        vim_info_error_msg = self._format_vim_error_msg(vim_info["error_msg"] + task_warning_msg)
                    elif task_warning_msg:
                        vim_info_error_msg = self._format_vim_error_msg(task_warning_msg)
                    task_vim_info = task.get("vim_info")
                    task_error_msg = task.get("error_msg")
                    task_vim_status = task["extra"].get("vim_status")
                    if task_vim_status != vim_info["status"] or task_error_msg != vim_info_error_msg or \
                            (vim_info.get("vim_info") and task_vim_info != vim_info["vim_info"]):
                        temp_dict = {"status": vim_info["status"], "error_msg": vim_info_error_msg}
                        if vim_info.get("vim_info"):
                            temp_dict["vim_info"] = vim_info["vim_info"]
                        self.db.update_rows('instance_vms', UPDATE=temp_dict, WHERE={"uuid": task["item_id"]})
                        task["extra"]["vim_status"] = vim_info["status"]
                        task["error_msg"] = vim_info_error_msg
                        if vim_info.get("vim_info"):
                            task["vim_info"] = vim_info["vim_info"]
                        task_need_update = True

                    if task_need_update:
                        self.db.update_rows(
                            'vim_wim_actions',
                            UPDATE={"extra": yaml.safe_dump(task["extra"], default_flow_style=True, width=256),
                                    "error_msg": task.get("error_msg"), "modified_at": now},
                            WHERE={'instance_action_id': task['instance_action_id'],
                                    'task_index': task['task_index']})
                    if task["extra"].get("vim_status") == "BUILD":
                        self._insert_refresh(task, now + self.REFRESH_BUILD)
                    else:
                        self._insert_refresh(task, now + self.REFRESH_ACTIVE)

        if net_to_refresh_list:
            now = time.time()
            try:
                vim_dict = self.vim.refresh_nets_status(net_to_refresh_list)
            except vimconn.vimconnException as e:
                # Mark all tasks at VIM_ERROR status
                self.logger.error("task=several get-net: vimconnException when trying to refresh nets " + str(e))
                vim_dict = {}
                for vim_id in net_to_refresh_list:
                    vim_dict[vim_id] = {"status": "VIM_ERROR", "error_msg": str(e)}

            for vim_id, vim_info in vim_dict.items():
                # look for task
                for task in net_to_refresh_dict[vim_id]:
                    task_id = task["instance_action_id"] + "." + str(task["task_index"])
                    self.logger.debug("task={} get-net: vim_net_id={} result={}".format(task_id, task["vim_id"], vim_info))

                    task_vim_info = task.get("vim_info")
                    task_vim_status = task["extra"].get("vim_status")
                    task_error_msg = task.get("error_msg")
                    task_sdn_net_id = task["extra"].get("sdn_net_id")

                    vim_info_status = vim_info["status"]
                    vim_info_error_msg = vim_info.get("error_msg")
                    # get ovim status
                    if task_sdn_net_id:
                        try:
                            with self.db_lock:
                                sdn_net = self.ovim.show_network(task_sdn_net_id)
                        except (ovimException, Exception) as e:
                            text_error = "ovimException getting network snd_net_id={}: {}".format(task_sdn_net_id, e)
                            self.logger.error("task={} get-net: {}".format(task_id, text_error), exc_info=True)
                            sdn_net = {"status": "ERROR", "last_error": text_error}
                        if sdn_net["status"] == "ERROR":
                            if not vim_info_error_msg:
                                vim_info_error_msg = str(sdn_net.get("last_error"))
                            else:
                                vim_info_error_msg = "VIM_ERROR: {} && SDN_ERROR: {}".format(
                                    self._format_vim_error_msg(vim_info_error_msg, 1024 // 2 - 14),
                                    self._format_vim_error_msg(sdn_net["last_error"], 1024 // 2 - 14))
                            vim_info_status = "ERROR"
                        elif sdn_net["status"] == "BUILD":
                            if vim_info_status == "ACTIVE":
                                vim_info_status = "BUILD"

                    # update database
                    if vim_info_error_msg:
                        vim_info_error_msg = self._format_vim_error_msg(vim_info_error_msg)
                    if task_vim_status != vim_info_status or task_error_msg != vim_info_error_msg or \
                            (vim_info.get("vim_info") and task_vim_info != vim_info["vim_info"]):
                        task["extra"]["vim_status"] = vim_info_status
                        task["error_msg"] = vim_info_error_msg
                        if vim_info.get("vim_info"):
                            task["vim_info"] = vim_info["vim_info"]
                        temp_dict = {"status": vim_info_status, "error_msg": vim_info_error_msg}
                        if vim_info.get("vim_info"):
                            temp_dict["vim_info"] = vim_info["vim_info"]
                        self.db.update_rows('instance_nets', UPDATE=temp_dict, WHERE={"uuid": task["item_id"]})
                        self.db.update_rows(
                            'vim_wim_actions',
                            UPDATE={"extra": yaml.safe_dump(task["extra"], default_flow_style=True, width=256),
                                    "error_msg": task.get("error_msg"), "modified_at": now},
                            WHERE={'instance_action_id': task['instance_action_id'],
                                    'task_index': task['task_index']})
                    if task["extra"].get("vim_status") == "BUILD":
                        self._insert_refresh(task, now + self.REFRESH_BUILD)
                    else:
                        self._insert_refresh(task, now + self.REFRESH_ACTIVE)

        return nb_processed

    def _insert_refresh(self, task, threshold_time=None):
        """Insert a task at list of refreshing elements. The refreshing list is ordered by threshold_time (task['modified_at']
        It is assumed that this is called inside this thread
        """
        if not self.vim:
            return
        if not threshold_time:
            threshold_time = time.time()
        task["modified_at"] = threshold_time
        task_name = task["item"][9:] + "-" + task["action"]
        task_id = task["instance_action_id"] + "." + str(task["task_index"])
        for index in range(0, len(self.refresh_tasks)):
            if self.refresh_tasks[index]["modified_at"] > threshold_time:
                self.refresh_tasks.insert(index, task)
                break
        else:
            index = len(self.refresh_tasks)
            self.refresh_tasks.append(task)
        self.logger.debug("task={} new refresh name={}, modified_at={} index={}".format(
            task_id, task_name, task["modified_at"], index))

    def _remove_refresh(self, task_name, vim_id):
        """Remove a task with this name and vim_id from the list of refreshing elements.
        It is assumed that this is called inside this thread outside _refres_elements method
        Return True if self.refresh_list is modified, task is found
        Return False if not found
        """
        index_to_delete = None
        for index in range(0, len(self.refresh_tasks)):
            if self.refresh_tasks[index]["name"] == task_name and self.refresh_tasks[index]["vim_id"] == vim_id:
                index_to_delete = index
                break
        else:
            return False
        if not index_to_delete:
            del self.refresh_tasks[index_to_delete]
        return True

    def _proccess_pending_tasks(self):
        nb_created = 0
        nb_processed = 0
        while self.pending_tasks:
            task = self.pending_tasks.pop(0)
            nb_processed += 1
            try:
                # check if tasks that this depends on have been completed
                dependency_not_completed = False
                for task_index in task["extra"].get("depends_on", ()):
                    task_dependency = task["depends"].get("TASK-" + str(task_index))
                    if not task_dependency:
                        task_dependency = self._look_for_task(task["instance_action_id"], task_index)
                        if not task_dependency:
                            raise VimThreadException(
                                "Cannot get depending net task trying to get depending task {}.{}".format(
                                    task["instance_action_id"], task_index))
                        # task["depends"]["TASK-" + str(task_index)] = task_dependency #it references another object,so database must be look again
                    if task_dependency["status"] == "SCHEDULED":
                        dependency_not_completed = True
                        break
                    elif task_dependency["status"] == "FAILED":
                        raise VimThreadException(
                            "Cannot {} {}, (task {}.{}) because depends on failed {}.{}, (task{}.{}): {}".format(
                                task["action"], task["item"],
                                task["instance_action_id"], task["task_index"],
                                task_dependency["instance_action_id"], task_dependency["task_index"],
                                task_dependency["action"], task_dependency["item"], task_dependency.get("error_msg")))
                if dependency_not_completed:
                    # Move this task to the end.
                    task["extra"]["tries"] = task["extra"].get("tries", 0) + 1
                    if task["extra"]["tries"] <= 3:
                        self.pending_tasks.append(task)
                        continue
                    else:
                        raise VimThreadException(
                            "Cannot {} {}, (task {}.{}) because timeout waiting to complete {} {}, "
                            "(task {}.{})".format(task["action"], task["item"],
                                                  task["instance_action_id"], task["task_index"],
                                                  task_dependency["instance_action_id"], task_dependency["task_index"],
                                                  task_dependency["action"], task_dependency["item"]))

                if task["status"] == "SUPERSEDED":
                    # not needed to do anything but update database with the new status
                    result = True
                    database_update = None
                elif not self.vim:
                    task["status"] = "ERROR"
                    task["error_msg"] = self.error_status
                    result = False
                    database_update = {"status": "VIM_ERROR", "error_msg": task["error_msg"]}
                elif task["item"] == 'instance_vms':
                    if task["action"] == "CREATE":
                        result, database_update = self.new_vm(task)
                        nb_created += 1
                    elif task["action"] == "DELETE":
                        result, database_update = self.del_vm(task)
                    else:
                        raise vimconn.vimconnException(self.name + "unknown task action {}".format(task["action"]))
                elif task["item"] == 'instance_nets':
                    if task["action"] == "CREATE":
                        result, database_update = self.new_net(task)
                        nb_created += 1
                    elif task["action"] == "DELETE":
                        result, database_update = self.del_net(task)
                    elif task["action"] == "FIND":
                        result, database_update = self.get_net(task)
                    else:
                        raise vimconn.vimconnException(self.name + "unknown task action {}".format(task["action"]))
                elif task["item"] == 'instance_sfis':
                    if task["action"] == "CREATE":
                        result, database_update = self.new_sfi(task)
                        nb_created += 1
                    elif task["action"] == "DELETE":
                        result, database_update = self.del_sfi(task)
                    else:
                        raise vimconn.vimconnException(self.name + "unknown task action {}".format(task["action"]))
                elif task["item"] == 'instance_sfs':
                    if task["action"] == "CREATE":
                        result, database_update = self.new_sf(task)
                        nb_created += 1
                    elif task["action"] == "DELETE":
                        result, database_update = self.del_sf(task)
                    else:
                        raise vimconn.vimconnException(self.name + "unknown task action {}".format(task["action"]))
                elif task["item"] == 'instance_classifications':
                    if task["action"] == "CREATE":
                        result, database_update = self.new_classification(task)
                        nb_created += 1
                    elif task["action"] == "DELETE":
                        result, database_update = self.del_classification(task)
                    else:
                        raise vimconn.vimconnException(self.name + "unknown task action {}".format(task["action"]))
                elif task["item"] == 'instance_sfps':
                    if task["action"] == "CREATE":
                        result, database_update = self.new_sfp(task)
                        nb_created += 1
                    elif task["action"] == "DELETE":
                        result, database_update = self.del_sfp(task)
                    else:
                        raise vimconn.vimconnException(self.name + "unknown task action {}".format(task["action"]))
                else:
                    raise vimconn.vimconnException(self.name + "unknown task item {}".format(task["item"]))
                    # TODO
            except VimThreadException as e:
                result = False
                task["error_msg"] = str(e)
                task["status"] = "FAILED"
                database_update = {"status": "VIM_ERROR", "error_msg": task["error_msg"]}
                if task["item"] == 'instance_vms':
                    database_update["vim_vm_id"] = None
                elif task["item"] == 'instance_nets':
                    database_update["vim_net_id"] = None

            no_refresh_tasks = ['instance_sfis', 'instance_sfs',
                                'instance_classifications', 'instance_sfps']
            if task["action"] == "DELETE":
                action_key = task["item"] + task["item_id"]
                del self.grouped_tasks[action_key]
            elif task["action"] in ("CREATE", "FIND") and task["status"] in ("DONE", "BUILD"):
                if task["item"] not in no_refresh_tasks:
                    self._insert_refresh(task)

            task_id = task["instance_action_id"] + "." + str(task["task_index"])
            self.logger.debug("task={} item={} action={} result={}:'{}' params={}".format(
                task_id, task["item"], task["action"], task["status"],
                task["vim_id"] if task["status"] == "DONE" else task.get("error_msg"), task["params"]))
            try:
                now = time.time()
                self.db.update_rows(
                    table="vim_wim_actions",
                    UPDATE={"status": task["status"], "vim_id": task.get("vim_id"), "modified_at": now,
                            "error_msg": task["error_msg"],
                            "extra": yaml.safe_dump(task["extra"], default_flow_style=True, width=256)},
                    WHERE={"instance_action_id": task["instance_action_id"], "task_index": task["task_index"]})
                if result is not None:
                    self.db.update_rows(
                        table="instance_actions",
                        UPDATE={("number_done" if result else "number_failed"): {"INCREMENT": 1},
                                "modified_at": now},
                        WHERE={"uuid": task["instance_action_id"]})
                if database_update:
                    self.db.update_rows(table=task["item"],
                                        UPDATE=database_update,
                                        WHERE={"uuid": task["item_id"]})
            except db_base_Exception as e:
                self.logger.error("task={} Error updating database {}".format(task_id, e), exc_info=True)

            if nb_created == 10:
                break
        return nb_processed

    def _insert_pending_tasks(self, vim_actions_list):
        for task in vim_actions_list:
            if task["datacenter_vim_id"] != self.datacenter_tenant_id:
                continue
            item = task["item"]
            item_id = task["item_id"]
            action_key = item + item_id
            if action_key not in self.grouped_tasks:
                self.grouped_tasks[action_key] = []
            task["params"] = None
            task["depends"] = {}
            if task["extra"]:
                extra = yaml.load(task["extra"])
                task["extra"] = extra
                task["params"] = extra.get("params")
                depends_on_list = extra.get("depends_on")
                if depends_on_list:
                    for dependency_task in depends_on_list:
                        if isinstance(dependency_task, int):
                            index = dependency_task
                        else:
                            instance_action_id, _, task_id = dependency_task.rpartition(".")
                            if instance_action_id != task["instance_action_id"]:
                                continue
                            index = int(task_id)

                        if index < len(vim_actions_list) and vim_actions_list[index]["task_index"] == index and \
                                vim_actions_list[index]["instance_action_id"] == task["instance_action_id"]:
                            task["depends"]["TASK-" + str(index)] = vim_actions_list[index]
                            task["depends"]["TASK-{}.{}".format(task["instance_action_id"], index)] = vim_actions_list[index]
                if extra.get("interfaces"):
                    task["vim_interfaces"] = {}
            else:
                task["extra"] = {}
            if "error_msg" not in task:
                task["error_msg"] = None
            if "vim_id" not in task:
                task["vim_id"] = None

            if task["action"] == "DELETE":
                need_delete_action = False
                for to_supersede in self.grouped_tasks.get(action_key, ()):
                    if to_supersede["action"] == "FIND" and to_supersede.get("vim_id"):
                        task["vim_id"] = to_supersede["vim_id"]
                    if to_supersede["action"] == "CREATE" and to_supersede["extra"].get("created", True) and \
                            (to_supersede.get("vim_id") or to_supersede["extra"].get("sdn_net_id")):
                        need_delete_action = True
                        task["vim_id"] = to_supersede["vim_id"]
                        if to_supersede["extra"].get("sdn_net_id"):
                            task["extra"]["sdn_net_id"] = to_supersede["extra"]["sdn_net_id"]
                        if to_supersede["extra"].get("interfaces"):
                            task["extra"]["interfaces"] = to_supersede["extra"]["interfaces"]
                        if to_supersede["extra"].get("created_items"):
                            if not task["extra"].get("created_items"):
                                task["extra"]["created_items"] = {}
                            task["extra"]["created_items"].update(to_supersede["extra"]["created_items"])
                    # Mark task as SUPERSEDED.
                    #   If task is in self.pending_tasks, it will be removed and database will be update
                    #   If task is in self.refresh_tasks, it will be removed
                    to_supersede["status"] = "SUPERSEDED"
                if not need_delete_action:
                    task["status"] = "SUPERSEDED"

                self.grouped_tasks[action_key].append(task)
                self.pending_tasks.append(task)
            elif task["status"] == "SCHEDULED":
                self.grouped_tasks[action_key].append(task)
                self.pending_tasks.append(task)
            elif task["action"] in ("CREATE", "FIND"):
                self.grouped_tasks[action_key].append(task)
                if task["status"] in ("DONE", "BUILD"):
                    self._insert_refresh(task)
            # TODO add VM reset, get console, etc...
            else:
                raise vimconn.vimconnException(self.name + "unknown vim_action action {}".format(task["action"]))

    def insert_task(self, task):
        try:
            self.task_queue.put(task, False)
            return None
        except Queue.Full:
            raise vimconn.vimconnException(self.name + ": timeout inserting a task")

    def del_task(self, task):
        with self.task_lock:
            if task["status"] == "SCHEDULED":
                task["status"] = "SUPERSEDED"
                return True
            else:  # task["status"] == "processing"
                self.task_lock.release()
                return False

    def run(self):
        self.logger.debug("Starting")
        while True:
            self.get_vimconnector()
            self.logger.debug("Vimconnector loaded")
            self._reload_vim_actions()
            reload_thread = False

            while True:
                try:
                    while not self.task_queue.empty():
                        task = self.task_queue.get()
                        if isinstance(task, list):
                            self._insert_pending_tasks(task)
                        elif isinstance(task, str):
                            if task == 'exit':
                                return 0
                            elif task == 'reload':
                                reload_thread = True
                                break
                        self.task_queue.task_done()
                    if reload_thread:
                        break
                    nb_processed = self._proccess_pending_tasks()
                    nb_processed += self._refres_elements()
                    if not nb_processed:
                        time.sleep(1)

                except Exception as e:
                    self.logger.critical("Unexpected exception at run: " + str(e), exc_info=True)

        self.logger.debug("Finishing")

    def _look_for_task(self, instance_action_id, task_id):
        """
        Look for a concrete task at vim_actions database table
        :param instance_action_id: The instance_action_id
        :param task_id: Can have several formats:
            <task index>: integer
            TASK-<task index> :backward compatibility,
            [TASK-]<instance_action_id>.<task index>: this instance_action_id overrides the one in the parameter
        :return: Task dictionary or None if not found
        """
        if isinstance(task_id, int):
            task_index = task_id
        else:
            if task_id.startswith("TASK-"):
                task_id = task_id[5:]
            ins_action_id, _, task_index = task_id.rpartition(".")
            if ins_action_id:
                instance_action_id = ins_action_id

        tasks = self.db.get_rows(FROM="vim_wim_actions", WHERE={"instance_action_id": instance_action_id,
                                                            "task_index": task_index})
        if not tasks:
            return None
        task = tasks[0]
        task["params"] = None
        task["depends"] = {}
        if task["extra"]:
            extra = yaml.load(task["extra"])
            task["extra"] = extra
            task["params"] = extra.get("params")
            if extra.get("interfaces"):
                task["vim_interfaces"] = {}
        else:
            task["extra"] = {}
        return task

    @staticmethod
    def _format_vim_error_msg(error_text, max_length=1024):
        if error_text and len(error_text) >= max_length:
            return error_text[:max_length // 2 - 3] + " ... " + error_text[-max_length // 2 + 3:]
        return error_text

    def new_vm(self, task):
        task_id = task["instance_action_id"] + "." + str(task["task_index"])
        try:
            params = task["params"]
            depends = task.get("depends")
            net_list = params[5]
            for net in net_list:
                if "net_id" in net and is_task_id(net["net_id"]):  # change task_id into network_id
                    task_dependency = task["depends"].get(net["net_id"])
                    if not task_dependency:
                        task_dependency = self._look_for_task(task["instance_action_id"], net["net_id"])
                        if not task_dependency:
                            raise VimThreadException(
                                "Cannot get depending net task trying to get depending task {}.{}".format(
                                    task["instance_action_id"], net["net_id"]))
                    network_id = task_dependency.get("vim_id")
                    if not network_id:
                        raise VimThreadException(
                            "Cannot create VM because depends on a network not created or found: " +
                            str(depends[net["net_id"]]["error_msg"]))
                    net["net_id"] = network_id
            params_copy = deepcopy(params)
            vim_vm_id, created_items = self.vim.new_vminstance(*params_copy)

            # fill task_interfaces. Look for snd_net_id at database for each interface
            task_interfaces = {}
            for iface in params_copy[5]:
                task_interfaces[iface["vim_id"]] = {"iface_id": iface["uuid"]}
                result = self.db.get_rows(
                    SELECT=('sdn_net_id', 'interface_id'),
                    FROM='instance_nets as ine join instance_interfaces as ii on ii.instance_net_id=ine.uuid',
                    WHERE={'ii.uuid': iface["uuid"]})
                if result:
                    task_interfaces[iface["vim_id"]]["sdn_net_id"] = result[0]['sdn_net_id']
                    task_interfaces[iface["vim_id"]]["interface_id"] = result[0]['interface_id']
                else:
                    self.logger.critical("task={} new-VM: instance_nets uuid={} not found at DB".format(task_id,
                                                                                                        iface["uuid"]), exc_info=True)

            task["vim_info"] = {}
            task["vim_interfaces"] = {}
            task["extra"]["interfaces"] = task_interfaces
            task["extra"]["created"] = True
            task["extra"]["created_items"] = created_items
            task["error_msg"] = None
            task["status"] = "DONE"
            task["vim_id"] = vim_vm_id
            instance_element_update = {"status": "BUILD", "vim_vm_id": vim_vm_id, "error_msg": None}
            return True, instance_element_update

        except (vimconn.vimconnException, VimThreadException) as e:
            self.logger.error("task={} new-VM: {}".format(task_id, e))
            error_text = self._format_vim_error_msg(str(e))
            task["error_msg"] = error_text
            task["status"] = "FAILED"
            task["vim_id"] = None
            instance_element_update = {"status": "VIM_ERROR", "vim_vm_id": None, "error_msg": error_text}
            return False, instance_element_update

    def del_vm(self, task):
        task_id = task["instance_action_id"] + "." + str(task["task_index"])
        vm_vim_id = task["vim_id"]
        interfaces = task["extra"].get("interfaces", ())
        try:
            for iface in interfaces.values():
                if iface.get("sdn_port_id"):
                    try:
                        with self.db_lock:
                            self.ovim.delete_port(iface["sdn_port_id"], idempotent=True)
                    except ovimException as e:
                        self.logger.error("task={} del-VM: ovimException when deleting external_port={}: {} ".format(
                            task_id, iface["sdn_port_id"], e), exc_info=True)
                        # TODO Set error_msg at instance_nets

            self.vim.delete_vminstance(vm_vim_id, task["extra"].get("created_items"))
            task["status"] = "DONE"
            task["error_msg"] = None
            return True, None

        except vimconn.vimconnException as e:
            task["error_msg"] = self._format_vim_error_msg(str(e))
            if isinstance(e, vimconn.vimconnNotFoundException):
                # If not found mark as Done and fill error_msg
                task["status"] = "DONE"
                return True, None
            task["status"] = "FAILED"
            return False, None

    def _get_net_internal(self, task, filter_param):
        """
        Common code for get_net and new_net. It looks for a network on VIM with the filter_params
        :param task: task for this find or find-or-create action
        :param filter_param: parameters to send to the vimconnector
        :return: a dict with the content to update the instance_nets database table. Raises an exception on error, or
            when network is not found or found more than one
        """
        vim_nets = self.vim.get_network_list(filter_param)
        if not vim_nets:
            raise VimThreadExceptionNotFound("Network not found with this criteria: '{}'".format(filter_param))
        elif len(vim_nets) > 1:
            raise VimThreadException("More than one network found with this criteria: '{}'".format(filter_param))
        vim_net_id = vim_nets[0]["id"]

        # Discover if this network is managed by a sdn controller
        sdn_net_id = None
        result = self.db.get_rows(SELECT=('sdn_net_id',), FROM='instance_nets',
                                    WHERE={'vim_net_id': vim_net_id,
                                            'datacenter_tenant_id': self.datacenter_tenant_id},
                                    ORDER="instance_scenario_id")
        if result:
            sdn_net_id = result[0]['sdn_net_id']

        task["status"] = "DONE"
        task["extra"]["vim_info"] = {}
        task["extra"]["created"] = False
        task["extra"]["sdn_net_id"] = sdn_net_id
        task["error_msg"] = None
        task["vim_id"] = vim_net_id
        instance_element_update = {"vim_net_id": vim_net_id, "created": False, "status": "BUILD",
                                   "error_msg": None, "sdn_net_id": sdn_net_id}
        return instance_element_update

    def get_net(self, task):
        task_id = task["instance_action_id"] + "." + str(task["task_index"])
        try:
            params = task["params"]
            filter_param = params[0]
            instance_element_update = self._get_net_internal(task, filter_param)
            return True, instance_element_update

        except (vimconn.vimconnException, VimThreadException) as e:
            self.logger.error("task={} get-net: {}".format(task_id, e))
            task["status"] = "FAILED"
            task["vim_id"] = None
            task["error_msg"] = self._format_vim_error_msg(str(e))
            instance_element_update = {"vim_net_id": None, "status": "VIM_ERROR",
                                       "error_msg": task["error_msg"]}
            return False, instance_element_update

    def new_net(self, task):
        vim_net_id = None
        sdn_net_id = None
        task_id = task["instance_action_id"] + "." + str(task["task_index"])
        action_text = ""
        try:
            # FIND
            if task["extra"].get("find"):
                action_text = "finding"
                filter_param = task["extra"]["find"][0]
                try:
                    instance_element_update = self._get_net_internal(task, filter_param)
                    return True, instance_element_update
                except VimThreadExceptionNotFound:
                    pass
            # CREATE
            params = task["params"]
            action_text = "creating VIM"
            vim_net_id = self.vim.new_network(*params[0:3])

            net_name = params[0]
            net_type = params[1]
            wim_account_name = None
            if len(params) >= 4:
                wim_account_name = params[3]

            sdn_controller = self.vim.config.get('sdn-controller')
            if sdn_controller and (net_type == "data" or net_type == "ptp"):
                network = {"name": net_name, "type": net_type, "region": self.vim["config"]["datacenter_id"]}

                vim_net = self.vim.get_network(vim_net_id)
                if vim_net.get('encapsulation') != 'vlan':
                    raise vimconn.vimconnException(
                        "net '{}' defined as type '{}' has not vlan encapsulation '{}'".format(
                            net_name, net_type, vim_net['encapsulation']))
                network["vlan"] = vim_net.get('segmentation_id')
                action_text = "creating SDN"
                with self.db_lock:
                    sdn_net_id = self.ovim.new_network(network)

                if wim_account_name and self.vim.config["wim_external_ports"]:
                    # add external port to connect WIM. Try with compute node __WIM:wim_name and __WIM
                    action_text = "attaching external port to ovim network"
                    sdn_port_name = sdn_net_id + "." + task["vim_id"]
                    sdn_port_name = sdn_port_name[:63]
                    sdn_port_data = {
                        "compute_node": "__WIM:" + wim_account_name[0:58],
                        "pci": None,
                        "vlan": network["vlan"],
                        "net_id": sdn_net_id,
                        "region": self.vim["config"]["datacenter_id"],
                        "name": sdn_port_name,
                    }
                    try:
                        with self.db_lock:
                            sdn_external_port_id = self.ovim.new_external_port(sdn_port_data)
                    except ovimException:
                        sdn_port_data["compute_node"] = "__WIM"
                        with self.db_lock:
                            sdn_external_port_id = self.ovim.new_external_port(sdn_port_data)
                    self.logger.debug("Added sdn_external_port {} to sdn_network {}".format(sdn_external_port_id,
                                                                                            sdn_net_id))

            task["status"] = "DONE"
            task["extra"]["vim_info"] = {}
            task["extra"]["sdn_net_id"] = sdn_net_id
            task["extra"]["created"] = True
            task["error_msg"] = None
            task["vim_id"] = vim_net_id
            instance_element_update = {"vim_net_id": vim_net_id, "sdn_net_id": sdn_net_id, "status": "BUILD",
                                       "created": True, "error_msg": None}
            return True, instance_element_update
        except (vimconn.vimconnException, ovimException) as e:
            self.logger.error("task={} new-net: Error {}: {}".format(task_id, action_text, e))
            task["status"] = "FAILED"
            task["vim_id"] = vim_net_id
            task["error_msg"] = self._format_vim_error_msg(str(e))
            task["extra"]["sdn_net_id"] = sdn_net_id
            instance_element_update = {"vim_net_id": vim_net_id, "sdn_net_id": sdn_net_id, "status": "VIM_ERROR",
                                       "error_msg": task["error_msg"]}
            return False, instance_element_update

    def del_net(self, task):
        net_vim_id = task["vim_id"]
        sdn_net_id = task["extra"].get("sdn_net_id")
        try:
            if sdn_net_id:
                # Delete any attached port to this sdn network. There can be ports associated to this network in case
                # it was manually done using 'openmano vim-net-sdn-attach'
                with self.db_lock:
                    port_list = self.ovim.get_ports(columns={'uuid'},
                                                    filter={'name': 'external_port', 'net_id': sdn_net_id})
                    for port in port_list:
                        self.ovim.delete_port(port['uuid'], idempotent=True)
                    self.ovim.delete_network(sdn_net_id, idempotent=True)
            if net_vim_id:
                self.vim.delete_network(net_vim_id)
            task["status"] = "DONE"
            task["error_msg"] = None
            return True, None
        except ovimException as e:
            task["error_msg"] = self._format_vim_error_msg("ovimException obtaining and deleting external "
                                                           "ports for net {}: {}".format(sdn_net_id, str(e)))
        except vimconn.vimconnException as e:
            task["error_msg"] = self._format_vim_error_msg(str(e))
            if isinstance(e, vimconn.vimconnNotFoundException):
                # If not found mark as Done and fill error_msg
                task["status"] = "DONE"
                return True, None
        task["status"] = "FAILED"
        return False, None

    ## Service Function Instances

    def new_sfi(self, task):
        vim_sfi_id = None
        try:
            # Waits for interfaces to be ready (avoids failure)
            time.sleep(1)
            dep_id = "TASK-" + str(task["extra"]["depends_on"][0])
            task_id = task["instance_action_id"] + "." + str(task["task_index"])
            error_text = ""
            interfaces = task.get("depends").get(dep_id).get("extra").get("interfaces")
            ingress_interface_id = task.get("extra").get("params").get("ingress_interface_id")
            egress_interface_id = task.get("extra").get("params").get("egress_interface_id")
            ingress_vim_interface_id = None
            egress_vim_interface_id = None
            for vim_interface, interface_data in interfaces.iteritems():
                if interface_data.get("interface_id") == ingress_interface_id:
                    ingress_vim_interface_id = vim_interface
                    break
            if ingress_interface_id != egress_interface_id:
                for vim_interface, interface_data in interfaces.iteritems():
                    if interface_data.get("interface_id") == egress_interface_id:
                        egress_vim_interface_id = vim_interface
                        break
            else:
                egress_vim_interface_id = ingress_vim_interface_id
            if not ingress_vim_interface_id or not egress_vim_interface_id:
                self.logger.error("Error creating Service Function Instance, Ingress: %s, Egress: %s",
                                  ingress_vim_interface_id, egress_vim_interface_id)
                return False, None
            # At the moment, every port associated with the VM will be used both as ingress and egress ports.
            # Bear in mind that different VIM connectors might support SFI differently. In the case of OpenStack, only the
            # first ingress and first egress ports will be used to create the SFI (Port Pair).
            ingress_port_id_list = [ingress_vim_interface_id]
            egress_port_id_list = [egress_vim_interface_id]
            name = "sfi-%s" % task["item_id"][:8]
            # By default no form of IETF SFC Encapsulation will be used
            vim_sfi_id = self.vim.new_sfi(name, ingress_port_id_list, egress_port_id_list, sfc_encap=False)

            task["extra"]["created"] = True
            task["error_msg"] = None
            task["status"] = "DONE"
            task["vim_id"] = vim_sfi_id
            instance_element_update = {"status": "ACTIVE", "vim_sfi_id": vim_sfi_id, "error_msg": None}
            return True, instance_element_update

        except (vimconn.vimconnException, VimThreadException) as e:
            self.logger.error("Error creating Service Function Instance, task=%s: %s", task_id, str(e))
            error_text = self._format_vim_error_msg(str(e))
            task["error_msg"] = error_text
            task["status"] = "FAILED"
            task["vim_id"] = None
            instance_element_update = {"status": "VIM_ERROR", "vim_sfi_id": None, "error_msg": error_text}
            return False, instance_element_update

    def del_sfi(self, task):
        sfi_vim_id = task["vim_id"]
        try:
            self.vim.delete_sfi(sfi_vim_id)
            task["status"] = "DONE"
            task["error_msg"] = None
            return True, None

        except vimconn.vimconnException as e:
            task["error_msg"] = self._format_vim_error_msg(str(e))
            if isinstance(e, vimconn.vimconnNotFoundException):
                # If not found mark as Done and fill error_msg
                task["status"] = "DONE"
                return True, None
            task["status"] = "FAILED"
            return False, None

    def new_sf(self, task):
        vim_sf_id = None
        try:
            task_id = task["instance_action_id"] + "." + str(task["task_index"])
            error_text = ""
            depending_tasks = ["TASK-" + str(dep_id) for dep_id in task["extra"]["depends_on"]]
            # sfis = task.get("depends").values()[0].get("extra").get("params")[5]
            sfis = [task.get("depends").get(dep_task) for dep_task in depending_tasks]
            sfi_id_list = []
            for sfi in sfis:
                sfi_id_list.append(sfi.get("vim_id"))
            name = "sf-%s" % task["item_id"][:8]
            # By default no form of IETF SFC Encapsulation will be used
            vim_sf_id = self.vim.new_sf(name, sfi_id_list, sfc_encap=False)

            task["extra"]["created"] = True
            task["error_msg"] = None
            task["status"] = "DONE"
            task["vim_id"] = vim_sf_id
            instance_element_update = {"status": "ACTIVE", "vim_sf_id": vim_sf_id, "error_msg": None}
            return True, instance_element_update

        except (vimconn.vimconnException, VimThreadException) as e:
            self.logger.error("Error creating Service Function, task=%s: %s", task_id, str(e))
            error_text = self._format_vim_error_msg(str(e))
            task["error_msg"] = error_text
            task["status"] = "FAILED"
            task["vim_id"] = None
            instance_element_update = {"status": "VIM_ERROR", "vim_sf_id": None, "error_msg": error_text}
            return False, instance_element_update

    def del_sf(self, task):
        sf_vim_id = task["vim_id"]
        try:
            self.vim.delete_sf(sf_vim_id)
            task["status"] = "DONE"
            task["error_msg"] = None
            return True, None

        except vimconn.vimconnException as e:
            task["error_msg"] = self._format_vim_error_msg(str(e))
            if isinstance(e, vimconn.vimconnNotFoundException):
                # If not found mark as Done and fill error_msg
                task["status"] = "DONE"
                return True, None
            task["status"] = "FAILED"
            return False, None

    def new_classification(self, task):
        vim_classification_id = None
        try:
            params = task["params"]
            task_id = task["instance_action_id"] + "." + str(task["task_index"])
            dep_id = "TASK-" + str(task["extra"]["depends_on"][0])
            error_text = ""
            interfaces = task.get("depends").get(dep_id).get("extra").get("interfaces").keys()
            # Bear in mind that different VIM connectors might support Classifications differently.
            # In the case of OpenStack, only the first VNF attached to the classifier will be used
            # to create the Classification(s) (the "logical source port" of the "Flow Classifier").
            # Since the VNFFG classifier match lacks the ethertype, classification defaults to
            # using the IPv4 flow classifier.
            name = "c-%s" % task["item_id"][:8]
            # if not CIDR is given for the IP addresses, add /32:
            ip_proto = int(params.get("ip_proto"))
            source_ip = params.get("source_ip")
            destination_ip = params.get("destination_ip")
            source_port = params.get("source_port")
            destination_port = params.get("destination_port")
            definition = {"logical_source_port": interfaces[0]}
            if ip_proto:
                if ip_proto == 1:
                    ip_proto = 'icmp'
                elif ip_proto == 6:
                    ip_proto = 'tcp'
                elif ip_proto == 17:
                    ip_proto = 'udp'
                definition["protocol"] = ip_proto
            if source_ip:
                if '/' not in source_ip:
                    source_ip += '/32'
                definition["source_ip_prefix"] = source_ip
            if source_port:
                definition["source_port_range_min"] = source_port
                definition["source_port_range_max"] = source_port
            if destination_port:
                definition["destination_port_range_min"] = destination_port
                definition["destination_port_range_max"] = destination_port
            if destination_ip:
                if '/' not in destination_ip:
                    destination_ip += '/32'
                definition["destination_ip_prefix"] = destination_ip

            vim_classification_id = self.vim.new_classification(
                name, 'legacy_flow_classifier', definition)

            task["extra"]["created"] = True
            task["error_msg"] = None
            task["status"] = "DONE"
            task["vim_id"] = vim_classification_id
            instance_element_update = {"status": "ACTIVE", "vim_classification_id": vim_classification_id, "error_msg": None}
            return True, instance_element_update

        except (vimconn.vimconnException, VimThreadException) as e:
            self.logger.error("Error creating Classification, task=%s: %s", task_id, str(e))
            error_text = self._format_vim_error_msg(str(e))
            task["error_msg"] = error_text
            task["status"] = "FAILED"
            task["vim_id"] = None
            instance_element_update = {"status": "VIM_ERROR", "vim_classification_id": None, "error_msg": error_text}
            return False, instance_element_update

    def del_classification(self, task):
        classification_vim_id = task["vim_id"]
        try:
            self.vim.delete_classification(classification_vim_id)
            task["status"] = "DONE"
            task["error_msg"] = None
            return True, None

        except vimconn.vimconnException as e:
            task["error_msg"] = self._format_vim_error_msg(str(e))
            if isinstance(e, vimconn.vimconnNotFoundException):
                # If not found mark as Done and fill error_msg
                task["status"] = "DONE"
                return True, None
            task["status"] = "FAILED"
            return False, None

    def new_sfp(self, task):
        vim_sfp_id = None
        try:
            params = task["params"]
            task_id = task["instance_action_id"] + "." + str(task["task_index"])
            depending_tasks = [task.get("depends").get("TASK-" + str(tsk_id)) for tsk_id in task.get("extra").get("depends_on")]
            error_text = ""
            sf_id_list = []
            classification_id_list = []
            for dep in depending_tasks:
                vim_id = dep.get("vim_id")
                resource = dep.get("item")
                if resource == "instance_sfs":
                    sf_id_list.append(vim_id)
                elif resource == "instance_classifications":
                    classification_id_list.append(vim_id)

            name = "sfp-%s" % task["item_id"][:8]
            # By default no form of IETF SFC Encapsulation will be used
            vim_sfp_id = self.vim.new_sfp(name, classification_id_list, sf_id_list, sfc_encap=False)

            task["extra"]["created"] = True
            task["error_msg"] = None
            task["status"] = "DONE"
            task["vim_id"] = vim_sfp_id
            instance_element_update = {"status": "ACTIVE", "vim_sfp_id": vim_sfp_id, "error_msg": None}
            return True, instance_element_update

        except (vimconn.vimconnException, VimThreadException) as e:
            self.logger.error("Error creating Service Function, task=%s: %s", task_id, str(e))
            error_text = self._format_vim_error_msg(str(e))
            task["error_msg"] = error_text
            task["status"] = "FAILED"
            task["vim_id"] = None
            instance_element_update = {"status": "VIM_ERROR", "vim_sfp_id": None, "error_msg": error_text}
            return False, instance_element_update
        return

    def del_sfp(self, task):
        sfp_vim_id = task["vim_id"]
        try:
            self.vim.delete_sfp(sfp_vim_id)
            task["status"] = "DONE"
            task["error_msg"] = None
            return True, None

        except vimconn.vimconnException as e:
            task["error_msg"] = self._format_vim_error_msg(str(e))
            if isinstance(e, vimconn.vimconnNotFoundException):
                # If not found mark as Done and fill error_msg
                task["status"] = "DONE"
                return True, None
            task["status"] = "FAILED"
            return False, None
