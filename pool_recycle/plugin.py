#!/usr/bin/env python

# Copyright 2015 tsuru-pool-recycle-plugin authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import sys
import argparse
import json
import urllib2
import socket
import re
import time

from functools import partial
from urlparse import urlparse


class NewNodeError(Exception):
    def __init__(self, name):
        super(Exception, self).__init__(name)
        self.name = name

    def __str__(self):
        return 'Error creating new node: "{}"'.format(self.name)

    def __unicode__(self):
        return unicode(str(self))


class MoveNodeContainersError(Exception):
    def __init__(self, name):
        super(Exception, self).__init__(name)
        self.name = name

    def __str__(self):
        return 'Error moving node containers: "{}"'.format(self.name)

    def __unicode__(self):
        return unicode(str(self))


class RemoveNodeFromPoolError(Exception):
    def __init__(self, name):
        super(Exception, self).__init__(name)
        self.name = name

    def __str__(self):
        return 'Error removing node from pool: "{}"'.format(self.name)

    def __unicode__(self):
        return unicode(str(self))


class RemoveMachineFromIaaSError(Exception):
    def __init__(self, name, machine_id=None):
        super(Exception, self).__init__(name)
        self.name = name
        self.machine_id = machine_id

    def __str__(self):
        if self.machine_id is None:
            return 'Error removing machine from IaaS: "{}"'.format(self.name)
        return 'Error removing machine {} from IaaS: "{}"'.format(self.machine_id, self.name)

    def __unicode__(self):
        return unicode(str(self))


class TsuruPool(object):

    def __init__(self, pool):
        try:
            self.tsuru_target = os.environ['TSURU_TARGET'].rstrip("/")
            self.tsuru_token = os.environ['TSURU_TOKEN']
        except KeyError:
            raise KeyError("TSURU_TARGET or TSURU_TOKEN envs not set")
        self.pool = pool

    def get_nodes(self):
        return_code, docker_nodes = self.__tsuru_request("GET", "/docker/node")
        if return_code not in [200, 201, 204]:
            raise Exception('Error get nodes from tsuru: "{}"'.format(docker_nodes))
        docker_nodes = json.loads(docker_nodes.read())
        pool_nodes = []
        if 'nodes' in docker_nodes and docker_nodes['nodes'] is not None:
            for node in docker_nodes['nodes']:
                if ('pool' in node['Metadata'] and
                   node['Metadata']['pool'] == self.pool):
                    pool_nodes.append(node['Address'])
        return pool_nodes

    def get_machine_metadata_from_iaas(self, node):
        node_hostname = self.get_address(node)
        return_code, iaas_machines = self.__tsuru_request("GET", "/iaas/machines")
        if return_code not in [200, 201, 204]:
            raise Exception('Error get iaas machines from tsuru: "{}"'.format(iaas_machines))
        for machine in json.load(iaas_machines):
            try:
                if machine['Address'] == node_hostname:
                    return {'id': machine['Id'], 'metadata': machine['CreationParams']}
            except:
                pass
        return None

    def get_node_metadata(self, node):
        node_hostname = self.get_address(node)
        return_code, nodes_list = self.__tsuru_request("GET", "/docker/node")
        if return_code not in [200, 201, 204]:
            raise Exception('Error get node metadata from tsuru: "{}"'.format(nodes_list))

        try:
            machines_nodes_list = json.load(nodes_list)
            for node_data in machines_nodes_list['nodes']:
                node_data_hostname = self.get_address(node_data['Address'])
                if node_data_hostname == node_hostname:
                    return node_data['Metadata']
        except:
            pass
        return None

    def add_node_to_pool(self, node_url, docker_port, docker_scheme, params={}):
        if not (re.match(r'^https?://', node_url)):
            node_url = '{}://{}:{}'.format(docker_scheme, node_url, docker_port)
        try:
            post_data = dict({"address": node_url, "pool": self.pool}, **params)
        except TypeError:
            post_data = {"address": node_url, "pool": self.pool}
        (return_code,
         msg) = self.__tsuru_request("POST", "/docker/node?register=true", post_data)
        if return_code not in [200, 201, 204]:
            raise NewNodeError(msg)
        return True

    def create_new_node(self, iaas_template):
        actual_nodes_list = self.get_nodes()
        (return_code,
         msg) = self.__tsuru_request("POST", "/docker/node?register=false",
                                             {'template': iaas_template})
        if return_code not in [200, 201, 204]:
            raise NewNodeError(msg)

        new_nodes_list = self.get_nodes()
        new_node = set(new_nodes_list) - set(actual_nodes_list)
        if len(new_node) == 1:
            return new_node.pop()
        raise NewNodeError("New node not found on Tsuru")

    def get_machines_templates(self):
        (return_code,
         machines_templates) = self.__tsuru_request("GET", "/iaas/templates")
        if return_code not in [200, 201, 204]:
            raise Exception('Error getting machines templates '
                            'on tsuru: "{}"'.format(machines_templates))
        machines_templates = json.loads(machines_templates.read())
        iaas_templates = []
        if machines_templates is None:
            return iaas_templates
        for template in machines_templates:
            for item in template['Data']:
                if 'pool' == item['Name'] and self.pool == item['Value']:
                    iaas_templates.append(template['Name'])
        return iaas_templates

    def remove_node_from_pool(self, node):
        headers = {'address': node}
        return_code, msg = self.__tsuru_request("DELETE", "/docker/node",
                                                headers)
        if return_code not in [200, 201, 204]:
            raise RemoveNodeFromPoolError(msg)

        return True

    def remove_machine_from_iaas(self, node):
        machine_metadata = self.get_machine_metadata_from_iaas(node)
        if machine_metadata is None:
            raise RemoveMachineFromIaaSError("machine {} not found on IaaS".format(node))
        machine_id = machine_metadata['id']
        return_code, msg = self.__tsuru_request("DELETE", "/iaas/machines/{}".format(machine_id))

        if return_code not in [200, 201, 204]:
            raise RemoveMachineFromIaaSError(msg, machine_id)
        return True

    def move_node_containers(self, node, new_node, cur_retry=0, max_retry=10, wait_timeout=180):
        node_from = self.get_address(node)
        node_to = self.get_address(new_node)
        if node_from is None or node_to is None:
            raise MoveNodeContainersError('node address {} or {} '
                                          '- are invalids'.format(node, new_node))

        (return_code,
         move_progress) = self.__tsuru_request("POST",
                                               "/docker/containers/move",
                                               {'from': node_from,
                                                'to': node_to})
        if return_code not in [200, 201, 204]:
            raise MoveNodeContainersError(move_progress)

        moving_error = False
        no_data = True
        for move_msg in self.json_parser(move_progress):
            no_data = False
            if 'Error' in move_msg['Message']:
                moving_error = True
                sys.stderr.write("{}\n".format(move_msg['Message']))
            else:
                sys.stdout.write("{}\n".format(move_msg['Message']))

        if moving_error and cur_retry < max_retry:
            sys.stdout.write("Retrying move containers from {} to {}. "
                             "Waiting for {} seconds...".format(node, new_node, wait_timeout))
            sys.stdout.flush()
            time.sleep(wait_timeout)
            self.move_node_containers(node, new_node, (cur_retry + 1), max_retry, wait_timeout)
            return True
        elif moving_error and cur_retry >= max_retry:
            sys.stderr.write("Error: Max retry reached for moving on {} attempts.".format(max_retry + 1))
            raise MoveNodeContainersError("moving containers from {} to {} aborted on error."
                                          .format(node, new_node))
        if no_data:
            raise MoveNodeContainersError("moving containers from {} to {} aborted on error."
                                          .format(node, new_node))
        return True

    def __tsuru_request(self, method, path, body=None):
        url = "{}{}".format(self.tsuru_target, path)
        request = urllib2.Request(url)
        request.add_header("Authorization", "bearer " + self.tsuru_token)
        request.get_method = lambda: method

        if body:
            request.add_data(json.dumps(body))

        response_code = response_msg = None
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            response_code = e.code
            response_msg = e.read().rstrip("\n")
            pass

        if response_code:
            return response_code, response_msg

        response_code = response.getcode()
        return response_code, response

    @staticmethod
    def get_address(node_name):
        try:
            socket.inet_aton(node_name)
            return(node_name)
        except socket.error:
            return urlparse(node_name).hostname

    @staticmethod
    def json_parser(fileobj, decoder=json.JSONDecoder(), buffersize=2048):
        buffer = ''
        first_chunk = True
        recheck_first_chunk = False
        for chunk in iter(partial(fileobj.read, buffersize), ''):
            buffer += chunk
            while buffer:
                try:
                    result, index = decoder.raw_decode(buffer)
                    first_chunk = False
                    # Try to move index to next json
                    while (index < len(buffer)) and (buffer[index] != '{'):
                        index = index + 1
                    yield result
                    buffer = buffer[index:]
                except ValueError:
                    # Try to cleanup first chunk until find initial json
                    # string
                    if first_chunk:
                        index_first_chunk = 0
                        while (index_first_chunk < len(buffer)) and (buffer[index_first_chunk] != '{'):
                            index_first_chunk = index_first_chunk + 1
                            recheck_first_chunk = True
                        buffer = buffer[index_first_chunk:]
                        first_chunk = False
                        if recheck_first_chunk:
                            recheck_first_chunk = False
                            continue
                    # Not enough data to decode, read more
                    break


def pool_recycle(pool_name, destroy_node=False, dry_mode=False, max_retry=10, wait_timeout=180,
                 docker_port='4243', docker_scheme='http', pre_provision=False):
    pool_handler = TsuruPool(pool_name)
    pool_templates = pool_handler.get_machines_templates()
    if pool_templates == []:
        raise Exception('Pool "{}" does not contain any template associate'.format(pool_name))
    templates_len = len(pool_templates)
    template_idx = 0
    nodes_to_recycle = pool_handler.get_nodes()

    pre_provision_nodes = []
    if pre_provision and not dry_mode:
        try:
            for _ in xrange(len(nodes_to_recycle)):
                sys.stdout.write('Creating new node on pool "{}" '
                                 'using {} template\n'.format(pool_name, pool_templates[template_idx]))
                new_node = {}
                new_node['address'] = pool_handler.create_new_node(pool_templates[template_idx])
                new_node['metadata'] = pool_handler.get_node_metadata(new_node['address'])
                pool_handler.remove_node_from_pool(new_node['address'])
                pre_provision_nodes.append(new_node)
                template_idx += 1
                if template_idx >= templates_len:
                    template_idx = 0
        except Exception, e:
            for node in pre_provision_nodes:
                try:
                    pool_handler.remove_node_from_pool(node['address'])
                except:
                    pass
                try:
                    pool_handler.remove_machine_from_iaas(node['address'])
                except:
                    pass
            raise e

    new_node = None
    for node in nodes_to_recycle:
        if dry_mode:
            sys.stdout.write('Creating new node on pool "{}" using "{}" '
                             'template\n'.format(pool_name, pool_templates[template_idx]))
            sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
            sys.stdout.write('Moving all containers on old node "{}" to new node\n'.format(node))
            template_idx += 1
            if template_idx >= templates_len:
                template_idx = 0
            if destroy_node:
                sys.stdout.write('Machine {} removed from IaaS\n'.format(node))
            sys.stdout.write('\n')
            continue
        try:
            if pre_provision:
                new_node = pre_provision_nodes.pop()
                sys.stdout.write('Using {} node as destination node\n'.format(new_node['address']))
            else:
                sys.stdout.write('Creating new node on pool "{}" '
                                 'using {} template\n'.format(pool_name, pool_templates[template_idx]))
                new_node = {}
                new_node['address'] = pool_handler.create_new_node(pool_templates[template_idx])
            sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
            node_data = pool_handler.get_node_metadata(node)
            pool_handler.remove_node_from_pool(node)
            if pre_provision:
                pool_handler.add_node_to_pool(new_node['address'], docker_port, docker_scheme,
                                              new_node['metadata'])
            sys.stdout.write('Moving all containers from old node "{}"'
                             ' to new node "{}"\n'.format(node, new_node['address']))
            pool_handler.move_node_containers(node, new_node['address'], 0, max_retry, wait_timeout)
            template_idx += 1
            if template_idx >= templates_len:
                template_idx = 0
            if destroy_node:
                pool_handler.remove_machine_from_iaas(node)
                sys.stdout.write('Machine {} removed from IaaS\n'.format(node))
            new_node = None
        except (MoveNodeContainersError, RemoveNodeFromPoolError, KeyboardInterrupt), e:
            ''' Try to re-insert node on pool '''
            pool_handler.add_node_to_pool(node, docker_port, docker_scheme, node_data)
            if not dry_mode and pre_provision:
                for node in pre_provision_nodes:
                    pool_handler.remove_machine_from_iaas(node['address'])
            raise e
        except Exception, e:
            if not dry_mode and pre_provision:
                for node in pre_provision_nodes:
                    pool_handler.remove_machine_from_iaas(node['address'])
            raise e


def pool_recycle_parser(args):
    parser = argparse.ArgumentParser(description="Tsuru pool nodes recycle")
    parser.add_argument("-p", "--pool", required=True,
                        help="Docker tsuru pool")
    parser.add_argument("-r", "--destroy-node", required=False, action='store_true',
                        help="Destroy olds docker nodes after recycle")
    parser.add_argument("-d", "--dry-run", required=False, action='store_true',
                        help="Dry run all recycle actions")
    parser.add_argument("-m", "--max_retry", required=False, default=10, type=int,
                        help="Max retries attempts to move a node on failure")
    parser.add_argument("-t", "--timeout", required=False, default=180, type=int,
                        help="Max timeout between moves on failures attempts")
    parser.add_argument("-P", "--docker-port", required=False, default='4243',
                        help="Docker port - if something goes wrong, "
                             "node will be re-add using it as docker port "
                             "(only when using IaaS)")
    parser.add_argument("-s", "--docker-scheme", required=False,
                        default='http', help="Docker scheme - if something goes "
                        "wrong, node will be re-add using it as docker scheme "
                        "(only when using IaaS)")
    parser.add_argument("--pre_provision", required=False, action='store_true',
                        help="Pre-provision all nodes on IaaS before start moving")
    parsed = parser.parse_args(args)
    pool_recycle(parsed.pool, parsed.destroy_node, parsed.dry_run,
                 parsed.max_retry, parsed.timeout, parsed.docker_port, parsed.docker_scheme,
                 parsed.pre_provision)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    pool_recycle_parser(args)

if __name__ == "__main__":
    main()
