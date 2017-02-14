#!/usr/bin/env python

# Copyright 2015 tsuru-pool-recycle-plugin authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import sys
import argparse
import socket
import re
import time

from urlparse import urlparse
from tsuruclient import client


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
            self.client = client.Client(self.tsuru_target, self.tsuru_token)
        except KeyError:
            raise KeyError("TSURU_TARGET or TSURU_TOKEN envs not set")
        self.pool = pool

    def get_nodes(self):
        try:
            docker_nodes = self.client.nodes.list()
        except Exception as ex:
            raise Exception('Error get nodes from tsuru: "{}"'.format(ex))

        pool_nodes = []
        if 'nodes' in docker_nodes and docker_nodes['nodes'] is not None:
            for node in docker_nodes['nodes']:
                if ('pool' in node['Metadata'] and
                   node['Metadata']['pool'] == self.pool):
                    pool_nodes.append(node['Address'])
        return pool_nodes

    def get_machine_metadata_from_iaas(self, node):
        node_hostname = self.get_address(node)
        try:
            iaas_machines = self.client.machines.list()
        except Exception as ex:
            raise Exception('Error get iaas machines from tsuru: "{}"'
                            .format(ex))

        for machine in iaas_machines:
            try:
                if machine['Address'] == node_hostname:
                    return {'id': machine['Id'], 'metadata': machine['CreationParams']}
            except:
                pass
        return None

    def get_node_metadata(self, node):
        node_hostname = self.get_address(node)
        try:
            nodes_list = self.client.nodes.list()
        except Exception as ex:
            raise Exception('Error get node metadata from tsuru: "{}"'
                            .format(ex))

        try:
            for node_data in nodes_list['nodes']:
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
            post_data = dict({
                "address": node_url,
                "pool": self.pool
            }, **params)
        except TypeError:
            post_data = {"address": node_url, "pool": self.pool}
        post_data["register"] = "true"
        try:
            self.client.nodes.create(**post_data)
        except Exception as ex:
            raise NewNodeError("{}".format(ex))
        return True

    def create_new_node(self, iaas_template):
        actual_nodes_list = self.get_nodes()
        try:
            data = {
                "register": "false",
                "Metadata.template": iaas_template
            }
            create_stream = self.client.nodes.create(**data)
        except Exception as ex:
            raise NewNodeError("{}".format(ex))

        for l in create_stream:
            sys.stdout.write(".")

        new_nodes_list = self.get_nodes()
        new_node = set(new_nodes_list) - set(actual_nodes_list)
        if len(new_node) == 1:
            return new_node.pop()
        raise NewNodeError("New node not found on Tsuru")

    def get_machines_templates(self):
        try:
            machines_templates = self.client.templates.list()
        except Exception as ex:
            raise Exception('Error getting machines templates on tsuru: {}'
                            .format(ex))
        iaas_templates = []
        if machines_templates is None:
            return iaas_templates
        for template in machines_templates:
            for item in template['Data']:
                if 'pool' == item['Name'] and self.pool == item['Value']:
                    iaas_templates.append(template['Name'])
        return iaas_templates

    def remove_node_from_pool(self, node):
        params = {'no-rebalance': 'true', 'address': node}
        try:
            self.client.nodes.remove(**params)
        except Exception as ex:
            raise RemoveNodeFromPoolError("{}".format(ex))
        return True

    def remove_machine_from_iaas(self, node):
        machine_metadata = self.get_machine_metadata_from_iaas(node)
        if machine_metadata is None:
            raise RemoveMachineFromIaaSError("machine {} not found on IaaS".format(node))
        machine_id = machine_metadata['id']
        try:
            self.client.machines.delete(machine_id)
        except Exception as ex:
            raise RemoveMachineFromIaaSError("{}".format(ex), machine_id)
        return True

    def move_node_containers(self, node, new_node, cur_retry=0, max_retry=10, wait_timeout=180):
        node_from = self.get_address(node)
        node_to = self.get_address(new_node)
        if node_from is None or node_to is None:
            raise MoveNodeContainersError('node address {} or {} '
                                          '- are invalids'.format(node, new_node))

        try:
            move_progress = self.client.containers.move(src=node_from, dst=node_to)
        except Exception as ex:
            raise MoveNodeContainersError("{}".format(ex))

        moving_error = False
        no_data = True
        for move_msg in move_progress:
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

    @staticmethod
    def get_address(node_name):
        try:
            socket.inet_aton(node_name)
            return(node_name)
        except socket.error:
            return urlparse(node_name).hostname


def pool_recycle(pool_name, destroy_node=False, dry_mode=False, max_retry=10, wait_timeout=180,
                 docker_port='4243', docker_scheme='http'):
    pool_handler = TsuruPool(pool_name)
    pool_templates = pool_handler.get_machines_templates()
    if pool_templates == []:
        raise Exception('Pool "{}" does not contain any template associate'.format(pool_name))
    templates_len = len(pool_templates)
    template_idx = 0
    nodes_to_recycle = pool_handler.get_nodes()

    new_node = None
    for node in nodes_to_recycle:
        if dry_mode:
            sys.stdout.write('Creating new node on pool "{}" using "{}" '
                             'template\n'.format(pool_name, pool_templates[template_idx]))
            sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
            sys.stdout.write('Moving all containers on old node "{}" to new node\n'.format(node))
            template_idx = (template_idx + 1) % templates_len
            if destroy_node:
                sys.stdout.write('Machine {} removed from IaaS\n'.format(node))
            sys.stdout.write('\n')
            continue
        try:
            sys.stdout.write('Creating new node on pool "{}" '
                             'using {} template\n'.format(pool_name, pool_templates[template_idx]))
            new_node = {}
            new_node['address'] = pool_handler.create_new_node(pool_templates[template_idx])
            sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
            node_data = pool_handler.get_node_metadata(node)
            pool_handler.remove_node_from_pool(node)
            sys.stdout.write('Moving all containers from old node "{}"'
                             ' to new node "{}"\n'.format(node, new_node['address']))
            pool_handler.move_node_containers(node, new_node['address'], 0, max_retry, wait_timeout)
            template_idx = (template_idx + 1) % templates_len
            if destroy_node:
                pool_handler.remove_machine_from_iaas(node)
                sys.stdout.write('Machine {} removed from IaaS\n'.format(node))
            new_node = None
        except (MoveNodeContainersError, RemoveNodeFromPoolError, KeyboardInterrupt), e:
            ''' Try to re-insert node on pool '''
            pool_handler.add_node_to_pool(node, docker_port, docker_scheme, node_data)
            raise e
        except Exception, e:
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
    parsed = parser.parse_args(args)
    pool_recycle(parsed.pool, parsed.destroy_node, parsed.dry_run,
                 parsed.max_retry, parsed.timeout, parsed.docker_port, parsed.docker_scheme)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    pool_recycle_parser(args)

if __name__ == "__main__":
    main()
