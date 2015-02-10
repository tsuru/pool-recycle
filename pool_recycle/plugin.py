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
            raise Exception('Error get nodes from '
                            'tsuru: "{}"'.format(docker_nodes))
        docker_nodes = json.loads(docker_nodes.read())
        pool_nodes = []
        if 'nodes' in docker_nodes and docker_nodes['nodes'] is not None:
            for node in docker_nodes['nodes']:
                if ('pool' in node['Metadata'] and
                   node['Metadata']['pool'] == self.pool):
                    pool_nodes.append(node['Address'])
        return pool_nodes

    def add_node_to_pool(self, node_url, docker_port, docker_scheme):
        if not (re.match(r'^https?://', node_url)):
            node_url = '{}://{}:{}'.format(docker_scheme, node_url, docker_port)
        (return_code,
         msg) = self.__tsuru_request("POST", "/docker/node?register=false",
                                             {"address": node_url, "pool": self.pool})
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
        return False

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
                if 'pool' in item['Name'] and self.pool in item['Value']:
                    iaas_templates.append(template['Name'])
        return iaas_templates

    def remove_node_from_tsuru(self, node, destroy_node=False):
        headers = {'address': node}
        if destroy_node:
            headers['remove_iaas'] = "true"
        return_code, msg = self.__tsuru_request("DELETE", "/docker/node",
                                                headers)
        if return_code not in [200, 201, 204]:
            raise RemoveNodeFromPoolError(msg)
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
            time.sleep(wait_timeout)
            self.move_node_containers(node, new_node, (cur_retry + 1), max_retry)
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


def pool_recycle(pool_name, destroy_node=False, dry_mode=False, docker_port='4243',
                 docker_scheme='http'):
    pool_handler = TsuruPool(pool_name)
    pool_templates = pool_handler.get_machines_templates()
    if pool_templates == []:
        raise Exception('Pool "{}" does not contain any template associate'.format(pool_name))
    templates_len = len(pool_templates)
    template_idx = 0
    for node in pool_handler.get_nodes():
        if dry_mode:
            sys.stdout.write('Creating new node on pool "{}" using "{}" '
                             'template\n'.format(pool_name, pool_templates[template_idx]))
            sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
            sys.stdout.write('Moving all containers on old node "{}" to new node\n\n'.format(node))
            template_idx += 1
            if template_idx >= templates_len:
                template_idx = 0
            continue
        try:
            sys.stdout.write('Creating new node on pool "{}" '
                             'using {} template\n'.format(pool_name, pool_templates[template_idx]))
            new_node = pool_handler.create_new_node(pool_templates[template_idx])
            sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
            pool_handler.remove_node_from_tsuru(node)
            sys.stdout.write('Moving all containers from old node "{}"'
                             ' to new node "{}"\n'.format(node, new_node))
            pool_handler.move_node_containers(node, new_node)
            template_idx += 1
            if template_idx >= templates_len:
                template_idx = 0
        except (MoveNodeContainersError, RemoveNodeFromPoolError), e:
            ''' Try to re-insert node on pool '''
            pool_handler.add_node_to_pool(node, docker_port, docker_scheme)
            sys.stderr.write('Error: {}\n'.format(e.message))
            sys.exit(1)
        except Exception, e:
            sys.stderr.write('Error: {}\n'.format(e.message))
            sys.exit(1)


def pool_recycle_parser(args):
    parser = argparse.ArgumentParser(description="Tsuru pool nodes recycle")
    parser.add_argument("-p", "--pool", required=True,
                        help="Docker tsuru pool")
    parser.add_argument("-r", "--destroy-node", required=False,
                        help="Destroy olds docker nodes after recycle")
    parser.add_argument("-d", "--dry-run", required=False, action='store_true',
                        help="Dry run all recycle actions")
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
                 parsed.docker_port, parsed.docker_scheme)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    pool_recycle_parser(args)

if __name__ == "__main__":
    main()
