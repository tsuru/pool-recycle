# Copyright 2015 tsuru authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import sys
import argparse
import json
import urllib2

from urlparse import urlparse
from functools import partial


class TsuruPool(object):

    def __init__(self, pool, dry_mode=False):
        try:
            self.tsuru_target = os.environ['TSURU_TARGET'].rstrip("/")
            self.tsuru_token = os.environ['TSURU_TOKEN']
        except KeyError:
            raise KeyError("TSURU_TARGET or TSURU_TOKEN envs not set")
        self.pool = pool

    def get_nodes(self):
        return_code, docker_nodes = self.__tsuru_request("GET", "/docker/node")
        if return_code != 200:
            return None
        docker_nodes = json.loads(docker_nodes.read())
        pool_nodes = []
        if 'machines' in docker_nodes and docker_nodes['machines'] is not None:
            for node in docker_nodes['machines']:
                node_params = node['CreationParams']
                if 'pool' in node_params and node_params['pool'] == self.pool:
                    pool_nodes.append(node['Address'])
        if 'nodes' in docker_nodes and docker_nodes['nodes'] is not None:
            for node in docker_nodes['nodes']:
                if ('pool' in node['Metadata'] and
                   node['Metadata']['pool'] == self.pool):
                    pool_nodes.append(node['Address'])
        return pool_nodes

    def create_new_node(self, iaas_template):
        (return_code,
         body) = self.__tsuru_request("POST", "/docker/node?register=false",
                                              {'template': iaas_template})
        if return_code != 200:
            return False
        return True

    def get_machines_templates(self):
        (return_code,
         machines_templates) = self.__tsuru_request("GET", "/iaas/templates")
        if return_code != 200:
            return None
        machines_templates = json.loads(machines_templates.read())
        iaas_templates = []
        if machines_templates is None:
            return iaas_templates
        for template in machines_templates:
            for item in template['Data']:
                if 'pool' in item['Name'] and self.pool in item['Value']:
                    iaas_templates.append(template['Name'])
        return iaas_templates

    def remove_node_from_tsuru(self, node):
        return_code, msg = self.__tsuru_request("DELETE", "/docker/node",
                                                {'address': node})
        if return_code != 200:
            raise Exception('Error removing node from tsuru: "{}"'.format(msg))
        return True

    def move_node_containers(self, node, new_node):
        node_from = urlparse(node).hostname
        node_to = urlparse(new_node).hostname
        (return_code,
         move_progress) = self.__tsuru_request("POST",
                                               "/docker/containers/move",
                                               {'from': node_from, 'to': node_to})
        if return_code != 200:
            raise Exception('Error moving containers on tsuru: "{}"'.format(move_progress))
            return False

        moving_error = False
        for move_msg in self.json_parser(move_progress):
            if 'Error moving' in move_msg['Message']:
                moving_error = True
                sys.stderr.write("{}\n".format(move_msg['Message']))
            else:
                sys.stdout.write("{}\n".format(move_msg['Message']))

        if moving_error:
            return False
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
    def json_parser(fileobj, decoder=json.JSONDecoder(), buffersize=2048):
        buffer = ''
        first_chunk = True
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
                        buffer = buffer[index_first_chunk:]
                        first_chunk = False
                    # Not enough data to decode, read more
                    break


def pool_recycle(pool_name, destroy_node=False, dry_mode=False):
    pool_handler = TsuruPool(pool_name, dry_mode)
    for node in pool_handler.get_nodes():
        try:
            new_node = pool_handler.create_new_node(node['iaas_template'])
            pool_handler.remove_node_from_pool(node)
            pool_handler.move_node_containers(node, new_node)
        except:
            """ Verify possible exceptions and actions """

        pool_handler.remove_node_from_tsuru(node, destroy_node)


def pool_recycle_parser(args):
    parser = argparse.ArgumentParser(description="Tsuru pool nodes recycle")
    parser.add_argument("-p", "--pool", required=True,
                        help="Docker tsuru pool")
    parser.add_argument("-r", "--destroy-node", required=False,
                        help="Destroy olds docker nodes after recycle")
    parser.add_argument("-d", "--dry-run", required=False, action='store_true',
                        help="Dry run all recycle actions")
    parsed = parser.parse_args(args)
    pool_recycle(parsed.pool, parsed.destroy_node, parsed.dry_run)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    pool_recycle_parser(args)

if __name__ == "__main__":
    main()
