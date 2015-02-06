# Copyright 2015 tsuru authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import sys
import argparse
import json
from urlparse import urlparse
import urllib2


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
                    docker_host = urlparse(node['Address']).hostname
                    pool_nodes.append(docker_host)
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
        return_code, _ = self.__tsuru_request("DELETE", "/docker/node",
                                              {'address': node})
        if return_code != 200:
            return False
        return True

    def move_node_containers(self, node, new_node):
        (return_code,
         move_progress) = self.__tsuru_request("POST",
                                               "/docker/containers/move",
                                               {'from': node, 'to': new_node})
        if return_code != 200:
            return False

        data = move_progress.read(8192)

        while data:
            move_msg = json.loads(data)
            if 'Error moving' in move_msg['Message']:
                moving_error = True
            print move_msg['Message']
            data = move_progress.read(8192)

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

        response = urllib2.urlopen(request)
        return response.getcode(), response


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
