#!/usr/bin/env python

# Copyright 2015 tsuru-pool-recycle-plugin authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import sys
import argparse
import socket
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

    def create_new_node(self, iaas_template, curr_try=0, max_retry=10, wait_timeout=60):
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
            continue

        new_nodes_list = self.get_nodes()
        new_node = set(new_nodes_list) - set(actual_nodes_list)
        if len(new_node) == 1:
            return new_node.pop()

        if curr_try == max_retry:
            raise NewNodeError("New node not found on Tsuru.")

        sys.stderr.write("Node creation failed. Retrying in {} seconds\n"
                         .format(wait_timeout))
        time.sleep(wait_timeout)
        return self.create_new_node(iaas_template=iaas_template,
                                    curr_try=curr_try+1, max_retry=max_retry,
                                    wait_timeout=wait_timeout)

    def get_machines_templates(self):
        try:
            machines_templates = self.client.templates.list()
        except Exception as ex:
            raise Exception('Error getting machines templates on tsuru: {}'
                            .format(ex))
        iaas_templates = []
        for template in machines_templates:
            for item in template['Data']:
                if 'pool' == item['Name'] and self.pool == item['Value']:
                    iaas_templates.append(template['Name'])
        return iaas_templates

    def remove_node(self, node, curr_try=0, max_retry=10, wait_timeout=60):
        params = {"destroy": "true", "address": node}
        try:
            self.client.nodes.remove(**params)
            eventArgs = {
                "kindname": "node.delete",
                "target.type": "node",
                "target.value": node,
            }
            running = True
            while running:
                event = self.client.events.list(**eventArgs)[0]
                running = event["Running"] == "true"
                if event["Error"] == "true":  # TODO: check this
                    raise RemoveNodeFromPoolError(event)
                if running:
                    sys.stdout.write("Node delete still running. Sleeping for 15 seconds.\n")
                    time.sleep(15)
        except Exception as ex:
            if curr_try == max_retry:
                raise RemoveNodeFromPoolError("Maximum number of retries exceeded: {}".format(ex))
            sys.stderr.write("Node delete failed: {}. Retrying in {} seconds.\n".format(ex, wait_timeout))
            time.sleep(wait_timeout)
            return self.remove_node(node, curr_try=curr_try+1,
                                    max_retry=max_retry,
                                    wait_timeout=wait_timeout)

        return True

    @staticmethod
    def get_address(node_name):
        try:
            socket.inet_aton(node_name)
            return(node_name)
        except socket.error:
            return urlparse(node_name).hostname


def pool_recycle(pool_name, dry_mode=False, max_retry=10, wait_timeout=60):
    pool_handler = TsuruPool(pool_name)
    pool_templates = pool_handler.get_machines_templates()
    if pool_templates == []:
        raise Exception('Pool "{}" does not contain any template associate'.format(pool_name))
    templates_len = len(pool_templates)
    template_idx = 0
    nodes_to_recycle = pool_handler.get_nodes()

    sys.stdout.write('Going to recycle {} node(s) from pool "{}" using {} templates.\n'
                     .format(len(nodes_to_recycle), pool_name, len(pool_templates)))

    new_node = None
    for node in nodes_to_recycle:
        if dry_mode:
            sys.stdout.write('Creating new node on pool "{}" using "{}" '
                             'template\n'.format(pool_name, pool_templates[template_idx]))
            sys.stdout.write('Destroying node "{}\n'.format(node))
            template_idx = (template_idx + 1) % templates_len
            sys.stdout.write('\n')
            continue
        sys.stdout.write('Creating new node on pool "{}" '
                         'using {} template\n'.format(pool_name, pool_templates[template_idx]))
        new_node = pool_handler.create_new_node(pool_templates[template_idx], wait_timeout=wait_timeout)
        sys.stdout.write('Node {} successfully created.\n'.format(new_node))
        sys.stdout.write('Removing node "{}" from pool "{}"\n'.format(node, pool_name))
        pool_handler.remove_node(node, max_retry=max_retry, wait_timeout=wait_timeout)
        template_idx = (template_idx + 1) % templates_len
        new_node = None


def pool_recycle_parser(args):
    parser = argparse.ArgumentParser(description="Tsuru pool nodes recycle")
    parser.add_argument("-p", "--pool", required=True,
                        help="Tsuru pool")
    parser.add_argument("-d", "--dry-run", required=False, action='store_true',
                        help="Dry run all recycle actions")
    parser.add_argument("-m", "--max_retry", required=False, default=10, type=int,
                        help="Max retries attempts to move a node on failure")
    parser.add_argument("-t", "--timeout", required=False, default=60, type=int,
                        help="Max timeout between moves on failures attempts")
    parsed = parser.parse_args(args)
    pool_recycle(parsed.pool, parsed.dry_run, parsed.max_retry, parsed.timeout)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    pool_recycle_parser(args)

if __name__ == "__main__":
    main()
