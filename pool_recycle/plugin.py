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

try:
    from tsuruclient import client
except:
    sys.stderr.write("This plugin requires tsuruclient module: https://pypi.python.org/pypi/tsuruclient\n")
    sys.exit(1)


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
        try:
            self.user = self.client.users.info()
        except Exception as ex:
            raise Exception("Failed to get current user info: {}".format(ex))
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

    def create_new_node(self, iaas_template, curr_try=0, max_retry=10,
                        retry_interval=60):
        try:
            data = {
                "register": "false",
                "Metadata.template": iaas_template
            }
            self.client.nodes.create(**data)
            eventArgs = {
                "ownername": self.user["Email"],
                "kindname": "node.create",
            }
            event = self.wait_event("Node create", max_retry=max_retry, **eventArgs)
        except Exception as ex:
            if curr_try == max_retry:
                raise NewNodeError("Maximum number of retries exceeded: {}"
                                   .format(ex))
            sys.stderr.write("Node creation failed: {}. Retrying in {} seconds\n"
                             .format(ex, retry_interval))
            time.sleep(retry_interval)
            return self.create_new_node(iaas_template=iaas_template,
                                        curr_try=curr_try+1,
                                        max_retry=max_retry,
                                        retry_interval=retry_interval)
        return event["Target"]["Value"]

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

    def wait_event(self, msg, max_retry=10, **kwargs):
        running = True
        curr_try = 0
        while running:
            try:
                event = self.client.events.list(**kwargs)[0]
            except Exception, ex:
                if curr_try == max_retry:
                    sys.stderr.write("Failed to retrieve event.")
                    raise ex
                curr_try = curr_try + 1
                sys.stderr.write("Failed to get event. Retrying in 15 seconds.")
                time.sleep(15)
                continue
            curr_try = 0
            running = event["Running"]
            if event["Error"] != "":
                raise Exception(event["Error"])
            if running:
                sys.stdout.write("{} still running. Sleeping for 15 seconds.\n"
                                 .format(msg))
                time.sleep(15)
        return event

    def remove_node(self, node, curr_try=0, max_retry=10, retry_interval=60):
        params = {"remove-iaas": "true", "address": node}
        try:
            self.client.nodes.remove(**params)
            eventArgs = {
                "kindname": "node.delete",
                "target.type": "node",
                "target.value": node,
            }
            self.wait_event("Node delete", max_retry=max_retry, **eventArgs)
        except Exception as ex:
            if curr_try == max_retry:
                raise RemoveNodeFromPoolError("Maximum number of retries exceeded: {}".format(ex))
            sys.stderr.write("Node delete failed: {}. Retrying in {} seconds.\n"
                             .format(ex, retry_interval))
            time.sleep(retry_interval)
            return self.remove_node(node, curr_try=curr_try+1,
                                    max_retry=max_retry,
                                    retry_interval=retry_interval)

        return True

    @staticmethod
    def get_address(node_name):
        try:
            socket.inet_aton(node_name)
            return(node_name)
        except socket.error:
            return urlparse(node_name).hostname

    def disable_healing(self):
        sys.stdout.write("Disabling healing for pool.\n")
        healing = self.client.healings.list()
        if self.pool in healing:
            def clean_up():
                sys.stdout.write("Re-enabling healing for pool.\n")
                self.client.healings.update(**{"pool": self.pool,
                                               "Enabled": healing[self.pool]["Enabled"]})
        else:
            def clean_up():
                sys.stdout.write("Removing disabled healing.\n")
                self.client.healings.remove(self.pool)
        self.client.healings.update(**{"pool": self.pool, "Enabled": False})
        return clean_up


def pool_recycle(pool_name, dry_mode=False, max_retry=10, retry_interval=60):
    pool_handler = TsuruPool(pool_name)
    pool_templates = pool_handler.get_machines_templates()
    if pool_templates == []:
        raise Exception('Pool "{}" does not contain any template associate'.format(pool_name))
    templates_len = len(pool_templates)
    template_idx = 0
    nodes_to_recycle = pool_handler.get_nodes()
    recycle_len = len(nodes_to_recycle)
    sys.stdout.write('Going to recycle {} node(s) from pool "{}" using {} templates.\n'
                     .format(recycle_len, pool_name, len(pool_templates)))
    enable_healing = pool_handler.disable_healing()
    new_node = None
    for idx, node in enumerate(nodes_to_recycle):
        sys.stdout.write('({}/{}) Creating new node on pool "{}" '
                         'using "{}" template\n'
                         .format(idx+1, recycle_len,
                                 pool_name, pool_templates[template_idx]))

        if dry_mode:
            sys.stdout.write('Destroying node "{}\n'.format(node))
            template_idx = (template_idx + 1) % templates_len
            sys.stdout.write('\n')
            continue

        try:
            new_node = pool_handler.create_new_node(pool_templates[template_idx],
                                                    retry_interval=retry_interval)
            sys.stdout.write('Node {} successfully created.\n'.format(new_node))
            sys.stdout.write('Removing node "{}" from pool "{}"\n'
                             .format(node, pool_name))
            pool_handler.remove_node(node, max_retry=max_retry,
                                     retry_interval=retry_interval)
            template_idx = (template_idx + 1) % templates_len
            new_node = None
        except (Exception, KeyboardInterrupt), e:
            sys.stderr.write("Failed: {}\n".format(e))
            enable_healing()
            sys.exit(1)

    enable_healing()
    sys.stdout.write('Done.\n')


def pool_recycle_parser(args):
    parser = argparse.ArgumentParser(description="Tsuru pool nodes recycle")
    parser.add_argument("-p", "--pool", required=True,
                        help="Tsuru pool")
    parser.add_argument("-d", "--dry-run", required=False, action='store_true',
                        help="Dry run all recycle actions")
    parser.add_argument("-m", "--max_retry", required=False, default=10, type=int,
                        help="Max retries attempts to move a node on failure")
    parser.add_argument("-i", "--retry-interval", required=False, default=60, type=int,
                        help="Time, in seconds, between retry attempts.")
    parsed = parser.parse_args(args)
    pool_recycle(parsed.pool, parsed.dry_run, parsed.max_retry,
                 parsed.retry_interval)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    pool_recycle_parser(args)

if __name__ == "__main__":
    main()
