# Copyright 2015 tsuru-pool-recycle-plugin authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import unittest
import json

from mock import patch, Mock, call
from pool_recycle import plugin
from pool_recycle.plugin import (RemoveNodeFromPoolError, NewNodeError)


class FakeTsuruPool(object):

    def __init__(self, pool, move_node_containers_error=False, remove_node_from_pool_error=False,
                 remove_machine_from_iaas_error=False, pre_provision_error=False,
                 raise_errors_on_call_counter=0):
        self.pool = pool
        self.nodes_on_pool = ['127.0.0.1', '10.10.1.1', '10.1.1.2']
        self.machines_on_pool = ['127.0.0.1', '10.10.1.1', '10.1.1.2']
        self.new_nodes = ['1.2.3.4', '5.6.7.8', '9.10.11.12']
        self.move_node_containers_error = move_node_containers_error
        self.remove_node_from_pool_error = remove_node_from_pool_error
        self.remove_machine_from_iaas_error = remove_machine_from_iaas_error
        self.pre_provision_error = pre_provision_error
        self.call_count = 0
        self.raise_errors_on_call_counter = raise_errors_on_call_counter

    def get_machines_templates(self):
        return ['templateA', 'templateB']

    def get_nodes(self):
        return list(self.nodes_on_pool)

    def get_machines(self):
        return list(self.machines_on_pool)

    def remove_node_from_pool(self, node):
        if self.remove_node_from_pool_error and self.call_count >= self.raise_errors_on_call_counter:
            raise RemoveNodeFromPoolError("error on node {}".format(node))
        self.call_count += 1
        self.nodes_on_pool.remove(node)

    def create_new_node(self, template):
        if self.pre_provision_error and self.call_count >= self.raise_errors_on_call_counter:
            raise NewNodeError("error adding new node on IaaS")
        new_node = self.new_nodes.pop(0)
        self.nodes_on_pool.append(new_node)
        self.machines_on_pool.append(new_node)
        self.call_count += 1
        return new_node

    def add_node_to_pool(self, node_url, docker_port, docker_scheme, metadata):
        self.nodes_on_pool.append(node_url)
        return True

    def get_machine_metadata_from_iaas(self, node):
        return {'id': '001122', 'metadata': {'bla': 'ble', 'xxx': 'yyy'}}

    def get_node_metadata(self, node):
        return {'bla': 'ble', 'xxx': 'yyy'}


class TsuruPoolTestCase(unittest.TestCase):

    def setUp(self):
        os.environ["TSURU_TARGET"] = "https://cloud.tsuru.io/"
        os.environ["TSURU_TOKEN"] = "abc123"
        self.patcher = patch('urllib2.urlopen')
        self.urlopen_mock = self.patcher.start()

    def test_missing_env_var(self):
        del os.environ['TSURU_TOKEN']
        self.assertRaisesRegexp(KeyError,
                                "TSURU_TARGET or TSURU_TOKEN envs not set",
                                plugin.TsuruPool, "foobar")

    @patch('tsuruclient.nodes.Manager.list')
    def test_get_nodes_from_pool(self, mock):
        docker_nodes_json = '''
{
    "machines": [
        {
            "Id": "f04388e3-02e0-46ec-93c8-9e5ba095eeb8",
            "Iaas": "cloudstack",
            "Status": "running",
            "Address": "10.10.34.221",
            "CreationParams": {
                "displayname": "machine_a",
                "pool": "foobar"
            }
        },
        {
            "Id": "c56ba117-cac2-4aba-b3c9-bc273ca79db0",
            "Iaas": "cloudstack",
            "Status": "running",
            "Address": "10.20.42.42",
            "CreationParams": {
                "displayname": "machine_b",
                "pool": "bilbo"
            }
        },
        {
            "Id": "c059bfb9-7323-41a4-96dc-a44b8c5d97da",
            "Iaas": "cloudstack",
            "Status": "running",
            "Address": "10.30.33.182",
            "CreationParams": {
                "displayname": "machine_c"
            }
        }
    ],
    "nodes": [
        {
            "Address": "http://10.2.25.169:4243",
            "Metadata": {
                "pool": "bilbo"
            },
            "Status": "waiting"
        },
        {
            "Address": "http://10.23.26.76:4243",
            "Metadata": {
                "pool": "foobar"
            },
            "Status": "waiting"
        },
        {
            "Address": "http://10.25.23.138:4243",
            "Metadata": {
                "LastSuccess": "2015-02-04T11:47:54-02:00",
                "pool": "foobar"
            },
            "Status": "ready"
        }
    ]
}
        '''
        mock.return_value = json.loads(docker_nodes_json)
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_nodes(), ['http://10.23.26.76:4243',
                                                        'http://10.25.23.138:4243'])

        docker_nodes_null = '{ "machines": null, "nodes": null }'
        mock.return_value = json.loads(docker_nodes_null)
        self.assertListEqual(pool_handler.get_nodes(), [])

    @patch('tsuruclient.nodes.Manager.create')
    def test_create_new_node(self, mock):
        mock.return_value = {}
        pool_handler = plugin.TsuruPool("foobar")
        pool_handler.get_nodes = Mock()
        pool_handler.get_nodes.side_effect = [['192.168.1.1', 'http://10.1.1.1:2723',
                                               '10.10.10.1'],
                                              ['192.168.1.1', '10.2.3.2', '10.10.10.1',
                                               'http://10.1.1.1:2723']]
        return_new_node = pool_handler.create_new_node("my_template")
        self.assertEqual(return_new_node, '10.2.3.2')

    @patch('tsuruclient.templates.Manager.list')
    def test_return_machines_templates(self, mock):
        machines_templates_json = '''
[
    {
        "Name": "template_red",
        "IaaSName": "cloudstack_prod",
        "Data": [
            {
                "Name": "pool",
                "Value": "foobar"
            },
            {
                "Name": "projectid",
                "Value": "222f0798-e472-4216-a8ed-ce1950f419e8"
            },
            {
                "Name": "displayname",
                "Value": "test_a"
            },
            {
                "Name": "networkids",
                "Value": "513ef8b6-bd98-4e6b-89a6-6ca8a859fbb4"
            }
        ]
    },
    {
        "Name": "template_blue",
        "IaaSName": "cloudstack_prod",
        "Data": [
            {
                "Name": "pool",
                "Value": "infra"
            },
            {
                "Name": "projectid",
                "Value": "222f0798-e472-4216-a8ed-ce1950f419e8"
            },
            {
                "Name": "displayname",
                "Value": "test_infra"
            },
            {
                "Name": "networkids",
                "Value": "97d7ad56-62b4-4d43-805a-2aee42619ac6"
            }
        ]
    },
    {
        "Name": "template_yellow",
        "IaaSName": "cloudstack_dev",
        "Data": [
            {
                "Name": "pool",
                "Value": "foobar"
            },
            {
                "Name": "projectid",
                "Value": "222f0798-e472-4216-a8ed-ce1950f419e8"
            },
            {
                "Name": "displayname",
                "Value": "docker_xxx"
            },
            {
                "Name": "networkids",
                "Value": "97d7ad56-62b4-4d43-805a-2aee42619ac6"
            }
        ]
    },
    {
        "Name": "template_green",
        "IaaSName": "cloudstack_dev",
        "Data": [
            {
                "Name": "pool",
                "Value": "xxx_foobar"
            },
            {
                "Name": "projectid",
                "Value": "222f0798-e472-4216-a8ed-ce1950f419e8"
            },
            {
                "Name": "displayname",
                "Value": "docker_xxx"
            },
            {
                "Name": "networkids",
                "Value": "97d7ad56-62b4-4d43-805a-2aee42619ac6"
            }
        ]
    }
]
        '''
        mock.return_value = json.loads(machines_templates_json)
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_machines_templates(),
                             ['template_red', 'template_yellow'])
        mock.side_effect = Exception()
        self.assertRaisesRegexp(Exception, 'Error getting machines templates',
                                pool_handler.get_machines_templates)

    @patch('tsuruclient.events.Manager.list')
    @patch('tsuruclient.nodes.Manager.remove')
    def test_remove_node(self, mock_delete, mock_events):
        mock_events.return_value = [{"Running": "false", "Error": ""}]
        mock_delete.return_value = {}
        pool_handler = plugin.TsuruPool("foobar")
        return_remove_node = pool_handler.remove_node('http://127.0.0.1:4243',
                                                      max_retry=0)
        self.assertEqual(return_remove_node, True)
        mock_delete.side_effect = Exception("No such node in storage")
        self.assertRaisesRegexp(Exception, 'No such node in storage',
                                pool_handler.remove_node,
                                'http://127.0.0.1:4243', 0, 0)

    @patch("sys.stdout")
    @patch('pool_recycle.plugin.TsuruPool.get_nodes')
    @patch('pool_recycle.plugin.TsuruPool.get_machines_templates')
    def test_pool_recycle_on_dry_mode(self, get_machines_templates, get_nodes, stdout):
        get_machines_templates.return_value = ['templateA', 'templateB', 'templateC']
        get_nodes.return_value = ['http://127.0.0.1:4243', '10.10.2.2',
                                  '10.2.3.2', 'http://2.3.2.1:2123']
        plugin.pool_recycle('foobar', True)
        call_stdout_list = [call('Going to recycle 4 node(s) from pool "foobar" using 3 templates.\n'),
                            call('(1/4) Creating new node on pool "foobar" using "templateA" template\n'),
                            call('Destroying node "http://127.0.0.1:4243\n'),
                            call('\n'),
                            call('(2/4) Creating new node on pool "foobar" using "templateB" template\n'),
                            call('Destroying node "10.10.2.2\n'),
                            call('\n'),
                            call('(3/4) Creating new node on pool "foobar" using "templateC" template\n'),
                            call('Destroying node "10.2.3.2\n'),
                            call('\n'),
                            call('(4/4) Creating new node on pool "foobar" using "templateA" template\n'),
                            call('Destroying node "http://2.3.2.1:2123\n'),
                            call('\n')]

        self.assertEqual(stdout.write.call_args_list, call_stdout_list)

    @patch("sys.stdout")
    @patch('pool_recycle.plugin.TsuruPool')
    def test_pool_recycle_success_removing_node_from_iaas(self, tsuru_pool_mock, stdout):
        tsuru_pool_mock.return_value = FakeTsuruPool('foobar')
        plugin.pool_recycle('foobar', True)
        call_stdout_list = [call('Going to recycle 3 node(s) from pool "foobar" using 2 templates.\n'),
                            call('(1/3) Creating new node on pool "foobar" using "templateA" template\n'),
                            call('Destroying node "127.0.0.1\n'),
                            call('\n'),
                            call('(2/3) Creating new node on pool "foobar" using "templateB" template\n'),
                            call('Destroying node "10.10.1.1\n'),
                            call('\n'),
                            call('(3/3) Creating new node on pool "foobar" using "templateA" template\n'),
                            call('Destroying node "10.1.1.2\n'),
                            call('\n')]
        stdout.write.assert_has_calls(call_stdout_list)

    @patch('sys.stderr')
    @patch('sys.stdout')
    @patch('pool_recycle.plugin.pool_recycle')
    def test_pool_recycle_parser_with_all_options_set(self, pool_recycle, stdout, stderr):
        args = ["-p", "foobar", "-d", "-m", "100", "-i", "30"]
        plugin.pool_recycle_parser(args)
        pool_recycle.assert_called_once_with('foobar', True, 100, 30)

    def tearDown(self):
        self.patcher.stop()
