# Copyright 2015 tsuru-pool-recycle-plugin authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import unittest
import urllib2
import json

from io import StringIO
from mock import patch, Mock, call
from pool_recycle import plugin
from pool_recycle.plugin import MoveNodeContainersError, RemoveNodeFromPoolError


class FakeTsuruPool(object):

    def __init__(self, pool, move_node_containers_error=False, remove_node_from_tsuru_error=False):
        self.pool = pool
        self.nodes = ['127.0.0.1', 'http://10.10.1.1:2222', '10.1.1.1']
        self.new_nodes = ['1.2.3.4', '5.6.7.8', '9.10.11.12']
        self.move_node_containers_error = move_node_containers_error
        self.remove_node_from_tsuru_error = remove_node_from_tsuru_error

    def get_machines_templates(self):
        return ['templateA', 'templateB']

    def get_nodes(self):
        return self.nodes

    def remove_node_from_tsuru(self, node):
        if self.remove_node_from_tsuru_error:
            raise RemoveNodeFromPoolError("error on node {}".format(node))
        return True

    def move_node_containers(self, node, new_node):
        if self.move_node_containers_error:
            raise MoveNodeContainersError("error moving {} to {}".format(node, new_node))
        return True

    def create_new_node(self, template):
        return self.new_nodes.pop(0)

    def add_node_to_pool(self, node_url, docker_port, docker_scheme):
        return True


class FakeURLopenResponse(StringIO):

    def __init__(self, *args):
        try:
            self.code = args[1]
        except IndexError:
            self.code = 200
            pass
        try:
            self.msg = args[2]
        except IndexError:
            self.msg = "OK"
            pass
        self.headers = {'content-type': 'text/plain; charset=utf-8'}
        StringIO.__init__(self, unicode(args[0]))

    def getcode(self):
        return self.code


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

    def test_get_nodes_from_pool(self):
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
        docker_nodes_null = '{ "machines": null, "nodes": null }'
        self.urlopen_mock.side_effect = [FakeURLopenResponse(docker_nodes_json),
                                         FakeURLopenResponse(docker_nodes_null)]
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_nodes(), ['http://10.23.26.76:4243',
                                                        'http://10.25.23.138:4243'])
        self.assertListEqual(pool_handler.get_nodes(), [])

    def test_create_new_node(self):
        self.urlopen_mock.return_value = FakeURLopenResponse(None, 200)
        pool_handler = plugin.TsuruPool("foobar")
        pool_handler.get_nodes = Mock()
        pool_handler.get_nodes.side_effect = [['192.168.1.1', 'http://10.1.1.1:2723',
                                               '10.10.10.1'],
                                              ['192.168.1.1', '10.2.3.2', '10.10.10.1',
                                               'http://10.1.1.1:2723']]
        return_new_node = pool_handler.create_new_node("my_template")
        self.assertEqual(return_new_node, '10.2.3.2')

    @patch.object(plugin.TsuruPool, '_TsuruPool__tsuru_request')
    def test_add_node_to_pool(self, mocked_tsuru_request):
        mocked_tsuru_request.return_value = (200, None)
        pool_handler = plugin.TsuruPool("foobar")
        pool_handler.add_node_to_pool('127.0.0.1', '4243', 'http')
        node_add_dict = {'address': 'http://127.0.0.1:4243', 'pool': 'foobar'}
        mocked_tsuru_request.assert_called_once_with("POST", "/docker/node?register=false",
                                                     node_add_dict)

    def test_return_machines_templates(self):
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
    }
]
        '''
        self.urlopen_mock.side_effect = [FakeURLopenResponse(machines_templates_json, 200),
                                         FakeURLopenResponse(None, 500)]
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_machines_templates(),
                             ['template_red', 'template_yellow'])
        self.assertRaisesRegexp(Exception, 'Error getting machines templates',
                                pool_handler.get_machines_templates)

    def test_remove_node_from_tsuru(self):
        http_error = urllib2.HTTPError(None, 500, None, None, StringIO(u"No such node in storage"))
        self.urlopen_mock.side_effect = [FakeURLopenResponse(None, 200), http_error]
        pool_handler = plugin.TsuruPool("foobar")
        return_remove_node = pool_handler.remove_node_from_tsuru('http://127.0.0.1:4243')
        self.assertEqual(return_remove_node, True)
        self.assertRaisesRegexp(Exception, 'No such node in storage',
                                pool_handler.remove_node_from_tsuru,
                                'http://127.0.0.1:4243')

    @patch("sys.stderr")
    @patch("sys.stdout")
    def test_move_node_containers(self, stdout, stderr):
        fake_buffer_error = ['garbage in first chunk\ {"Message":"Moving 2 units..."}\n'
                             '{"Message":"Error moving unit: abcd1234"}\n',
                             '{"Message":"Error moving unit: xyzabcd234"}\n']
        fake_buffer_error = "".join([x for x in fake_buffer_error])

        fake_buffer_successfully = '{ "Message":"Container moved successfully" }'

        fake_empty_buffer = ' '

        self.urlopen_mock.side_effect = [FakeURLopenResponse(fake_buffer_error, 200),
                                         FakeURLopenResponse(fake_buffer_successfully, 200),
                                         FakeURLopenResponse(fake_empty_buffer, 200)]

        pool_handler = plugin.TsuruPool("foobar")

        move_return_value = pool_handler.move_node_containers('http://10.10.1.2:123', 'https://1.2.3.4')
        stdout.write.assert_called_with("Moving 2 units...\n")
        stderr.write.assert_has_call("Error moving unit: abcd1234\n")
        stderr.write.assert_has_call("Error moving unit: xyzabcd234\n")
        self.assertEqual(move_return_value, False)

        move_return_value_2 = pool_handler.move_node_containers('http://10.1.1.2:123', '1.2.3.7')
        stdout.write.assert_called_with("{}\n".format(json.loads(fake_buffer_successfully)['Message']))
        self.assertEqual(move_return_value_2, True)

        move_return_value_3 = pool_handler.move_node_containers('http://10.10.1.2:123', 'http://1.2.3.4:432')
        self.assertEqual(move_return_value_3, True)
        self.assertRaisesRegexp(MoveNodeContainersError, 'node address .+ are invalids',
                                pool_handler.move_node_containers,
                                'http://10.10.1.2:123', '1.2.3.4:432')

    @patch("time.sleep")
    @patch("sys.stderr")
    @patch("sys.stdout")
    def test_move_node_containers_docker_connection_error(self, stdout, stderr, sleep):
        fake_buffer_docker_connection_error = ['{"Message":"Moving 2 units..."}\n'
                                               '{"Message":"Error moving unit: abcd1234"}\n'
                                               '{"Message":"Error moving container: Error moving'
                                               ' unit: cannot connect to Docker endpoint"}\n'
                                               '{"Message":"Error moving unit: xyzabcd234"}\n',
                                               '{"Message":"Moving 2 units..."}\n'
                                               '{"Message":"Error moving unit: abcd1234"}\n'
                                               '{"Message":"Error moving container: Error moving unit:'
                                               ' cannot connect to Docker endpoint"}\n'
                                               '{"Message":"Error moving unit: xyzabcd234"}\n',
                                               '{"Message": "Moving unit abcd1234"}\n'
                                               '{"Message": "Moving unit xyzabc234"}\n'
                                               '{"Message": "Container moved successfully"}\n']

        docker_connection_error = [FakeURLopenResponse(fake_buffer_docker_connection_error[0], 200),
                                   FakeURLopenResponse(fake_buffer_docker_connection_error[1], 200),
                                   FakeURLopenResponse(fake_buffer_docker_connection_error[2], 200)]

        self.urlopen_mock.side_effect = docker_connection_error

        pool_handler = plugin.TsuruPool("foobar")

        move_return_value = pool_handler.move_node_containers('http://1.2.3.4:123', 'http://5.6.7.8:234')
        self.assertEqual(move_return_value, True)

        stderr_calls = []
        for message_block in fake_buffer_docker_connection_error:
            for line in message_block.split('\n'):
                if line is not '' and 'Error' in line:
                    message = json.loads(line)['Message']
                    stderr_calls.append(call(str(message + '\n')))

        stdout_calls = [call('Moving 2 units...\n'),
                        call('Retrying move containers from http://1.2.3.4:123 to'
                             ' http://5.6.7.8:234. Waiting for 180 seconds...'),
                        call('Moving 2 units...\n'),
                        call('Retrying move containers from http://1.2.3.4:123 to'
                             ' http://5.6.7.8:234. Waiting for 180 seconds...'),
                        call('Moving unit abcd1234\n'),
                        call('Moving unit xyzabc234\n'),
                        call('Container moved successfully\n')]

        self.assertEqual(stdout.write.call_args_list, stdout_calls)
        self.assertEqual(stderr.write.call_args_list, stderr_calls)
        sleep.assert_has_calls([call(180), call(180)])

    @patch("sys.stdout")
    @patch('pool_recycle.plugin.TsuruPool.get_nodes')
    @patch('pool_recycle.plugin.TsuruPool.get_machines_templates')
    def test_pool_recycle_on_dry_mode(self, get_machines_templates, get_nodes, stdout):
        get_machines_templates.return_value = ['templateA', 'templateB', 'templateC']
        get_nodes.return_value = ['http://127.0.0.1:4243', '10.10.2.2',
                                  '10.2.3.2', 'http://2.3.2.1:2123']
        plugin.pool_recycle('foobar', False, True)
        call_stdout_list = [call('Creating new node on pool "foobar" using "templateA" template\n'),
                            call('Removing node "http://127.0.0.1:4243" from pool "foobar"\n'),
                            call('Moving all containers on old node "http://127.0.0.1:4243" to new node\n\n'),
                            call('Creating new node on pool "foobar" using "templateB" template\n'),
                            call('Removing node "10.10.2.2" from pool "foobar"\n'),
                            call('Moving all containers on old node "10.10.2.2" to new node\n\n'),
                            call('Creating new node on pool "foobar" using "templateC" template\n'),
                            call('Removing node "10.2.3.2" from pool "foobar"\n'),
                            call('Moving all containers on old node "10.2.3.2" to new node\n\n'),
                            call('Creating new node on pool "foobar" using "templateA" template\n'),
                            call('Removing node "http://2.3.2.1:2123" from pool "foobar"\n'),
                            call('Moving all containers on old node "http://2.3.2.1:2123" to new node\n\n')]

        self.assertEqual(stdout.write.call_args_list, call_stdout_list)

    @patch("sys.stdout")
    @patch('pool_recycle.plugin.TsuruPool')
    def test_pool_recycle_success(self, tsuru_pool_mock, stdout):
        tsuru_pool_mock.return_value = FakeTsuruPool('foobar')
        plugin.pool_recycle('foobar')
        call_stdout_list = [call('Creating new node on pool "foobar" using templateA template\n'),
                            call('Removing node "127.0.0.1" from pool "foobar"\n'),
                            call('Moving all containers from old node "127.0.0.1" to new node "1.2.3.4"\n'),
                            call('Creating new node on pool "foobar" using templateB template\n'),
                            call('Removing node "http://10.10.1.1:2222" from pool "foobar"\n'),
                            call('Moving all containers from old node "http://10.10.1.1:2222" '
                                 'to new node "5.6.7.8"\n'),
                            call('Creating new node on pool "foobar" using templateA template\n'),
                            call('Removing node "10.1.1.1" from pool "foobar"\n'),
                            call('Moving all containers from old node "10.1.1.1" to new node "9.10.11.12"\n')]
        stdout.write.assert_has_calls(call_stdout_list)

    @patch('sys.stderr')
    @patch('sys.stdout')
    @patch('pool_recycle.plugin.TsuruPool')
    def test_pool_recycle_error_on_moving_containers(self, tsuru_pool_mock, stdout, stderr):
        tsuru_pool_mock.return_value = FakeTsuruPool('foobar', True)
        with self.assertRaises(SystemExit):
            plugin.pool_recycle('foobar')
        call_stdout_list = [call('Creating new node on pool "foobar" using templateA template\n'),
                            call('Removing node "127.0.0.1" from pool "foobar"\n'),
                            call('Moving all containers from old node "127.0.0.1" to new node "1.2.3.4"\n')]
        stdout.write.assert_has_calls(call_stdout_list)
        stderr.write.assert_called_once_with('Error: error moving 127.0.0.1 to 1.2.3.4\n')

    def tearDown(self):
        self.patcher.stop()
