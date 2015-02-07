# Copyright 2015 tsuru authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import unittest
import urllib2
import json

from io import StringIO
from mock import patch
from pool_recycle import plugin


class MockResponse(StringIO):

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
        StringIO.__init__(self, args[0])

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
        docker_nodes_json = u'''
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
                "pool": "whatever"
            },
            "Status": "ready"
        }
    ]
}
        '''
        docker_nodes_null_machines = u'''
{
    "machines": null,
    "nodes": [
        {
            "Address": "http://127.0.0.1:2375",
            "Metadata": {
                "LastSuccess": "2015-02-05T11:46:42Z",
                "pool": "foobar"
            },
            "Status": "ready"
        }
    ]
}
        '''
        self.urlopen_mock.side_effect = [MockResponse(docker_nodes_json),
                                         MockResponse(docker_nodes_null_machines)]
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_nodes(), ['10.10.34.221',
                                                        'http://10.23.26.76:4243'])
        self.assertListEqual(pool_handler.get_nodes(), ['http://127.0.0.1:2375'])

    def test_return_machines_templates(self):
        machines_templates_json = u'''
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
        self.urlopen_mock.side_effect = [MockResponse(machines_templates_json, 200),
                                         MockResponse(None, 500)]
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_machines_templates(),
                             ['template_red', 'template_yellow'])
        self.assertEqual(pool_handler.get_machines_templates(), None)

    def test_remove_node_from_tsuru(self):
        http_error = urllib2.HTTPError(None, 500, None, None, StringIO(u"No such node in storage"))
        self.urlopen_mock.side_effect = [MockResponse(None, 200), http_error]
        pool_handler = plugin.TsuruPool("foobar")
        return_remove_node = pool_handler.remove_node_from_tsuru('http://127.0.0.1:4243')
        self.assertEqual(return_remove_node, True)
        self.assertRaisesRegexp(Exception, 'No such node in storage',
                                pool_handler.remove_node_from_tsuru,
                                'http://127.0.0.1:4243')

    @patch("sys.stderr")
    @patch("sys.stdout")
    def test_move_node_containers(self, stdout, stderr):
        fake_buffer_error = u'''garbage in first chunk\ {"Message":"Moving 2 units..."}\n
                                {"Message":"Error moving unit: abcd1234"}\n
                                {"Message":"Error moving unit: xyzabcd234"}\n
                            '''

        fake_buffer_successfully = u'''
                                   { "Message":"Container moved successfully" }
                                   '''

        fake_empty_buffer = u''

        self.urlopen_mock.side_effect = [MockResponse(fake_buffer_error, 200),
                                         MockResponse(fake_buffer_successfully, 200),
                                         MockResponse(fake_empty_buffer, 200)]

        pool_handler = plugin.TsuruPool("foobar")
        move_return_value = pool_handler.move_node_containers('http://10.10.1.2:123',
                                                              '1.2.3.4:2222')
        stdout.write.assert_called_with("Moving 2 units...\n")
        stderr.write.assert_has_call("Error moving unit: abcd1234\n")
        stderr.write.assert_has_call("Error moving unit: xyzabcd234\n")
        self.assertEqual(move_return_value, False)

        move_return_value_2 = pool_handler.move_node_containers('http://10.1.1.2:123',
                                                                '1.2.3.7:432')
        stdout.write.assert_called_with("{}\n".format(json.loads(fake_buffer_successfully)['Message']))
        self.assertEqual(move_return_value_2, True)

        move_return_value_3 = pool_handler.move_node_containers('http://10.10.1.2:123',
                                                                'http://1.2.3.4:432')
        self.assertEqual(move_return_value_3, True)

    def tearDown(self):
        self.patcher.stop()
