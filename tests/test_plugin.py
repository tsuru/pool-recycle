# Copyright 2015 tsuru authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import unittest

from mock import patch
from pool_recycle import plugin


class MockResponse(object):

    def __init__(self, resp_data, code=200, msg='OK'):
        self.resp_data = resp_data
        self.code = code
        self.msg = msg
        self.headers = {'content-type': 'text/plain; charset=utf-8'}

    def read(self):
        return self.resp_data

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
                "pool": "whatever"
            },
            "Status": "ready"
        }
    ]
}
        '''
        docker_nodes_null_machines = '''
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
                                                        '10.23.26.76'])
        self.assertListEqual(pool_handler.get_nodes(), ['127.0.0.1'])

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
        self.urlopen_mock.return_value = MockResponse(machines_templates_json)
        pool_handler = plugin.TsuruPool("foobar")
        self.assertListEqual(pool_handler.get_machines_templates(),
                             ['template_red', 'template_yellow'])

    def tearDown(self):
        self.patcher.stop()
