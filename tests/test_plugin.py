# Copyright 2015 tsuru authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import unittest

from pool_recycle import plugin


class TsuruPoolTestCase(unittest.TestCase):

    def setUp(self):
        os.environ["TSURU_TARGET"] = "https://cloud.tsuru.io/"
        os.environ["TSURU_TOKEN"] = "abc123"

    def test_missing_env_var(self):
        del os.environ['TSURU_TOKEN']
        self.assertRaisesRegexp(KeyError,
                                "TSURU_TARGET or TSURU_TOKEN envs not set",
                                plugin.TsuruPool, "foobar")
