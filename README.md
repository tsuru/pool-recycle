# Pool Recycle plugin for tsuru
[![Build Status](https://travis-ci.org/tsuru/pool-recycle.svg)](https://travis-ci.org/tsuru/pool-recycle)

This plugin allows tsuru to re-create one entire pool based on IaaS templates associate with it.

## Dependencies
You will need just admin permission in tsuru.

## Installing

As easy as any other tsuru plugin (use the same command to upgrade)
```bash
$ tsuru plugin-install pool-recycle https://raw.githubusercontent.com/tsuru/pool-recycle/master/pool_recycle/plugin.py
$ tsuru pool-recycle -h 
usage: pool-recycle [-h] -p POOL [-r DESTROY_NODE] [-d] [-P DOCKER_PORT]
                    [-s DOCKER_SCHEME]

Tsuru pool nodes recycle

optional arguments:
  -h, --help            show this help message and exit
  -p POOL, --pool POOL  Docker tsuru pool
  -r, --destroy-node    Destroy olds docker nodes after recycle
  -d, --dry-run         Dry run all recycle actions
  -m MAX_RETRY, --max_retry MAX_RETRY
                        Max retries attempts to move a node on failure
  -t TIMEOUT, --timeout TIMEOUT
                        Max timeout between moves on failures attempts
  -P DOCKER_PORT, --docker-port DOCKER_PORT
                        Docker port - if something goes wrong, node will be
                        re-add using it as docker port (only when using IaaS)
  -s DOCKER_SCHEME, --docker-scheme DOCKER_SCHEME
                        Docker scheme - if something goes wrong, node will be
                        re-add using it as docker scheme (only when using
                        IaaS)
  --pre_provision       Pre-provision all nodes on IaaS before start moving
```

## Example (running with dry mode)

```bash
$ tsuru pool-recycle -p theonepool -d
Creating new node on pool "theonepool" using "templateA" template
Removing node "http://127.0.0.1:2375" from pool "theonepool"
Moving all containers on old node "http://127.0.0.1:2375" to new node

Creating new node on pool "theonepool" using "templateB" template
Removing node "http://192.168.50.6:2375" from pool "theonepool"
Moving all containers on old node "http://192.168.50.6:2375" to new node
```

