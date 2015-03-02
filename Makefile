# Copyright 2015 tsuru authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

.PHONY: test deps

test: deps
	@python -m unittest discover --verbose
	@flake8 --max-line-length=110 .

deps:
	pip install -r requirements.txt

coverage: deps
	rm -f .coverage
	coverage run --source=. -m unittest discover
	coverage report -m --omit=test\*,run\*.py
