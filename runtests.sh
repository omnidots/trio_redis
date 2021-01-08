#!/bin/sh

pip install -e .
pip install -U -r tests/requirements.txt
flake8
pytest tests --docker-redis
