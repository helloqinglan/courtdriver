#!/usr/bin/env bash
set -ex

# SET THE FOLLOWING VARIABLES

# docker hub username
USERNAME=helloqinglan

# image name
IMAGE=courtdriver

docker build -t $USERNAME/$IMAGE:latest .