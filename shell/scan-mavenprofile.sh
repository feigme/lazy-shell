#!/bin/bash

prodProjectDirs=$(find /alidata/server/jenkins/workspace/ -type d -maxdepth 1 | grep -v "@tmp" | grep "/prod-")
for prodProjectDir in $prodProjectDirs; do

done