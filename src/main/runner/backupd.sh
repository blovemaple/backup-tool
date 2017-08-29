#!/bin/bash

cd "$(dirname $(readlink -f $0))"
java -cp "$(find lib|tr "\n" ":")" com.github.blovemaple.backupd.Runner -c conf.txt
