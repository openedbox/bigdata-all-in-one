#!/bin/sh
tar zcvf packages.tar.gz thrift-0.8.0.tar.gz web.py-0.37.tar.gz psutil-2.1.1.tar.gz pexpect-3.3.tar.gz thm/tools thm/packages
cat install_tdh_tools.sh packages.tar.gz >install_tools.bin
rm -rf packages.tar.gz
