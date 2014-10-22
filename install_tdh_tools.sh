#!/bin/sh
export PATH=/usr/bin:/bin:/sbin
umask 022
echo_args="-e "
dir_tmp=/tmp/tdh-install-tmp
dir_install=/root/tdh-tools
more <<"EOF"
Hadoop Cluster Installer (hadoop 2.5.0,storm 0.9.2,zookeeper 3.4.6)
EOF
agreed=
while [ x$agreed = x ]; do
    echo
    echo "Do you agree to the above license terms? [yes or no] "
    read reply leftover
    case $reply in
    y* | Y*)
        agreed=1;;
    n* | N*)
    echo "If you don't agree to the license you can't install this software";
    exit 1;;
    esac
done
echo "Preinstall..."
if [ ! -d "$dir_tmp" ] ; then
    mkdir $dir_tmp
fi
rm -rf $dir_tmp/*

yum -y install python-devel ntp gcc gcc-c++ libstdc++-devel make build openssh-clients wget lsof

echo "Unpacking..."
sed -n -e '1,/^exit 0$/!p' $0 > "${dir_tmp}/packages.tar.gz" 2>/dev/nul    

cd $dir_tmp
tar zxf packages.tar.gz

echo "check thrift"
if [ ! -s /usr/local/bin/thrift ]; then
	echo "install thrift"
	tar zxf thrift-0.8.0.tar.gz
	cd thrift-0.8.0
	./configure
	make
	make install
	cd $dir_tmp
fi

echo "check web.py"
if [ ! -s /usr/lib/python2.6/site-packages/web.py-0.37-py2.6.egg-info ]; then
	echo "install web.py"
	tar zxf web.py-0.37.tar.gz
	cd web.py-0.37
	python setup.py install
	cd $dir_tmp
fi

echo "check psutil"
if [ ! -s /usr/lib64/python2.6/site-packages/psutil-2.1.1-py2.6.egg-info ]; then
	echo "install psutil"
	tar zxf psutil-2.1.1.tar.gz
	cd psutil-2.1.1
	python setup.py install
	cd $dir_tmp
fi

echo "check pexpect"
if [ ! -s /usr/lib/python2.6/site-packages/pexpect-3.3-py2.6.egg-info ]; then
	echo "install pexpect"
	tar zxf pexpect-3.3.tar.gz
	cd pexpect-3.3
	python setup.py install
	cd $dir_tmp
fi

if [ ! -d "$dir_install" ] ; then
    mkdir $dir_install
fi
cp -rf ./thm/* $dir_install
rm -rf $dir_tmp
echo 'Done'
exit 0
