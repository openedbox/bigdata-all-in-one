import os
from helper import SshClient, ScpClient, HadoopConfigGen


class HBase:
    def __init__(self):
        pass

    @staticmethod
    def install(options):

        hbase = HadoopConfigGen()
        hbase.add_property('hbase.rootdir', 'hdfs://%s/hbase' % options.hadoop_cluster_name)
        hbase.add_property('hbase.cluster.distributed', 'true')

        zookeeper_names = []
        for zookeeper_node in options.all_zookeeper_hosts:
            zookeeper_names.append(options.host_names[zookeeper_node])

        hbase.add_property('hbase.zookeeper.quorum', ','.join(zookeeper_names))
        hbase.add_property('hbase.zookeeper.property.clientPort', '2181')
        hbase.add_property('hbase.zookeeper.property.dataDir', options.zookeeper_data_dir)
        hbase.add_property('hbase.tmp.dir', options.hbase_tmp_dir)

        hbase.save('./tmp/hbase-site.xml')

        regionserver_path = './tmp/regionservers'
        if os.path.exists(regionserver_path):
            os.remove(regionserver_path)

        regionservers = open(regionserver_path, 'w')
        for hb in options.hbase_nodes:
            regionservers.write('%s\n' % options.host_names[hb])
        regionservers.close()

        for host in options.all_hbase_hosts:
            ssh = SshClient(host, options.root_password)
            if 'cannot open' in ssh.execute("file " + options.hbase_path):
                ScpClient.local2remote(host, options.root_password, options.hbase_package_path,
                                       '/usr/local/hbase')
                ssh.execute('tar zxf /usr/local/hbase -C /usr/local')
                ssh.execute('rm -rf /usr/local/hbase')
                ssh.execute(
                    'echo export HBASE_HOME=%s >>/etc/profile' % options.hbase_path)
                ssh.execute('echo export PATH=\$HBASE_HOME/bin:\$PATH >>/etc/profile')
                ssh.execute('source /etc/profile')

                ssh.execute('rm -rf %s/lib/slf4j-log4j12*.jar' % options.hbase_path)

                ssh.execute(
                    'sed -i \'s:# export JAVA_HOME=/usr/java/jdk1.6.0/:export JAVA_HOME=%s:\' %s/conf/hbase-env.sh' % (
                        options.jdk_path, options.hbase_path))

                ssh.execute(
                    'sed -i \'s:# export HBASE_MANAGES_ZK=true:export HBASE_MANAGES_ZK=false:\' %s/conf/hbase-env.sh' %
                    options.hbase_path)

                ScpClient.local2remote(host, options.root_password, './tmp/hbase-site.xml',
                                       '%s/conf/' % options.hbase_path)
                ScpClient.local2remote(host, options.root_password, './tmp/regionservers',
                                       '%s/conf/' % options.hbase_path)

            ssh.close()

    @staticmethod
    def start(options):
        master = SshClient(options.all_hbase_hosts[0], options.root_password)
        print 'start hbase cluster'
        master.execute('$HBASE_HOME/bin/hbase-daemon.sh start master')
        master.close()

        for regionserver in options.all_hbase_hosts:
            region = SshClient(regionserver, options.root_password)
            region.execute('$HBASE_HOME/bin/hbase-daemon.sh start regionserver')
            region.close()


    @staticmethod
    def stop(options):
        for regionserver in options.all_hbase_hosts:
            region = SshClient(regionserver, options.root_password)
            region.execute('$HBASE_HOME/bin/hbase-daemon.sh stop regionserver')
            region.close()

        master = SshClient(options.all_hbase_hosts[0], options.root_password)
        print 'stop hbase cluster'
        master.execute('$HBASE_HOME/bin/hbase-daemon.sh stop master')
        master.close()
