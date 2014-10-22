import os
from helper import SshClient, ScpClient, HadoopConfigGen


class Hadoop:
    def __init__(self):
        pass

    @staticmethod
    def install(options):
        if not os.path.exists('./tmp'):
            os.mkdir('./tmp')

        core = HadoopConfigGen()
        core.add_property('fs.defaultFS', 'hdfs://%s' % options.hadoop_cluster_name)
        core.add_property('hadoop.tmp.dir', options.hadoop_tmp_dir)

        zookeeper_names = []
        for zookeeper_host in options.all_zookeeper_hosts:
            zookeeper_names.append('%s:2181' % options.host_names[zookeeper_host])
        core.add_property('ha.zookeeper.quorum', ','.join(zookeeper_names))
        core.save('./tmp/core-site.xml')

        hdfs = HadoopConfigGen()
        hdfs.add_property('dfs.replication', '3')
        hdfs.add_property('dfs.permissions', 'false')
        hdfs.add_property('dfs.permissions.enabled', 'false')
        hdfs.add_property('dfs.namenode.name.dir', options.hadoop_name_dir)
        hdfs.add_property('dfs.datanode.data.dir', options.hadoop_data_dir)
        hdfs.add_property('dfs.nameservices', options.hadoop_cluster_name)
        hdfs.add_property('dfs.ha.namenodes.%s' % options.hadoop_cluster_name, 'nn1,nn2')
        hdfs.add_property('dfs.namenode.rpc-address.%s.nn1' % options.hadoop_cluster_name,
                          '%s:8020' % options.host_names[options.name_nodes[0]])
        hdfs.add_property('dfs.namenode.http-address.%s.nn1' % options.hadoop_cluster_name,
                          '%s:50070' % options.host_names[options.name_nodes[0]])
        hdfs.add_property('dfs.namenode.servicerpc-address.%s.nn1' % options.hadoop_cluster_name,
                          '%s:53310' % options.host_names[options.name_nodes[0]])
        hdfs.add_property('dfs.namenode.rpc-address.%s.nn2' % options.hadoop_cluster_name,
                          '%s:8020' % options.host_names[options.name_nodes[1]])
        hdfs.add_property('dfs.namenode.http-address.%s.nn2' % options.hadoop_cluster_name,
                          '%s:50070' % options.host_names[options.name_nodes[1]])
        hdfs.add_property('dfs.namenode.servicerpc-address.%s.nn2' % options.hadoop_cluster_name,
                          '%s:53310' % options.host_names[options.name_nodes[1]])

        hdfs.add_property('dfs.ha.automatic-failover.enabled.%s' % options.hadoop_cluster_name, 'true')

        journal_nodes = []
        for jn in options.journal_nodes:
            journal_nodes.append('%s:8485' % options.host_names[jn])

        hdfs.add_property('dfs.namenode.shared.edits.dir',
                          'qjournal://%s/%s' % (';'.join(journal_nodes), options.hadoop_cluster_name))
        hdfs.add_property('dfs.client.failover.proxy.provider.%s' % options.hadoop_cluster_name,
                          'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider')
        hdfs.add_property('dfs.journalnode.edits.dir', options.hadoop_journal_node_edits_dir)
        hdfs.add_property('dfs.ha.fencing.methods', 'sshfence')
        hdfs.add_property('dfs.ha.fencing.ssh.private-key-files', '/root/.ssh/id_rsa')
        hdfs.add_property('dfs.ha.fencing.ssh.connect-timeout', '10000')
        hdfs.add_property('dfs.namenode.handler.count', '100')
        hdfs.save('./tmp/hdfs-site.xml')

        mapred = HadoopConfigGen()
        mapred.add_property('mapreduce.framework.name', 'yarn')
        mapred.save('./tmp/mapred-site.xml')

        yarn = HadoopConfigGen()
        yarn.add_property('yarn.resourcemanager.hostname', options.host_names[options.name_nodes[0]])
        yarn.add_property('yarn.nodemanager.aux-services', 'mapreduce_shuffle')
        yarn.save('./tmp/yarn-site.xml')

        slave = './tmp/slaves'
        if os.path.exists(slave):
            os.remove(slave)
        slaves = open(slave, 'w')
        for dn in options.data_nodes:
            slaves.write('%s\n' % options.host_names[dn])
        slaves.close()

        for host in options.all_hadoop_hosts:
            ssh = SshClient(host, options.root_password)
            if 'cannot open' in ssh.execute("file " + options.hadoop_path):
                ScpClient.local2remote(host, options.root_password, options.hadoop_package_path,
                                       '/usr/local/hadoop')
                ssh.execute('tar zxf /usr/local/hadoop -C /usr/local')
                ssh.execute('rm -rf /usr/local/hadoop')
                ssh.execute('echo export HADOOP_PREFIX=%s >>/etc/profile' % options.hadoop_path)
                ssh.execute('echo export HADOOP_HOME=%s >>/etc/profile' % options.hadoop_path)
                ssh.execute('echo export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH >>/etc/profile')
                ssh.execute('source /etc/profile')
                ssh.execute('mkdir -p %s' % options.hadoop_tmp_dir)
                ssh.execute('sed -i \'s:${JAVA_HOME}:%s:\' %s/etc/hadoop/hadoop-env.sh' % (
                    options.jdk_path, options.hadoop_path))
                ssh.execute(
                    'sed -i \'s:# export JAVA_HOME=/home/y/libexec/jdk1.6.0/:export JAVA_HOME=%s:\' %s/etc/hadoop/yarn-env.sh' % (
                        options.jdk_path, options.hadoop_path))
                ScpClient.local2remote(host, options.root_password, './tmp/core-site.xml',
                                       '%s/etc/hadoop/' % options.hadoop_path)
                ScpClient.local2remote(host, options.root_password, './tmp/hdfs-site.xml',
                                       '%s/etc/hadoop/' % options.hadoop_path)
                ScpClient.local2remote(host, options.root_password, './tmp/mapred-site.xml',
                                       '%s/etc/hadoop/' % options.hadoop_path)
                ScpClient.local2remote(host, options.root_password, './tmp/yarn-site.xml',
                                       '%s/etc/hadoop/' % options.hadoop_path)
                ScpClient.local2remote(host, options.root_password, './tmp/slaves',
                                       '%s/etc/hadoop/' % options.hadoop_path)
            ssh.close()

    @staticmethod
    def init(options):
        master_first_ssh = SshClient(options.name_nodes[0], options.root_password)
        print 'format zookeeper for HA'
        master_first_ssh.execute('$HADOOP_HOME/bin/hdfs zkfc -formatZK')
        master_first_ssh.close()

        for host in options.journal_nodes:
            journal_ssh = SshClient(host, options.root_password)
            journal_ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start journalnode')
            journal_ssh.close()

        master_first_ssh = SshClient(options.name_nodes[0], options.root_password)
        print 'format first namenode'
        master_first_ssh.execute('$HADOOP_HOME/bin/hdfs namenode -format -clusterId c1')
        master_first_ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode')
        master_first_ssh.close()

        master_second_ssh = SshClient(options.name_nodes[1], options.root_password)
        print 'sync second namenode'
        master_second_ssh.execute('$HADOOP_HOME/bin/hdfs namenode -bootstrapStandby')
        master_second_ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode')
        master_second_ssh.close()

    @staticmethod
    def start(options):
        ssh = SshClient(options.name_nodes[0], options.root_password)
        print 'start hadoop cluster'
        ssh.execute('$HADOOP_HOME/sbin/start-all.sh')
        ssh.close()

        print 'start zookeeper failover cotroller'
        for host in options.all_hadoop_hosts:
            ssh = SshClient(host, options.root_password)
            ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start zkfc')
            ssh.close()

    @staticmethod
    def stop(options):
        ssh = SshClient(options.name_nodes[0], options.root_password)
        print 'stop hadoop cluster'
        ssh.execute('$HADOOP_HOME/sbin/stop-all.sh')
        ssh.close()

        print 'stop zookeeper failover cotroller'
        for host in options.all_hadoop_hosts:
            ssh = SshClient(host, options.root_password)
            ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh stop zkfc')
            ssh.close()
