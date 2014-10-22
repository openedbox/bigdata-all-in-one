from helper import SshClient


class ClusterOptions:
    def __init__(self, init_host_name=False):
        self.root_password = "new.1234"
        self.packages_path = "../packages"
        self.jdk_package_path = "%s/jdk-7u65-linux-x64.tar.gz" % self.packages_path
        self.jdk_path = "/usr/local/jdk1.7.0_65"

        '''
        hadoop
        '''
        self.name_nodes = ["10.211.55.231", "10.211.55.232"]
        self.data_nodes = ["10.211.55.231", "10.211.55.232", "10.211.55.233", "10.211.55.234", "10.211.55.235"]
        self.journal_nodes = ["10.211.55.231", "10.211.55.232", "10.211.55.233", "10.211.55.234", "10.211.55.235"]
        self.hadoop_path = "/usr/local/hadoop-2.5.0"
        self.hadoop_package_path = '%s/hadoop-2.5.0-centos63-x86-64.tar.gz' % self.packages_path
        self.hadoop_cluster_name = "mycluster"
        self.hadoop_tmp_dir = '%s/hadoopdir/tmp' % self.hadoop_path
        self.hadoop_journal_node_edits_dir = '%s/hadoopdir/journal' % self.hadoop_path
        self.hadoop_name_dir = '%s/hadoopdir/dfs/nn' % self.hadoop_path
        self.hadoop_data_dir = '%s/hadoopdir/dfs/dn' % self.hadoop_path

        '''
        zookeeper
        '''
        self.zookeeper_nodes = ["10.211.55.231", "10.211.55.232", "10.211.55.233", "10.211.55.234", "10.211.55.235"]
        self.zookeeper_path = "/usr/local/zookeeper-3.4.6"
        self.zookeeper_package_path = '%s/zookeeper-3.4.6.tar.gz' % self.packages_path
        self.zookeeper_data_dir = '%s/zkdata' % self.zookeeper_path
        self.zookeeper_log_dir = '%s/zkdatalog' % self.zookeeper_path

        '''
        storm
        '''
        self.nimbus_nodes = ["10.211.55.231"]
        self.supervisor_nodes = ["10.211.55.232", "10.211.55.233", "10.211.55.234", "10.211.55.235"]
        self.storm_path = "/usr/local/storm-yarn-1.0.0-alpha"
        self.storm_version = 'storm-0.9.2'
        self.storm_package_path = '%s/storm-yarn-1.0.0-alpha.tar.gz' % self.packages_path
        self.storm_local_dir = '%s/stormdata' % self.storm_path
        self.storm_ui_port = 6060
        self.storm_slot_ports = [6700, 6701, 6702, 6703]

        '''
        hbase
        '''
        self.hbase_path = '/usr/local/hbase-0.98.6.1-hadoop2'
        self.hbase_package_path = '%s/hbase-0.98.6.1-hadoop2-bin.tar.gz' % self.packages_path
        self.hbase_nodes = ["10.211.55.231", "10.211.55.232", "10.211.55.233", "10.211.55.234", "10.211.55.235"]
        self.hbase_tmp_dir = '%s/tmp' % self.hbase_path

        '''
        hive
        '''
        self.hive_path = "/usr/local/apache-hive-0.13.1-bin"
        self.hive_package_path = '%s/apache-hive-0.13.1-bin.tar.gz' % self.packages_path
        self.hive_meta_db_url = 'jdbc:mysql://10.211.55.220:3306/hivemeta'
        self.hive_meta_db_driver = 'com.mysql.jdbc.Driver'
        self.hive_meta_db_user = 'hive'
        self.hive_meta_db_password = 'new.1234'
        self.protobuf_jar = '%s/share/hadoop/tools/lib/protobuf-java-2.5.0.jar' % self.hadoop_path
        self.hbase_common_jar = '%s/lib/hbase-common-0.98.6.1-hadoop2.jar' % self.hbase_path
        self.hbase_client_jar = '%s/lib/hbase-client-0.98.6.1-hadoop2.jar' % self.hbase_path
        self.hive_db_driver_jar = '%s/mysql-connector-java-5.1.33-bin.jar' % self.packages_path

        self.host_names = {}

        self.all_hosts = (set(self.name_nodes)
                          | set(self.data_nodes)
                          | set(self.zookeeper_nodes)
                          | set(self.journal_nodes)
                          | set(self.nimbus_nodes)
                          | set(self.supervisor_nodes))

        self.all_hadoop_hosts = (set(self.name_nodes)
                                 | set(self.data_nodes)
                                 | set(self.journal_nodes))

        self.all_storm_hosts = (set(self.nimbus_nodes)
                                | set(self.supervisor_nodes))

        self.all_zookeeper_hosts = self.zookeeper_nodes

        self.hive_host = self.name_nodes[0]

        self.all_hbase_hosts = self.hbase_nodes

        if init_host_name:
            print 'init hostname for every host'
            for host in self.all_hosts:
                ssh = SshClient(host, self.root_password)
                self.host_names[host] = ssh.execute('uname -n')
                ssh.close()