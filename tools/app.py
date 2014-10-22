#!/usr/bin/python2

from optparse import OptionParser
import sys
from hive import Hive
from storm import Storm
from hadoop import Hadoop
from hbase import HBase
from zookeeper import Zookeeper
from infra import Infra
from clusterOptions import ClusterOptions


if "__main__" == __name__:
    usage = "usage: command for deploy data analysis platform (Hadoop 2.5,Storm 0.9.2)"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--command", action="store", type="string", dest="command",
                      help="| requireInstall   : "
                           "| deployAll        : "
                           "| startZookeeper   : "
                           "| initCluster      : "
                           "| startHadoop      : "
                           "| startHBase       : "
                           "| stopHBase        : "
                           "| stopHadoop       : "
                           "| stopZookeeper    : "
                           "| startStorm       : "
                           "| reboot           : ")
    options, args = parser.parse_args()

    if len(sys.argv) == 1:
        print "Type python %s -h or --help for options help." % sys.argv[0]
    else:
        if options.command == "":
            print "Must given -c option\"s value"
        else:
            if options.command == "requireInstall":
                Infra.install(ClusterOptions(True))

            if options.command == "deployAll":
                cluster_options = ClusterOptions(True)
                Zookeeper.install(cluster_options)
                Hadoop.install(cluster_options)
                Storm.install(cluster_options)
                Hive.install(cluster_options)
                HBase.install(cluster_options)

            if options.command == "startZookeeper":
                Zookeeper.start(ClusterOptions())

            if options.command == "stopZookeeper":
                Zookeeper.stop(ClusterOptions())

            if options.command == "startStorm":
                Storm.start(ClusterOptions())

            if options.command == "initCluster":
                Hadoop.init(ClusterOptions())

            if options.command == "startHadoop":
                Hadoop.start(ClusterOptions())

            if options.command == "stopHadoop":
                Hadoop.stop(ClusterOptions())

            if options.command == "startHBase":
                HBase.start(ClusterOptions())

            if options.command == "stopHBase":
                HBase.stop(ClusterOptions())

            if options.command == "reboot":
                Infra.reboot(ClusterOptions())

    print "Done"