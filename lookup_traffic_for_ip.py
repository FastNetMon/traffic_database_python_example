#!/usr/bin/python3

from clickhouse_driver import Client

client = Client(host='localhost')

lookup_ip = 'aa.xx.yy.zz'

#
# packetDiretion:
#    INCOMING = 0
#    OUTGOING = 1
#    INTERNAL = 2
#    OTHER    = 3
#

res = client.execute('SELECT packetDate,packetDateTime,source,sampleRatio,IPv4NumToString(srcIp),IPv4NumToString(dstIp),IPv6NumToString(srcIpv6),IPv6NumToString(dstIpv6),srcAsn,dstAsn,inputInterface,outputInterface,ipProtocolVersion,ttl,sourcePort,destinationPort,protocol,length,numberOfPackets, flags, ipFragmented, ipDontFragment, packetPayloadLength, packetPayloadFullLength, packetDirection, agentIpAddress FROM fastnetmon.traffic where packetDate = today() and (srcIp = IPv4StringToNum(%(src_ip)s) OR dstIp = IPv4StringToNum(%(dst_ip)s) ) LIMIT 1', {'src_ip': lookup_ip, 'dst_ip': lookup_ip})

for element in res:
    print(element)
