#!/usr/bin/python

from sys import stdin, stdout, stderr

data = stdin.readline()
stdout.write("OK\tMy Backend\n")
stdout.flush()

while True:
    data = stdin.readline().strip()
    stderr.write(data+"\n")
    query = data.split("\t")
    if query[0] == "AXFR":
        stdout.write("END\n")
        stdout.flush()
	continue
    kind, qname, qclass, qtype, id, remote_ip, local_ip = data.split("\t")
    stderr.write("QNAME: " + qname + ", REMOTE_IP: " + remote_ip + "\n")
    if qname == "www.live.com":
        if remote_ip == "10.0.196.2":
            r = "DATA\t" + qname + "\t" + qclass + "\tA\t3600\t" + id + "\t10.0.190.2\n"
	else:
            r = "DATA\t" + qname + "\t" + qclass + "\tA\t3600\t" + id + "\t10.0.184.2\n"
    else:
        r = "DATA\t" + qname + "\t" + qclass + "\tA\t3600\t" + id + "\t1.2.3.4\n"
    
    stdout.write(r)
    stderr.write(r)
    stdout.write("END\n")
    stdout.flush()