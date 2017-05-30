#!/usr/bin/python

import swiftclient
from swiftclient.exceptions import ClientException
import os,sys
import argparse
import datetime


parser = argparse.ArgumentParser()
parser.add_argument("-c","--container",help="container name", default="test")
parser.add_argument("-s","--segsize",help="segment size",default=1048576000)
parser.add_argument("-f", "--folder-to-upload", help="folder to upload", required=True)

args = parser.parse_args()

auth_url = 'https://83.241.182.227:5000/v3'
project_id = '85e545bc92cf4fd69012af9c2e4bcf19'
user_id = '4697383556134df4bff8806f27ec2ee1'
password = '3436357e2465184b6625753d152aa234'

container_name = args.container

try:
# Get a Swift client connection object
    conn = swiftclient.Connection( key=password, authurl=auth_url, auth_version='3', insecure=True, os_options={"project_id": project_id,"user_id": user_id })
    conn.put_container(args.container)

except ClientException:
    cmd = "zabbix_sender -z 83.241.182.247 -s %s -k  backupstatus  -o false" % container_name
    os.system(cmd)
    sys.exit(0)


for filename in os.listdir(args.folder_to_upload):

    filepath= args.folder_to_upload+"/"+filename
    obj_size = os.path.getsize(filepath)
    print obj_size
    seg_size = args.segsize
    segs = (obj_size / seg_size) + 1

    # create the container, and the segment container
    container_name = args.container
    seg_container_name = container_name + '_segments'
    print "container {0}".format(container_name)
    conn.put_container(container_name)
    print "container {0}".format(seg_container_name)
    conn.put_container(seg_container_name)

    # start upload of each segment
    print "begin segment upload of " + filename
    print "size " + str(obj_size) + ", " + str(segs) + " segs"

    fp = open(filepath, 'r')
    for n in range(1, segs + 1):
        seg_name = '%s/%08d' % (filename, n)
        if (obj_size - (n - 1) * seg_size < seg_size):
            size = obj_size - (n - 1) * seg_size
        else:
            size = seg_size
        fp.seek((n - 1) * seg_size)
        print "  upload segment " + str(n) + " size " + str(size)
        if datetime.date.today().strftime("%d")== "1" or datetime.date.today().strftime("%d")== "15":
            conn.put_object(seg_container_name,seg_name,fp,content_length=size,headers={'X-Delete-After':7889231})
        else:
            conn.put_object(seg_container_name, seg_name, fp, content_length=size, headers={'X-Delete-After': 1209600})
        print "end segment upload"

    print "create manifest"
    obj_manifest_header = {}
    obj_manifest_header['x-object-manifest'] = '%s/%s/' % (seg_container_name, filename)
    if datetime.date.today().strftime("%d") == "1" or datetime.date.today().strftime("%d") == "15":
        obj_manifest_header['X-Delete-After'] = 7889231
    else:
        obj_manifest_header['X-Delete-After']=1209600
    conn.put_object(container_name, filename, None, headers=obj_manifest_header,)
    print "done"

    fp.close()
    try:
        resp_headers = conn.head_object(container_name, filename)
        print ('The object was successfully created \n')
        cmd = "rm -rf " + "%s" %(filepath)
        os.system(cmd)
        cmd = "zabbix_sender -z 83.241.182.247 -s %s -k  backupstatus  -o true" % container_name
        os.system(cmd)
    except swiftclient.exceptions as e:
        if e.http_status == 404:
            print ('The Object was not found\n')
            cmd = "zabbix_sender -z 83.241.182.247 -s %s -k  backupstatus  -o false" % container_name
            os.system(cmd)
        else:
            print('An error occurred checking for the existence of the object\n')
