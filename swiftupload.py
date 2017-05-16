#!/usr/bin/python

import swiftclient
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c","--container",help="container name", default="test")
parser.add_argument("-s","--segsize",help="segment size",default=1048576000)
parser.add_argument("-f", "--folder-to-upload", help="folder to upload", required=True)

args = parser.parse_args()

auth_url = '<keystone auth url>'
project_id = '<project id>'
user_id = '<user id>'
password = '<password>'

# Get a Swift client connection object
conn = swiftclient.Connection(
        key=password,
        authurl=auth_url,
        auth_version='3',
        insecure=True,
        os_options={"project_id": project_id,
                    "user_id": user_id })

for filename in os.listdir(args.folder_to_upload):

    filepath= args.folder_to_upload+"/"+filename
    obj_size = os.path.getsize(filepath)
    print obj_size
    seg_size = args.segsize
    segs = (obj_size / seg_size) + 1

    # create the container, and the segment container
    con_name = args.container
    seg_con_name = con_name + '_segments'
    print "container {0}".format(con_name)
    conn.put_container(con_name)
    print "container {0}".format(seg_con_name)
    conn.put_container(seg_con_name)

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
        conn.put_object(seg_con_name,seg_name,fp,content_length=size,)
        print "end segment upload"

    print "create manifest"
    obj_manifest_header = {}
    obj_manifest_header['x-object-manifest'] = '%s/%s/' % (seg_con_name, filename)
    conn.put_object(con_name,
                        filename,
                        None,
                        headers=obj_manifest_header,
                        )
    print "done"

    fp.close()
    try:
        resp_headers = conn.head_object(con_name, filename)
        print ('The object was successfully created \n')
    except swiftclient.exceptions as e:
        if e.http_status == 404:
            print ('The Object was not found\n')
        else:
            print('An error occurred checking for the existence of the object\n')
