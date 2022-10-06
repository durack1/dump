#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 13:44:34 2022

PJD 30 Sep 2022     - Written to reorganize INM PMIP CMIP6 data removing duplication version dirs

@author: durack1
"""

import os
#import pdb
import re
import shutil
import sys

# %% function defs


def copyStuff(root, dirs, destDir, vers):
    # deal with multi-dir or single dir
    if not vers:
        fullPath = os.path.join(root, dirs[0])
        shutil.copytree(fullPath, fullPath.replace(
            "CMIP6", destDir))  # start copying
        return
    else:
        dirWritten = False
        for dirCount, dirPath in enumerate(dirs):
            fullPath = os.path.join(root, dirPath)
            fileList = os.listdir(fullPath)
            for fileCount, fileName in enumerate(fileList):
                srcPath = os.path.join(root, dirPath)
                src = os.path.join(root, dirPath, fileName)
                dstPath = os.path.join(root.replace(
                    "CMIP6", destDir), dirs[0])
                dst = os.path.join(root.replace(
                    "CMIP6", destDir), dirs[0], fileName)
                print("srcPath:", srcPath)
                print("src:", src)
                if not dirWritten:
                    # copy once
                    shutil.copytree(srcPath, dstPath)
                    dirWritten = True
                    continue  # add files to dir from second verDir
                if 'nc.2xYTPm' in fileName:
                    print("nc.2xYTPm")
                    print("fileName:", fileName, "skipped")
                    continue
                shutil.copy2(src, dst)
                print("dstPath:", dstPath)
                print("dst:", dst)
        return
    return


# %%
os.chdir("/p/user_pub/climate_work/durack1/tmp/")
destDir = "CMIP6-2"  # "/a/"
# if os.path.exists(destDir):
#    shutil.rmtree(destDir)
# os.makedirs(destDir)
count = 0
dupeDirs = 0
reTest = re.compile(r"v[0-9]{8}")
inmPath = "CMIP6"
for root, dirs, files in os.walk(inmPath):
    if dirs and (re.match(r"v[0-9]{8}", dirs[0]) and len(dirs) > 1):
        # copyStuff(root, dirs, destDir, vers=True)  # iterate over dupe dirs
        print("dirs:", dirs)
        count = count+1
        dupeDirs = dupeDirs + 1
        print(">1 version found: dupeDirs = ", dupeDirs, "count = ", count)
        print(root, dirs, files)
        continue
    elif dirs and (re.match(r"v[0-9]{8}", dirs[0]) and len(dirs) == 1):
        # copyStuff(root, dirs, destDir, vers=False)  # copy whole tree
        count = count+1
        print("1 version found: dupeDirs = ", dupeDirs, "count = ", count)
        continue
    elif files:
        #print("what is this all about")
        #print(root, dirs, files)
        # pdb.set_trace()
        pass
    if count > 10000:
        sys.exit()
