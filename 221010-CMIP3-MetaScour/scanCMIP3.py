#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 13:44:34 2022

PJD 30 Sep 2022     - Written to reorganize INM PMIP CMIP6 data removing duplication version dirs
PJD  5 Oct 2022     - updated to gather CMIP3 path information
PJD  6 Oct 2022     - updated to identify all bad files/dirs (replicate these into their own CMIP3/bad subdir tree)
PJD  6 Oct 2022     - Added badRoots and excludeDirs - to limit bombs
PJD  7 Oct 2022     - Added "PST" test to NCAR files (alongside "MDT")
PJD  8 Oct 2022     - Added ds.drop_vars call; add cmorVersion grab
PJD  9 Oct 2022     - Augmented NCAR timezones; cmorVersion to str type
PJD 10 Oct 2022     - Deal with edge case where CMOR was used to rewrite and CDO rewrote the rewritten /p/css03/esgf_publish/cmip3/ipcc/cfmip/2xco2/atm/mo/rsut/mpi_echam5/run1/rsut_CF1.nc
                    TODO: add time start/stop to files that exclude them
                    TODO: table mappings O1 = Omon?, O1e?

@author: durack1
"""

import datetime
import json
import os
import re
# import shutil
import xarray as xr
from xcdat import open_dataset
import pdb
# import sys
# import time

# %% function defs

"""
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
"""


def checkDate(dateStr):
    # assume 2022-10-05 format
    y, m, d = dateStr.split("-")
    if not 2003 <= int(y) <= 2008:
        print("year invalid:", y)
        return None
    if not 1 <= int(m) <= 12:
        print("month invalid:", m)
        return None
    if not 1 <= int(d) <= 31:
        print("day invalid:", d)
        return None

    return dateStr


def getTimes(time):
    y = int(time.dt.year.data)
    m = int(time.dt.month.data)
    d = int(time.dt.day.data)
    dateStr = makeDate(y, m, d, check=False)

    return dateStr


def makeDate(year, month, day, check):
    date = "-".join([str(year), str(month), str(day)])
    # print("makeDate: date =", date)
    # pdb.set_trace()
    if check:
        date = checkDate(date)

    return date


def makeDRS(filePath, date):
    # source = cmip3/ipcc/data10/picntrl/ocn/mo/thetao/iap_fgoals1_0_g/run2/{files}.nc
    # target = CMIP3/DAMIP/NCAR/CCSM4/historicalMisc/r2i1p11/Omon/vo/gu/v20121128
    # hard-coded
    mipEra = "CMIP3"
    gridLabel = "gu"
    # filePath bits
    sourceBits = filePath.split("/")
    experiments = {"pdcntrl", "picntrl",
                   "20c3m", "sresa1b", "sresa2", "sresb1"}
    # https://github.com/PCMDI/CMIP3_CVs/blob/main/src/writeJson.py#L63-L76
    experimentId = sourceBits[3]
    sourceId = sourceBits[7]
    # match up
    activityId = ["CMIP", "ScenarioMIP"]
    institutionId = []
    r = sourceBits[8].replace("run", "")
    ripf = "".join(["r", r, "i0p0f0"])  # switch X with runX
    tableId = ["mo", "da", "fixed"]
    varId = sourceBits[6]
    d = date.split("-")
    versionId = "".join(["v", d[0], d[1], d[2]])
    # composite
    destPath = os.path.join(mipEra, activityId, institutionId, sourceId,
                            experimentId, ripf, tableId, varId, gridLabel, versionId)

    return destPath


def fixFunc(fixStr, fixStrInfo):
    def fix(ds):
        print(fixStrInfo)
        exec(fixStr)  # ds.time.encoding["units"] = "days since 2001-1-1"

        return ds
    return fix


# %% deal with paths
os.chdir("/p/user_pub/climate_work/durack1/tmp/")
destDir = "CMIP3"  # "/a/"
# if os.path.exists(destDir):
#    shutil.rmtree(destDir)
# os.makedirs(destDir)

# %% create lookup lists
attList = ["history", "date", "cmor_version", "forcing"]
# "creation_date","license","tracking_id",
monList = ["Jan", "Feb", "Mar", "Apr", "May",
           "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# %% create exclude dirs
bad = {
    "/p/css03/esgf_publish/cmip3/ipcc/data3/sresa2/ice/mo/sic/ingv_echam4/run1": ["", "fix bad time:units 20O1-1-1", "ds.time.attrs['units'] = 'days since 2001-01-01'", []],
    "/p/css03/esgf_publish/cmip3/ipcc/data3/sresa2/ice/mo/sit/ingv_echam4/run1": ["", "fix bad time:units 20O1-1-1", "ds.time.attrs['units'] = 'days since 2001-01-01'", []],
    "/p/css03/esgf_publish/cmip3/ipcc/data8/picntrl/ocn/mo/rhopoto/ncar_ccsm3_0/run2": ["rhopoto_O1.PIcntrl_2.CCSM.ocnm.0585-01_cat_0589-12.nc", "drop bad time_bnds", None, ["time_bnds"]],
    "/p/css03/esgf_publish/cmip3/ipcc/data16/sresa1b/atm/mo/rlds/mpi_echam5/run2": ["rlds_A1.nc", "drop bad time_bnds", None, ["time_bnds"]],
    # /p/css03/esgf_publish/cmip3/ipcc/cfmip/2xco2/atm/mo/rsut/mpi_echam5/run1/rsut_CF1.nc
}
excludeDirs = set(["summer", ])

# %% iterate over files
cm3 = {}
cm3["!badFileList"] = {}
badFileCount, cmorCount, count = [0 for _ in range(3)]
for cmPath in ["/p/css03/esgf_publish/cmip3", "/p/css03/scratch/ipcc2_deleteme_July2020"]:
    # for cmPath in ["/p/css03/esgf_publish/cmip3/ipcc/cfmip/2xco2/atm/mo/rsut/mpi_echam5/run1"]:  # bug hunting
    for root, dirs, files in os.walk(cmPath):
        # Add dirs to exclude;
        [dirs.remove(d) for d in list(dirs) if d in excludeDirs]
        # 004306 filePath: /p/css03/esgf_publish/cmip3/ipcc/summer/T4031qtC.pop.h.0019-08-21-43200.nc
        if root in bad:
            # Weed out bad paths/files
            badFile = bad[root][0]
            fixStrInfo = bad[root][1]
            fixStr = bad[root][2]
            badVars = bad[root][3]
        else:
            fixStr, fixStrInfo, badVars = [None for _ in range(3)]
            # continue
        if files:
            # print("files:", files)
            files.sort()  # sort to process sequentially
            for c1, fileName in enumerate(files):
                filePath = os.path.join(root, fileName)
                print("{:06d}".format(count), "filePath:", filePath)
                if filePath[-3:] != ".nc":  # deal with *.nc.bad files
                    badFileCount = badFileCount+1
                    print("no date; filePath:", filePath)
                    cm3["!badFileCount"] = badFileCount
                    cm3["!badFileList"][badFileCount] = filePath
                elif filePath[-3:] == ".nc":  # process all "good" files
                    if c1 == 0:
                        cm3[root] = {}  # create dir entry for each file
                    elif root not in cm3.keys():
                        # create dir entry for each file, if first file bad
                        cm3[root] = {}
                    cmorVersion, dateFound = [
                        False for _ in range(2)]  # set for each file
                    count = count+1  # file counter
                    # deal with file issues
                    if (fixStr == None and badVars == None) or (badVars and (fileName != badFile)):
                        # print("if")
                        fh = open_dataset(filePath, use_cftime=True)
                    elif badVars and (fileName == badFile):  # badVars only
                        # print("elif")
                        print("badVars:", badVars)
                        fh = (
                            xr.open_dataset(
                                filePath, drop_variables=[badVars[0]])
                            .pipe(xr.decode_cf)
                        )
                    elif badFile == "":  # fixFunc for all files only - 9863
                        # print("else")
                        fh = (
                            xr.open_dataset(filePath, decode_times=False)
                            .pipe(fixFunc(fixStr, fixStrInfo))
                            .pipe(xr.decode_cf)
                        )
                    # is there a need for fixFunc AND badVars?
                    # pdb.set_trace()
                    if "T" in fh.cf.axes:
                        startTime = getTimes(fh.time[0])
                        endTime = getTimes(fh.time[-1])
                    else:
                        startTime, endTime = [None for _ in range(2)]
                    attDict = fh.attrs
                    for att in attList:
                        if not att in attDict.keys():
                            # print(att, "not in file, skipping..")
                            continue
                        if isinstance(attDict[att], str):
                            print("att:", att)
                            attStr = attDict[att]
                            # print("attStr:", attStr)
                            # BCCR_BCM2_0 format
                            if att == "date":
                                date = attStr
                                date = date.split("-")
                                day = date[0]
                                mon = "{:02d}".format(monList.index(date[1])+1)
                                yr = date[-1]
                                date = makeDate(yr, mon, day, check=True)
                                dateFound = True
                            # Deal with CMOR matches
                            if "CMOR rewrote data to comply" in attStr:
                                attStrInd = attStr.index(" At ")
                                attStr = attStr[attStrInd:]
                                date = re.findall(
                                    r"\d{1,2}/\d{1,2}/\d{2,4}", attStr)
                                date = date[0].split("/")
                                # assuming mm/dd/yyyy e.g. At 20:53:22 on 06/28/2005, CMOR rewrote data to comply with CF standards and IPCC Fourth Assessment requirements
                                date = makeDate(
                                    date[-1], date[0], date[1], check=True)
                                cmorCount = cmorCount+1
                                if "cmor_version" in fh.attrs.keys():
                                    cmorVersion = fh.attrs["cmor_version"]
                                dateFound = True
                            # Deal with regex matches
                            dateReg = [r"[0-3][0-9]/[0-3][0-9]/(?:[0-9][0-9])?[0-9][0-9]",
                                       r"year:[0-9]{4}:month:[0-9]{2}:day:[0-9]{2}",
                                       # r"Fri Aug  5 19:23:54 MDT 2005"
                                       r"[a-zA-Z]{3}\s[a-zA-Z]{3}\s{1,2}\d{1,2}\s\d{1,2}.\d{2}.\d{2}\s[A-Z]{3}\s\d{4}",
                                       ]
                            for dateFormat in dateReg:
                                if dateFound:
                                    continue
                                date = re.findall(dateFormat, attStr)
                                # timezones
                                timeZones = ["EDT", "MDT", "MST", "PDT", "PST"]
                                # CSIRO format
                                if date and ("year" in date[0]):
                                    date = date[0].replace("year:", "").replace(
                                        ":month:", "-").replace(":day:", "-")
                                    dateFound = True
                                # NCAR CCSM format
                                # elif date and (("MST" in date[0]) or ("PST" in date[0])) or ("PDT" in date[0]) or ("MDT" in date[0]) or ("EDT" in date[0]):
                                elif date and any(zone in date[0] for zone in timeZones):
                                    date = date[0].split(" ")
                                    mon = "{:02d}".format(
                                        monList.index(date[1])+1)
                                    yr = date[-1]
                                    if len(date) == 6:
                                        day = date[2]
                                    elif len(date) == 7:
                                        day = date[3]
                                    day = "{:02d}".format(int(day))
                                    date = makeDate(yr, mon, day, check=True)
                                    dateFound = True
                    # if a valid date start saving pieces
                    if date:
                        # save filePath, fileName, attName, date
                        cm3[root][fileName] = {}
                        cm3[root][fileName][att] = date
                        cm3[root][fileName]["time0"] = startTime
                        cm3[root][fileName]["timeN"] = endTime
                        if cmorVersion:
                            cm3[root][fileName]["cmorVersion"] = str(
                                cmorVersion)
                        cm3["!cmorCount"] = cmorCount
                        cm3["!fileCount"] = count  # https://ascii.cl/
                    if not date:
                        badFileCount = badFileCount+1
                        print("no date; filePath:", filePath)
                        cm3["!badFileCount"] = badFileCount
                        cm3["!badFileList"][badFileCount] = filePath
                    print("date:", date)

                    # close open file
                    fh.close()

        # save dictionary ## if files
        timeNow = datetime.datetime.now()
        timeFormatDir = timeNow.strftime("%y%m%d")
        outFile = "_".join([timeFormatDir, "cmip3.json"])
        if os.path.exists(outFile):
            os.remove(outFile)
        print("writing:", outFile)
        fH = open(outFile, "w")
        json.dump(
            cm3,
            fH,
            ensure_ascii=True,
            sort_keys=True,
            indent=4,
            separators=(",", ":"),
        )
        fH.close()


'''
att: history
date: 2004-12-02
002005 filePath: /p/css03/esgf_publish/cmip3/ipcc/data10/picntrl/ocn/mo/so/giss_model_e_h/run1/.so_O1.GISS3.PIcntrl.2120to2139.nc.1jbIG7
no date; filePath: /p/css03/esgf_publish/cmip3/ipcc/data10/picntrl/ocn/mo/so/giss_model_e_h/run1/.so_O1.GISS3.PIcntrl.2120to2139.nc.1jbIG7
002005 filePath: /p/css03/esgf_publish/cmip3/ipcc/data10/picntrl/ocn/mo/so/giss_model_e_h/run1/so_O1.GISS3.PIcntrl.2180to2199.nc
'''
