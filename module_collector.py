#!/usr/bin/python
import subprocess
import re
import socket
import requests
import os
import datetime
import math
import json
import logging
import licence_collector as lc
import common as c
from copy import deepcopy

# test
module_primitive = {"description": "", "domains": [], "licence_type": "",
                    "homepage": "", "support": "", "machines": {}, "licences": {}}
log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
diff_log = ""

# Calls  returns value between input strings.


def get_between(string_full, string_start, string_end):

    # Split between start and end strings.
    stdout = string_full.split(string_start)[1].split(string_end)[0]

    return stdout


def avail_path(machine, module_path):

    log.info("Reading modules from " + machine)
    # Check if running on mahuika01, and recheck modules
    # source /etc/bashrc doesn't work on maui for some reason

    log.info("Working... Takes about 100 sec... for some reason")

    stdout_full = subprocess.check_output(
        ("MODULEPATH=" + module_path + "; module -t avail"), stderr=subprocess.STDOUT, shell=True).decode("utf-8")

    main_dict = {}
    lastApp = ""
    # return main_dict

    # Get names of all apps
    for line in stdout_full.split('\n'):

        # Check if this is the same app as last time.
        thisApp = line.split('/')[0].strip()

        # Check nonzero
        if len(thisApp) > 0:
            # If new app, add to dictionary.
            if lastApp != thisApp:

                # Define dict
                main_dict[thisApp] = deepcopy(module_primitive)
                main_dict[thisApp]['machines'][machine] = []

                try:
                    data = subprocess.check_output(
                        "MODULEPATH=" + module_path + "; module -t whatis " + thisApp, stderr=subprocess.STDOUT, shell=True).decode("utf-8")

                except:
                    log.error("Module whatis for " +
                              thisApp + " failed, skipping...")
                    # return

                # Cant remember why I did this, and I aint touching it.
                regexHomepage = r"(?<=Homepage: )\S*"
                matchesHomepage = re.findall(regexHomepage, data)

                if len(matchesHomepage) > 0:
                    main_dict[thisApp]['homepage'] = matchesHomepage[0]

                if len(data.split("Description: ")) > 1:
                    short = data.split("Description: ")[1]
                elif len(data.split(": ")) > 1:
                    short = (data.split(": "))[1]
                else:
                    short = data

                main_dict[thisApp]['description'] = short.split(
                    thisApp + "/")[0]

            else:
                # If add to versionlist
                main_dict[thisApp]['machines'][machine].append(line)

            lastApp = thisApp

    log.info("Module avail complete")

    return main_dict


def assign_tags(module_dat, tag_field, tags):

    for tag, apps in tags.items():
        for app in apps:
            if app in module_dat:
                # if tag_field module_dat[app]:
                    # If list, append
                if isinstance(module_dat[app][tag_field], list):
                    if not tag in module_dat[app][tag_field]:
                        module_dat[app][tag_field].append(tag)
                        module_dat[app][tag_field].sort()
                # Else overwrite
                else:
                    module_dat[app][tag_field] = tag

            else:
                log.warning(
                    "Error! tag '" + app + "' does not correspond to a application on the platform.")


def main():
    # Start
    # Logger setup
    log.info("Starting...")
    # Read Settings
    settings = c.readmake_json('settings.json', {"remote": "", "token": "", "update": [
                               "mahuika", "maui"], "diff": ["mahuika", "maui"]})
    log.info(json.dumps(settings))

    #===== Checks =====#
    # Is correct user
    if os.environ['USER'] != "nesi-apps-admin":
        log.error(
            "COMMAND SHOULD BE RUN AS 'nesi-apps-admin' ELSE LICENCE STATS WONT WORK")
            
    # Folders exist?
    if not os.path.exists('tags'):
        log.warning("Creating missing directory 'tags'")
        os.makedirs('tags')
    if not os.path.exists('tags'):
        log.warning("Creating missing directory 'cache'")
        os.makedirs('cache')

    # On mahuika?
    if not (socket.gethostname().startswith('mahuika')):
        log.error("Currently must be run from Mahuika. Because I am lazy.")
        return 1

    #===== Module list =====#
    # Read cached data;
    mahuika_modules_cache = c.readmake_json('cache/mahuika_cache.json', {})
    maui_modules_cache = c.readmake_json('cache/maui_cache.json', {})

    if "mahuika" in settings['update']:
        mahuika_modules = avail_path(
            "mahuika", "/opt/nesi/CS400_centos7_bdw/modules/all:/opt/nesi/share/modules/all")

        # Compares new object with cached object and logs differences.
        diff_mahuika=c.deep_merge(mahuika_modules, mahuika_modules_cache, True)

        with open("diff_log.txt", "a") as diff_file:
            diff_file.write("=====================" + " Mahuika - " + datetime.datetime.now(
        ).strftime("%d/%m/%Y %H:%M:%S") + " ===================== \n" + diff_mahuika)
        log.info("Comparing mahuika modules with cache")
        

        # Update cache
        c.writemake_json('cache/mahuika_cache.json', mahuika_modules_cache)
    
    mahuika_modules = mahuika_modules_cache

    if "maui" in settings['update']:
        maui_modules = avail_path(
            "maui", "/opt/nesi/XC50_sles12_skl/modules/all:/opt/nesi/share/modules/all:/opt/cray/ari/modulefiles")

        # Compares new object with cached object and logs differences.
        diff_maui=c.deep_merge(maui_modules, maui_modules_cache, True)
        with open("diff_log.txt", "a") as diff_file:
            diff_file.write("=====================" + " Maui     - " + datetime.datetime.now(
        ).strftime("%d/%m/%Y %H:%M:%S") + " ===================== \n" + diff_maui)

        log.info("Comparing maui modules with cache")
    
        # Update cache
        c.writemake_json('cache/maui_cache.json', maui_modules_cache)

    maui_modules = maui_modules_cache
    
    # Merge cluster lists
    c.deep_merge(maui_modules, mahuika_modules)
    all_modules=mahuika_modules

    # Licence Stuff
    slurm_dat = lc.get_slurm_tokens()
    # lc.update_overwrites(slurm_dat)

    licence_meta = c.readmake_json('tags/licence_meta.json')
    c.deep_merge(licence_meta, slurm_dat)
    licence_list=slurm_dat

    for key, value in licence_list.items():
        lc.validate_lic_file(key, value)

    for key, value in licence_list.items():
        lc.lmutil(key, value)

    lc.assign_aliases(licence_list)   # Assign licence aliases if any.
    lc.update_history(licence_list)   # Loads previous history data.

    # lc.attach_lic(lic_dat, module_dat) # Attach Licence Data to module data.

    # attach tags
    # pull from repo
    domain_tags = c.pull(
        "https://raw.githubusercontent.com/nesi/modlist/master/domainTags.json")

    if isinstance(domain_tags, dict):
        c.writemake_json('tags/domain_tags.json', domain_tags)
    else:
        log.error("Using cached version of domain tags")
        domain_tags = c.readmake_json('domain_tags.json', {"biology": [], "engineering": [], "physics": [], "analytics": [
        ], "visualisation": [], "geology": [], "mathematics": [], "chemistry": [], "language": []})

    licence_tags = c.readmake_json('tags/licence_tags.json', {"proprietary": []})

    assign_tags(all_modules, "domains", domainTags)
    assign_tags(all_modules, "licence_type", licence_tags)

    # Apply Overwrites
    #module_overwrite_dat = c.readmake_json('master_overwrites.json')
    #c.deep_merge(module_dat, module_overwrite_dat)

    timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    log.info("Updated as of " + timestamp)
    output_dict = {"modules": all_modules, "date": timestamp}

    # Write to cache
    #c.writemake_json('cache/full_cache2.json', output_dict)
    c.writemake_json('module_list.json', output_dict)
    c.writemake_json('licence_list.json', licence_list)

    print("DONE!")

main()
