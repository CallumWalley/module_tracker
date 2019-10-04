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
import common as c
from common import log
from copy import deepcopy


def get_darcs_log():

    darcs_path="/opt/nesi/nesi-apps-admin/Mahuika/easyconfigs/"
    darcs_period="last day"

    try:
        shell_string="darcs changes --repodir " + darcs_path + " --match 'date \"" + darcs_period + "\"'"
        log.debug(shell_string)
        data = subprocess.check_output(shell_string, stderr=subprocess.STDOUT, shell=True).decode("utf-8")
        log.debug(data)
        log.info("Dracs log dun got.")

        return data

    except Exception as details:
        log.error("Failed to read darcs log: " + details)

def avail_path(machine, module_path):

    log.info("Reading modules from " + machine)
    # Check if running on mahuika01, and recheck modules
    # source /etc/bashrc doesn't work on maui for some reason

    log.info("Working... Takes about 100 sec... for some reason")


    shell_string="MODULEPATH='" + module_path + "';/usr/share/lmod/lmod/libexec/lmod -t avail"
    #log.debug(shell_string)
    try:
        stdout_full=subprocess.check_output(shell_string,  stderr=subprocess.STDOUT, shell=True).decode("utf-8").strip().split("MODULEPATH")[0]
        #log.debug(stdout_full)
    except Exception as details:
        log.error(shell_string + " failed: " + str(details))
    #else:         
    main_dict = {}
    lastApp = ""

    # Get names of all apps
    for line in stdout_full.split("\n"):
        log.debug(line)
        # Check if this is the same app as last time.
        thisApp = line.split("/")[0].strip()

        # Check nonzero
        if len(thisApp) > 0:
            # If new app, add to dictionary.
            if lastApp != thisApp:

                # Define dict
                main_dict[thisApp] = deepcopy(settings["default"])
                main_dict[thisApp]["machines"][machine] = []

                try:

                    shell_string="MODULEPATH='" + module_path + "';" + "/usr/share/lmod/lmod/libexec/lmod -t whatis " + thisApp
                    log.debug(shell_string)
                    data = subprocess.check_output(shell_string, stderr=subprocess.STDOUT, shell=True).decode("utf-8").split("MODULEPATH")[0]
                except Exception as details:
                    log.error("Module whatis for " + thisApp + " failed: " + details)
                    continue
                
                else:
                    # Cant remember why I did this, and I aint touching it.
                    regexHomepage = r"(?<=Homepage: )\S*"
                    matchesHomepage = re.findall(regexHomepage, data)

                    if len(matchesHomepage) > 0:
                        main_dict[thisApp]["homepage"] = matchesHomepage[0]

                    if len(data.split("Description: ")) > 1:
                        short = data.split("Description: ")[1]
                    elif len(data.split(": ")) > 1:
                        short = (data.split(": "))[1]
                    else:
                        short = data

                    main_dict[thisApp]["description"] = short.split(thisApp + "/")[0]

            else:
                # If add to versionlist
                main_dict[thisApp]["machines"][machine].append(line)

            lastApp = thisApp

    log.info("Module avail complete")

    return main_dict

def log_changes(name, new, old):
    # Compares new object with cached object and logs differences.
    diff_ = c.deep_merge(new, old, True)
    if len(diff_) > 0:
        with open("diff_log.txt", "a") as diff_file:
            diff_file.write(
                "\n====================="
                + " " + name +" - "
                + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                + " =====================\n"
                + diff_
            )
    log.info("Comparing " + name + " modules with cache")

def get_licences():
    licence_object=c.readmake_json(settings["licences_path"])

    alias = c.pull("https://raw.githubusercontent.com/nesi/modlist/master/aliases.json")
    if isinstance(alias, dict):
        c.writemake_json("cache/alias.json", alias)
    else:
        log.error("Using cached version of alias")
        alias = c.readmake_json("cache/alias.json")

    for module_name, module_values in all_modules.items():
        for licence_name, licence_values in licence_object.items():
            if module_name.lower() == licence_values["software_name"].lower() and licence_values["enabled"] and licence_values["visible"]:
                log.debug("Attaching licences to " + module_name)

                module_values["licences"][licence_name]={}
                
                # Only copy some values over
                module_values["licences"][licence_name]={key: licence_values[key] for key in ('license_type', 'use_conditions', 'hourly_averages', 'real_total')}

                # Add nice names if possible.
                if licence_values["institution"] in alias:
                    module_values["licences"][licence_name]["institution"]=alias[licence_values["institution"]]
                    log.debug(licence_name + ": using alias '" + alias[licence_values["institution"]] + "' in place of institution '" + licence_values["institution"] + "'.")
                else:
                    module_values["licences"][licence_name]["institution"]=licence_values["institution"]

                if licence_values["faculty"] in alias:
                    module_values["licences"][licence_name]["faculty"]=alias[licence_values["faculty"]]
                    log.debug(licence_name + ": using alias '" + alias[licence_values["faculty"]] + "' in place of faculty '" + licence_values["faculty"] + "'.")
                else:
                    module_values["licences"][licence_name]["faculty"]=licence_values["faculty"]

settings = c.readmake_json("settings.json")

log.info("Starting...")
log.debug(json.dumps(settings))

# ===== Module list =====#
# Read cached data;
mahuika_modules_cache = c.readmake_json("cache/mahuika_cache.json", {})
maui_modules_cache = c.readmake_json("cache/maui_cache.json", {})


if "mahuika" in settings["update"]:
    mahuika_modules = avail_path("mahuika", "/opt/nesi/CS400_centos7_bdw/modules/all:/opt/nesi/share/modules/all")
    # Since deepmerge sets under=over 'cache' is now most up to date object 
    log_changes("mahuika", mahuika_modules, mahuika_modules_cache)
    # Update cache
    c.writemake_json("cache/mahuika_cache.json", mahuika_modules_cache)

mahuika_modules = mahuika_modules_cache

if "maui" in settings["update"]:
    maui_modules = avail_path("maui", "/opt/nesi/XC50_sles12_skl/modules/all:/opt/nesi/share/modules/all:/opt/cray/ari/modulefiles")

    log_changes("maui", maui_modules, maui_modules_cache)
    # Update cache
    c.writemake_json("cache/maui_cache.json", maui_modules_cache)

maui_modules = maui_modules_cache

# Merge cluster lists
c.deep_merge(maui_modules, mahuika_modules)
all_modules = mahuika_modules

get_licences()

# attach tags
# pull from repo
domain_tags = c.pull("https://github.com/nesi/modlist/raw/master/domain_tags.json")

if isinstance(domain_tags, dict):
    c.writemake_json("cache/domain_tags.json", domain_tags)
else:
    log.error("Using cached version of domain tags")
    domain_tags = c.readmake_json(
        "cache/domain_tags.json"
    )

licence_tags = c.pull("https://github.com/nesi/modlist/raw/master/licence_tags.json")

if isinstance(domain_tags, dict):
    c.writemake_json("cache/licence_tags.json", domain_tags)
else:
    log.error("Using cached version of licence tags")
    domain_tags = c.readmake_json(
        "cache/licence_tags.json"
    )

c.assign_tags(all_modules, "domains", domain_tags)
c.assign_tags(all_modules, "licence_type", licence_tags)

darcs_log = get_darcs_log()

timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
log.info("Updated as of " + timestamp)

output_dict = { "modules": all_modules, "date": timestamp, "darcs_log":darcs_log }

c.post_kafka('environment-module-tracking', output_dict)
c.writemake_json("module_list.json", output_dict)

log.info("DONE!")
