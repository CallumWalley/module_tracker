#!/usr/bin/python
import subprocess
import re
import datetime
import copy
import requests

import common as c
from common import log




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
        log.error("Failed to read darcs log: " + str(details))

def module_avail(machine, module_path):

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
        

        if not line or line[0]=='/':
            log.debug("Path, skipping.")
            continue

        thisApp = line.split("/")[0].strip()
        
        if not len(thisApp) > 0:
            log.debug("Zero, skipping.")
            continue
        # Check nonzero
        
        # If new app, add to dictionary.
        if lastApp != thisApp:

            # Define dict
            main_dict[thisApp] = copy.deepcopy(settings["default"])
            main_dict[thisApp]["machines"][machine] = []

            try:
                shell_string="MODULEPATH='" + module_path + "';" + "/usr/share/lmod/lmod/libexec/lmod -t whatis " + thisApp
                log.debug(shell_string)
                data = subprocess.check_output(shell_string, stderr=subprocess.STDOUT, shell=True).decode("utf-8").split("MODULEPATH")[0]
            except Exception as details:
                log.error("Module whatis for " + thisApp + " failed: " + str(details))
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

def get_licences():

    # Kinda gross compared with other code.
    licence_object=c.readmake_json(settings["licences_path"])

    try:
        request = requests.get("https://raw.githubusercontent.com/nesi/modlist/master/aliases.json")
        if request.status_code == 200:
            try:
                alias = request.json()
            except Exception as details: 
                log.error("Failed to parse request from " +"https://raw.githubusercontent.com/nesi/modlist/master/aliases.json" + " to dictionary: " + str(details))
                log.debug(str(request.content))
                return 1
    except Exception as details:
        log.error("Failed to pull from " + "https://raw.githubusercontent.com/nesi/modlist/master/aliases.json" + " : " + str(details))
        log.error("Using cached version of aliaseses")
        alias = c.readmake_json(
            "cache/alias.json"
            )
    else:
        c.writemake_json("cache/alias.json", alias)

    for module_name, module_values in all_cluster_modules.items():
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

def get_tags():
    # Where standard format is:
    # module_list.json
    # {
    #   module1:{
    #       property1:[value1, value2]
    #   },
    #   module2:{
    #       property1:[value1, value2]
    #   }
    # }
    #
    # I'm defining a 'tag' as an attribute defined in the inverted format:
    # property1_tags.json
    # {
    #   value1:[module1, module2],
    #   value2:[module1, module2]
    # }
    # This makes it easier for people to assign optional tags
    for key, value in settings["tags"].items():
        if value["enabled"]:
            try:
                request = requests.get(value["remote"])
                if request.status_code == 200:
                    try:
                        tag_values = request.json()
                    except Exception as details: 
                        log.error("Failed to parse request from " + value["remote"] + " to dictionary: " + str(details))
                        log.debug(str(request.content))
                        return 1
            except Exception as details:
                log.error("Failed to pull from " + value["remote"] + " : " + str(details))
                log.error("Using cached version of " + key + " tags")
                tag_values = c.readmake_json(
                    value["cache"]
                )
            else:
                c.writemake_json(value["cache"], tag_values)

            log.debug(tag_values)
            for tag_key, module_value in tag_values.items():
                for module in module_value:
                    if module in all_cluster_modules.keys():
                        # If list, append
                        if isinstance(all_cluster_modules[module][key], list):
                            if not tag_key in all_cluster_modules[module][key]:
                                all_cluster_modules[module][key].append(tag_key)
                                all_cluster_modules[module][key].sort()
                        # Else overwrite
                        else:
                            all_cluster_modules[module][key] = tag_key
                    else:
                        log.warning("Tag '" + module + "' does not correspond to a application on the platform.")

def get_overwrites():
    overwrite_values=""
    remote=settings["master_overwrite"]["remote"]
    try:
        request = requests.get(remote)
        if request.status_code == 200:
            try:
                overwrite_values = request.json()
            except Exception as details: 
                log.error("Failed to parse request from " + remote + " to dictionary: " + str(details))
                log.debug(str(request.content))
                return 1
    except Exception as details:
        log.error("Failed to pull from " + remote + " : " + str(details))
        log.error("Using cached version of overwrites")
        overwrite_values = c.readmake_json(
            settings["master_overwrite"]["cache"]
        )
    else:
        c.writemake_json(settings["master_overwrite"]["cache"], overwrite_values)

    c.deep_merge(all_cluster_modules, overwrite_values)
settings = c.readmake_json("settings.json")

log.info("Starting...")
all_cluster_modules={}

# ====== Module list ======#
# Read cached data;
for key, value in settings["clusters"].items():
    if value["enabled"]:
        if value["update"]:
            cluster_modules = module_avail(key, value["module_path"])
            c.writemake_json(value["cache"], cluster_modules)
            #log_changes("mahuika", modules_cache, mahuika_modules_cache)
        else:
            cluster_modules = c.readmake_json(value["cache"], {})
        # Merge with combined dictionary.
        c.deep_merge(copy.deepcopy(cluster_modules), all_cluster_modules)

# ==== Attach Licences ====#
get_licences()

# ====== Attach Tags ======#
get_tags()
# ==== Apply Overwrites ===#
get_overwrites()
# ===== Get Darcs Log =====#
darcs_log = get_darcs_log()

# ==== Get time of run ====#
timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

log.info("Updated as of " + timestamp)

output_dict = { "modules": all_cluster_modules, "date": timestamp, "darcs_log":darcs_log }
try:
    c.post_kafka('environment-module-tracking', output_dict)
except Exception as details:
    log.info("Push to Kafka failed: " + str(details))

c.writemake_json("module_list.json", output_dict)

log.info("DONE!")

# def log_changes(name, new, old):
#     # Compares new object with cached object and logs differences.
#     diff_ = c.deep_merge(new, old, True)
#     if len(diff_) > 0:
#         with open("diff_log.txt", "a") as diff_file:
#             diff_file.write(
#                 "\n====================="
#                 + " " + name +" - "
#                 + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
#                 + " =====================\n"
#                 + diff_
#             )
#     log.info("Comparing " + name + " modules with cache")
