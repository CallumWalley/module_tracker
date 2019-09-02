import os
import json
import requests
import logging
import datetime
log = logging.getLogger(__name__)

def pull(address):
    request = requests.get(address)
    if request.status_code == 200:
        try:
            out_dict = request.json()
            return out_dict
        except:
            log.error("Failed to convert request from " + address +
                      " to dictionary.")
            print(request.content)
    else:
        log.error("Failed to pull from " + address + " (" +
                  request.status_code + ").")
    return 1


#Read file at {path}. If not exist make one with {default} value


def readmake_json(path, default={}):
    """Reads and returns JSON file as dictionary, if none exists one will be created with default value."""
    if not os.path.exists(path):
        log.error("No file at path '" + path + "'.")
        with open(path, "w") as json_file:
            json_file.write(json.dumps(default))
        log.error("Empty file created")

    with open(path) as json_file:
        log.info(path + " loaded.")
        return json.load(json_file)


def writemake_json(path, outject):
    with open(path, "w+") as json_file:
        json_file.write(json.dumps(outject))
        log.info(path + " updated")


def deep_merge(over, under, write_log=False):
    """Deep merges dictionary
    If conflict 'over' has right of way"""
    diff_log = ""
    #now=datetime.datetime.now()
    # Returns difference if write log true

    for key, value in over.items():

        if key in under:
            # If match, ignore.
            if under[key] == value:
                log.debug(key + ": no changes to make.")
                continue
            #If evaluates false, replace.
            elif not under[key]:

                log.debug("Property '" + key + "' SET to " + json.dumps(value))
                if write_log:
                    diff_log += ("Property '" + key +
                                              "' SET to " + json.dumps(value) +
                                              "\n")
                    log.info("Change written to log")
            # If (non-zero) dictionary
            elif isinstance(value, dict):
                # If dict key exists in both, we need to go deeper.
                if write_log:
                    diff_log += ("Inside " + key + "...\n")
                log.debug("Inside " + key + "...\n")
                node = under.setdefault(key, {})
                deep_merge(value, node, write_log)

            # Lists
            elif isinstance(value, list):
                #For each member of list
                for thing in value:
                    #Not duplicate
                    if not thing in under[key]:
                        under[key].append(thing)
                        log.debug("Property '" + key + "' appended with '" +
                                  json.dumps(thing) + "'")
                        if write_log:
                            diff_log += ("Property '" + key +
                                                      "' appended with '" +
                                                      json.dumps(thing) +
                                                      "'\n")
                            log.info("Change written to log")
            else:
                # Value replaced
                log.debug(key + " case 5")
                under[key] = value
                log.info("Property '" + key + "' CHANGED from '" + under[key] +
                         "' to '" + value + "'")
                if write_log:
                    diff_log += ("Property '" + key +
                                              "' CHANGED from '" + under[key] +
                                              "' to '" + value + "'\n")
                    log.info("Change written to log")

        else:
            # Set key equal to value
            under[key] = value
            log.debug("Property " + key + " SET to " + json.dumps(value))
            if write_log:
                diff_log += ("Property " + key + " SET to " +
                                          json.dumps(value))
                log.info("Change written to log")

    return diff_log
