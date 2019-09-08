import subprocess, math, os, stat, re, json, logging, time
import datetime as dt

import common as c
from copy import deepcopy
from pwd import getpwuid
from grp import getgrgid

def instintate_licences(licence_list,licence_meta):
    """Updates overwrite file with primitive of any new licences in dict"""

    for licence in licence_meta.keys():
        if not licence in licence_list:
            log.warning(licence + " is new licence. Being added to database wih default values.")
            licence_list[licence] = deepcopy(settings["default"])
    #c.writemake_json("tags/licence_meta.json", licence_meta)

def lmutil(licence_list):
    """Checks total of available licences for all objects passed"""

    flexlm_pattern = "Users of APPLICATION_NAME: \(?Total of (\S+) licenses? issued; Total of (\S+) licenses? in use\)?".replace(" ", " +")

    for key, value in licence_list.items():
        if value["file_address"] and value["feature"] and value["flex_method"] == "lmutil":
            c.log.info("Checking Licence server at " + value["file_address"] + " for '" + value["feature"] + "'.")
            pattern = flexlm_pattern.replace("APPLICATION_NAME", value["feature"])
            try:
                for line in (
                    subprocess.check_output(
                        "linx64/lmutil " + "lmstat " + "-f " + value["feature"] + " -c " + value["file_address"], stderr=subprocess.STDOUT, shell=True
                    )
                    .decode("utf-8")
                    .split("\n")
                ):
                    log.debug(line)
                    m = re.match(pattern, line)
                    if m:
                        hour_index = dt.datetime.now().hour - 1
                        value["in_use_real"] = float(m.groups()[1])

                        # Record to running history 
                        value["history"].append(value["in_use_real"])

                        # Pop extra array entries
                        while len(value["history"]) > value["history_points"] :
                            value["history"].pop(0)

                        # Find modified in use value
                        interesting=max(value["history"])
                        value["in_use_modified"] = min(max(interesting+value["buffer_constant"], round(interesting*(1+value["buffer_factor"]))), value["total"])


                        # Set if unset
                        if not len(value["day_ave"])==24 :
                            value["day_ave"]=[0]*24

                        # Update average
                        value["day_ave"][hour_index] = (
                            round(
                            ((value["in_use_real"] * settings["point_weight"]) + (value["day_ave"][hour_index] * (1 - settings["point_weight"]))), 2)
                            if value["day_ave"][hour_index]
                            else value["in_use_real"]
                        )
                        log.info(key + ": " + str(value["in_use_real"]) + " licences in use. Historic set to " + str(value["day_ave"][hour_index]))
                    else:
                        log.debug("Feature '" + value["feature"] + "' not found on server.")

            except:
                log.error("Failed to fetch " + key + " for unspecified reason")

    return

def validate(licence_list, licence_meta):
    """Checks for inconsistancies"""
    def _address(licence_list, licence_meta):
        for key, value in licence_list.items():
            if value["file_address"]:


                filename_end = "_" + value["faculty"] if value["faculty"] else ""

                standard_address = 'opt/nesi/mahuika/' + value["software_name"] + '/Licenses/' + value["institution"] + filename_end + '.lic'
                try:
                    statdat = os.stat(value["file_address"])
                    file_name = value["file_address"].split("/")[-1]

                    owner = getpwuid(statdat.st_uid).pw_name
                    group = getgrgid(statdat.st_gid).gr_name

                    # Check permissions of file
                    if statdat.st_mode == 432:
                        log.error(key + " file address permissions look weird.")

                    if value["file_group"] and group != value["file_group"]:
                        log.warning(key + ' file address group is "' + group + '", should be "' + value["file_group"] + '".')

                    if owner != settings['user']:
                        log.warning(key + " file address owner is '" + owner + "', should be '" + settings['user'] + "'.")

                    if value["file_address"] != standard_address:
                        log.warning('Would be cool if "' + value["file_address"] + '" was "' + standard_address + '", but no biggy.')

                except:
                    log.error(key + ' has an invalid file path attached "' + value["file_address"] + '"')
            else:
                log.error(key + " has no licence file associated.")

    _address(licence_list, licence_meta)

def apply_soak(licence_list):

    soak_count=""

    for key, value in licence_list.items():
        if value["enabled"]:
            soak_count += (key + ":" + str(round(value["in_use_modified"])) + ",")
        # Does nothing atm, idea is be able to set max total in use on cluster.
        #value.max_use

    cluster = "mahuika"
    res_name="licence_soak2"
    # starts in 1 minute, ends in 1 year.
    default_reservation = {
        "StartTime": (dt.datetime.now() + dt.timedelta(seconds=10)).strftime(("%Y-%m-%dT%H:%M:%S")),
        "EndTime": (dt.datetime.now() + dt.timedelta(days=365)).strftime(("%Y-%m-%dT%H:%M:%S")),
        "Users": "root",
        "Flags": "LICENSE_ONLY",
    }
    # 2009-02-06T16:00:00
    default_reservation_string = ""
    for key, value in default_reservation.items():
        default_reservation_string+= " " + key + "=" + str(value)

    try:
        sub_input="scontrol update -M " + cluster +" ReservationName=" + res_name + " licenses=\"" + soak_count + "\""
        log.debug(sub_input)
        subprocess.check_output(sub_input, shell=True).decode("utf-8")   
        log.info("Reservation updated successescsfully!")
    except:
        log.error("Failed to update 'licence_soak' attempting to create new reservation.")
        try:
            sub_input="scontrol create ReservationName=" + res_name + default_reservation_string + " licenses=\"" + soak_count + "\""
            log.debug(sub_input)
            subprocess.check_output(sub_input, shell=True).decode("utf-8")
            log.error("New reservation created successescsfully!")
        except:
            log.error("Failed! Everything failed!")

def main():
    # Checks all licences in "meta" are in "list"
    instintate_licences(licence_list, licence_meta)

    # Updates "list" with "meta" properties.
    c.deep_merge(licence_meta, licence_list)

    validate(licence_list, licence_meta)

    if not os.environ["USER"] == settings['user']:
        log.warning("LMUTIL skipped as user not '" + settings['user'] + "'")
        log.warning("APPLY_SOAK skipped as user not '" + settings['user'] + "'")
    else:
        apply_soak(licence_list)
        lmutil(licence_list)

    

    c.writemake_json('licence_list.json', licence_list)



# ===== Log Stuff =====#
log_path="warn.logs" 
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Log Info to console USE ENV VARIABLE LOGLEVEL TO OVERRIDE
console_logs = logging.StreamHandler()
console_logs.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
console_logs.setLevel(os.environ.get("LOGLEVEL", "INFO"))
log.addHandler(console_logs)

# Log warnings and above to text file.
file_logs = logging.FileHandler(log_path)
file_logs.setLevel("WARNING")
file_logs.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(file_logs)


log.info("Starting...")

settings = c.readmake_json('licence_collector_settings.json')

c.dummy_checks()

log.info(json.dumps(settings))

licence_meta = c.readmake_json("meta/licence_meta.json")
licence_list = c.readmake_json("licence_list.json")

# Is correct user
if os.environ["USER"] != settings['user']:
    log.error("COMMAND SHOULD BE RUN AS '" + settings['user'] + "' ELSE LICENCE STATS WONT WORK")

while 1:
    looptime=time.time()
    main()
    log.info("main loop time = " + str(time.time()-looptime))
    time.sleep((settings["poll_period"]-(time.time()-looptime)))



# def attach_lic(licence_dat, module_dat):
#     """Attach licence info for every application"""
#     for lic, lic_value in licence_dat.items():
#         # Check if in array of licences
#         for app, app_value in module_dat.items():
#             # If same, case insensitive.

#             if app.lower() == lic_value["software"].lower():
#                 app_value["licences"][lic] = lic_value
#                 log.info("Licence " + lic + " attached to " + app)

# def validate_slurm_tokens(license_list):
#     string_data = subprocess.check_output("sacctmgr -pns show resource withcluster", stderr=subprocess.STDOUT, shell=True).decode("utf-8").strip()
#     token_list = {}

#     for lic_string in string_data.split("\n"):

#         log.debug(lic_string)

#         lic_string_array = lic_string.split("|")
#         pre_at = lic_string_array[0].split("_")
#         post_at = lic_string_array[1].split("_")

#         token = lic_string_array[0] + "@" + lic_string_array[1]
#         if "token" not in token_list:
#             token_list[token] = {}  # deepcopy(licence_primitive)
#             print("not in list")

#         token_list[token]["software"] = pre_at[0]
#         token_list[token]["institution"] = post_at[0]
#         token_list[token]["total"] = math.floor(int(lic_string_array[3]) / 2)

#         if len(pre_at) > 1:
#             token_list[token]["lic_type"] = pre_at[1]

#         if len(post_at) > 1:
#             token_list[token]["faculty"] = post_at[1]

#         token_list[token]["server_type"] = lic_string_array[5]

#         if "cluster" in token_list[token]:
#             # print('in list')

#             token_list[token]["cluster"].append(lic_string_array[6])
#         else:
#             # print('not in list')

#             token_list[token]["cluster"] = [lic_string_array[6]]

#         token_list[token]["percent"] = [lic_string_array[7]]

#     print(token_list)

#     for token_name, licence_properties in license_list.items():

#         if token_name in token_list:
#             log.info(token_name + " has asocciated slurm token ")
#             for key, value in token_list[token_name].items():

#                 if key == "percent":
#                     if value != 50:
#                         log.debug("Percent allocated should be 50% (even if not on Maui)")
#                 elif value != licence_properties[key]:
#                     log.debug("Slurm token value " + key + " is " + json.dumps(value) + "and should be" + json.dumps(licence_properties[key]))

#         else:
#             log.error(token_name + " has NO asocciated slurm token ")

#         # if "enabled" in value and value['enabled']:
#         # else:
#         #     log.info("Licence object " + key + " is disabled and will not be evaluated.")

# def update_history(licences):
    # """Gets history data from previous license object"""
    # prev = c.readmake_json("cache/licence_meta.json")

    # for key, value in licences.items():
    #     if key in prev and prev[key]["history"]:
    #         value["history"] = prev[key]["history"]
# def add_new(licences):
#     for key, value in licences.items():

#  def generate_update(licences):
#     """Generates an object to pass to the licence checker"""
#     lmutil_obj = {}

#     for key, value in licences.items():
#         if value["flex_method"]:
#             if value["file_address"]:
#                 if value["file_address"] not in lmutil_obj:
#                     lmutil_obj[value["file_address"]] = []
#                 lmutil_obj[value["file_address"]].append({"feature": value["feature"], "licence": key, "count": 0})
#     return lmutil_obj

# def assign_aliases(licences):

#     aliases = c.readmake_json("cache/institutional_alias.json", {"inst": {}, "fac": {}})
#     for key, value in licences.items():

#         if value["institution"] in aliases["inst"]:
#             value["institution_alias"] = aliases["inst"][value["institution"]]

#         if value["faculty"] in aliases["fac"]:
#             value["faculty_alias"] = aliases["fac"][value["faculty"]]

