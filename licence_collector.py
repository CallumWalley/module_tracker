import subprocess, math, os, stat, re, datetime, json, logging

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
import common as c
from copy  import deepcopy 


from pwd import getpwuid
from grp import getgrgid

# Three different resources to track.
# - SLURM licence token.

# Master object structure.

licence_primitive={"software": "","number": "","institution": "","institution_alias": "","faculty": "","faculty_alias": "","lic_type": "","clusters": [], "server_type": "","file_address": "","file_group":"","feature": "","flex_daemon": "","flex_method": "", "poll_period":100 ,"conditions": "", "history":[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "enabled": False}
flexlm_pattern = 'Users of APPLICATION_NAME: \(?Total of (\S+) licenses? issued; Total of (\S+) licenses? in use\)?'.replace(' ', ' +')

# A day is worth this much when compared with previous average
day_weighting=0.2
log = logging.getLogger(__name__)

def get_slurm_tokens():

    # Init dictionary
    lic_dict={}

    # Check slurm resources
    string_data=subprocess.check_output("sacctmgr -pns show resource", stderr=subprocess.STDOUT, shell=True).decode("utf-8").strip()

    for lic_string in string_data.split('\n'):
    
        lic_string_array=lic_string.split('|')

        pre_at=lic_string_array[0].split('_')
        post_at=lic_string_array[1].split('_')

        token=(lic_string_array[0] + "@" + lic_string_array[1])

        lic_dict[token]=deepcopy(licence_primitive)

        lic_dict[token]['software'] = pre_at[0]
        lic_dict[token]['institution'] = post_at[0]

        lic_dict[token]['number']=math.floor(int(lic_string_array[3])/2)

        if len(pre_at)>1: 
            lic_dict[token]['lic_type'] = pre_at[1]

        if len(post_at)>1:
            lic_dict[token]['faculty'] = post_at[1] 

        lic_dict[token]['server_type']=lic_string_array[5]

    return lic_dict

def update_overwrites():
    """Updates overwrite file with primitive of any new licences in dict"""

    licence_meta=c.readmake_json('tags/licence_meta.json')
    slurm_tokens=get_slurm_tokens()

    for key in slurm_tokens.keys():
        if not key in licence_meta:
            licence_meta[key] = deepcopy(slurm_tokens[key])
            c.log.info('New primitive' + key + ' added to licence_meta')
    
    c.writemake_json('tags/licence_meta.json', licence_meta)



def assign_aliases(licences):

    aliases=c.readmake_json('cache/institutional_alias.json',{"inst":{},"fac":{}})

    for key, value in licences.items():

        if value['institution'] in aliases['inst']:
            value['institution_alias'] = aliases['inst'][value['institution']]

        if value['faculty'] in aliases['fac']:
            value['faculty_alias'] = aliases['fac'][value['faculty']]

def generate_update(licences):

    """Generates an object to pass to the licence checker"""
    lmutil_obj={}

    for key, value in licences.items():
        if value['flex_method']:
            if value['file_address']: 
                if value['file_address'] not in lmutil_obj:
                    lmutil_obj[value['file_address']]=[]
                lmutil_obj[value['file_address']].append({'feature':value['feature'], 'licence':key, 'count':0})
    return lmutil_obj

def lmutil(licence_list):
    """Checks number of available licences for all objects passed"""
    for key, value in licence_list.items():

        if not os.environ['USER'] == 'nesi-apps-admin':
            log.warning("LMUTIL for '" + key + "' skipped as user not 'nesi-apps-admin'")
            return
            
        if value['file_address'] and value['feature'] and value['flex_method'] == 'lmutil' :
            c.log.info("Checking Licence server at " + value['file_address'] + " for '" + value['feature'] + "'." )
            pattern = flexlm_pattern.replace('APPLICATION_NAME', value['feature'])
            
            for line in subprocess.check_output('linx64/lmutil ' + 'lmstat ' + '-f ' + value['feature'] + ' -c ' + value['file_address'], stderr=subprocess.STDOUT, shell=True).decode("utf-8").split('\n'):
                m = re.match(pattern, line)
                if m:                
                    hour_index=datetime.datetime.now().hour-1
                    value['in_use']=float(m.groups()[1])
                    log.info(value['in_use'] + " licences in use.")

                    # Adjust history value, unless zero then set.
                    value["history"][hour_index] = round(((value['in_use']*day_weighting) + (value["history"][hour_index]*(1-day_weighting))),2) if value["history"][hour_index] else value['in_use']
                    log.info("Adjusted mean value for hour " + str(hour_index) + " :" + value["history"][hour_index])

    return

def update_history(licences):
    """Gets history data from previous license object"""
    prev=c.readmake_json('cache/licence_meta.json')
    
    for key, value in licences.items():
        if key in prev and prev[key]['history']:
            value['history'] = prev[key]['history']
        
def validate_lic_file(key, value):

    if value['file_address']:
        try:
            statdat = os.stat(value['file_address'])

            file_name=value['file_address'].split('/')[-1]

            owner=getpwuid(statdat.st_uid).pw_name
            group=getgrgid(statdat.st_gid).gr_name

            # Check permissions of file
            if statdat.st_mode == 432:
                log.error(key + ' file address permissions look weird.')

            if value['file_group'] and group != value['file_group']:
                log.warning(key + ' file address group is "' +  group + '", should be "' + value['file_group'] + '".')

            if owner != "nesi-apps-admin":
                log.warning(key + ' file address group is "' +  group + '", should be "nesi-apps-admin".')

            standard_address=("opt/nesi/mahuika/" + value['software'] + "/Licenses/" + value['software'].lower() + "_" + value['lic_type'].lower() + "@" +  value['institution'] + "_" + value['faculty'] + ".lic" )

            if value['file_address'] != standard_address:
                log.warning('Would be cool if "' + value['file_address'] + '" was "' + standard_address + '", but no biggy.')

        except FileNotFoundError:
            log.error(key + ' has an invalid file path attached "' + value['file_address'] + '"')

    else:
        log.error(key + ' has no licence file associated.')
    
    return
    
#def apply_soak(licence_list):

   # os.subprocess.check_output('scontrol' + 'update' + 'res=LS_' + licence.token  ' licenses=' + soak_count, shell=True).decode("utf-8"))


def attach_lic(licence_dat, module_dat):
    """Attach licence info for every application"""
    for lic, lic_value in licence_dat.items():     
        #Check if in array of licences
        for app, app_value in module_dat.items():   
            #If same, case insensitive.
            
            if app.lower() == lic_value["software"].lower():
                app_value["licences"][lic]=lic_value
                log.info('Licence ' + lic + ' attached to ' + app)
        
# # stat.S_ISREG(statdat.st_mode)

# # while True:
# #    if datetime.time(8, 0) < datetime.datetime.now().time() < datetime.time(17,0):
#     interesting = False
#     settings = []
#     for (license_token, tracker) in trackers:
#         (target, current) = next(tracker)
#         if target != current:
#             interesting = True
#         settings.append(license_token + ':' + str(target))
#     if interesting:
#         cmd = ['scontrol', 'update', 'res=license_soak', 'licenses='+','.join(settings)]
#         print("command", make_timestamp_text(), *cmd)
#         try:
#             check_output(cmd)
#         except Exception as details:
#             print('scontrol', details)
        
# #     time.sleep(POLL)    

def validate_slurm_tokens(license_list):
    string_data=subprocess.check_output("sacctmgr -pns show resource withcluster", stderr=subprocess.STDOUT, shell=True).decode("utf-8").strip()
    token_list={}

    for lic_string in string_data.split('\n'):

        log.debug(lic_string)

        lic_string_array=lic_string.split('|')
        pre_at=lic_string_array[0].split('_')
        post_at=lic_string_array[1].split('_')

        token=(lic_string_array[0] + "@" + lic_string_array[1])
        if 'token' not in token_list:
            token_list[token]={}#deepcopy(licence_primitive)
            print('not in list')

        token_list[token]['software'] = pre_at[0]
        token_list[token]['institution'] = post_at[0]
        token_list[token]['number']=math.floor(int(lic_string_array[3])/2)

        if len(pre_at)>1: 
            token_list[token]['lic_type'] = pre_at[1]

        if len(post_at)>1:
            token_list[token]['faculty'] = post_at[1] 

        token_list[token]['server_type']=lic_string_array[5]

        if 'cluster' in token_list[token]:
            #print('in list')

            token_list[token]['cluster'].append(lic_string_array[6])
        else:
            #print('not in list')

            token_list[token]['cluster']=[lic_string_array[6]]

        token_list[token]['percent']=[lic_string_array[7]]

    print(token_list)

    for token_name, licence_properties in license_list.items():

        
        if token_name in token_list:
            log.info(token_name + " has asocciated slurm token ")
            for key, value in token_list[token_name].items():
                
                if key == 'percent':
                    if value != 50:
                        log.debug("Percent allocated should be 50% (even if not on Maui)")
                elif value != licence_properties[key]:
                    log.debug("Slurm token value " + key + " is " + json.dumps(value) + "and should be" +  json.dumps(licence_properties[key]))


        else:
            log.error(token_name + " has NO asocciated slurm token ")
        
        # if "enabled" in value and value['enabled']:
        # else:   
        #     log.info("Licence object " + key + " is disabled and will not be evaluated.")
    


def main():

    # Is correct user
    if os.environ['USER'] != "nesi-apps-admin":
        log.error(
            "COMMAND SHOULD BE RUN AS 'nesi-apps-admin' ELSE LICENCE STATS WONT WORK")

    licence_list = c.readmake_json('licence_list.json')

    for key, value in licence_list.items():
        validate_lic_file(key, value)

    
    #validate_slurm_tokens(licence_list)
    # Licence Stuff
    # slurm_dat = get_slurm_tokens()
    # lc.update_overwrites(slurm_dat)

    # licence_meta = c.readmake_json('tags/licence_meta.json')
    # c.deep_merge(licence_meta, slurm_dat)
    # licence_list=slurm_dat



    # for key, value in licence_list.items():
    #     lmutil(key, value)

    # assign_aliases(licence_list)   # Assign licence aliases if any.
    # update_history(licence_list)   # Loads previous history data.

    # # lc.attach_lic(lic_dat, module_dat) # Attach Licence Data to module data.
    # licence_tags = c.readmake_json('tags/licence_tags.json', {"proprietary": []})
    # c.assign_tags(licence_list, "licence_type", licence_tags)
    # c.writemake_json('licence_list.json', licence_list)


main()

