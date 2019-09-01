import subprocess, math, os, stat, re, datetime, json, logging

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
import common as c
from copy  import deepcopy 


from pwd import getpwuid
from grp import getgrgid

# Three different resources to track.
# - SLURM licence token.

# Master object structure.
day_weighting=0.2
log = logging.getLogger(__name__)

licence_primitive={"software": "","number": "","institution": "","institution_alias": "","faculty": "","faculty_alias": "","lic_type": "","server_type": "","file_address": "","file_group":"","feature": "","flex_daemon": "","flex_method": "", "poll_period":100 ,"conditions": "", "history":[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "enabled": False}
flexlm_pattern = 'Users of APPLICATION_NAME: \(?Total of (\S+) licenses? issued; Total of (\S+) licenses? in use\)?'.replace(' ', ' +')

# class Lic:
#     """A simple example class"""
#     def __init__(self, token):
#         self.token = token

#     software = ""
#     number : ""
#     institution: ""
#     institution_alias: ""
#     faculty: ""
#     faculty_alias: ""
#     lic_type: ""
#     server_type: ""
#     file_address: ""
#     file_group: ""
#     feature: ""
#     flex_daemon: ""
#     flex_method: ""
#     poll_period: 100
#     conditions: ""
#     history: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
#     enabled: False

#     def to_json(self):
#         return json.dumps(self.__dict__)

#     @classmethod
#     def from_json(cls, json_str):
#         json_dict = json.loads(json_str)
#         return cls(**json_dict)

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

def update_overwrites(licences):
    """Updates overwrite file with primitive of any new licences in dict"""

    overwrites=c.readmake_json('cache/licence_meta_overwrite.json')
    for key in licences.keys():
        if not key in overwrites:
            overwrites[key] = deepcopy(licence_primitive)
            c.log.warning('New primitive', key, ' added to licence_meta_overwrite')
    
    c.writemake_json('cache/licence_meta_overwrite.json', overwrites)

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

def lmutil(key, value):
    """Checks number of available licences for all objects passed"""
    
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
                count_lic=float(m.groups()[1])
                # Adjust history value, unless zero then set.
                value["history"][hour_index] = round(((count_lic*day_weighting) + (value["history"][hour_index]*(1-day_weighting))),2) if value["history"][hour_index] else count_lic
                # m.groups()[1]
                # print(tuple([int(n) for n in m.groups()]))
                log.info("Adjusted mean value for hour " + str(hour_index) + " :" + value["history"][hour_index])

    return

def update_history(licences):
    """Gets history data from previous license object"""
    prev=c.readmake_json('cache/licence_meta.json')
    
    for key, value in licences.items():
        if key in prev and prev[key]['history']:
            value['history'] = prev[key]['history']
        
def validate_lic_file(key,value):

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


def attach_lic(licence_dat, module_dat):
    """Attach licence info for every application"""
    for lic, lic_value in licence_dat.items():     
        #Check if in array of licences
        for app, app_value in module_dat.items():   
            #If same, case insensitive.
            
            if app.lower() == lic_value["software"].lower():
                app_value["licences"][lic]=lic_value
                log.info('Licence ' + lic + ' attached to ' + app)
        
#   stat.S_ISREG(statdat.st_mode)