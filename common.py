import os, json, requests, logging

log = logging.getLogger(__name__)

def pull(address):
    request=requests.get(address)
    if request.status_code == 200:
        try:
            out_dict=request.json()
            return out_dict
        except:
            log.error("Failed to convert request from " + address + " to dictionary.")  
            print(request.content) 
    else:
        log.error("Failed to pull from " + address + " (" + request.status_code + ").")
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

def deep_merge(over, under):
    """Deep merges dictionary
    If conflict 'over' has right of way"""

    for key, value in over.items():
        
        #If element is duplicate named dict, call self.
        if isinstance(value, dict) and key in under:

            log.debug("Recursed")
            node = under.setdefault(key, {})
            deep_merge(value, node)

        #If elements are unique dict.
        if isinstance(value, dict) and key not in under:

            log.debug("Key Added")
            #log.debug(key + " :" + value + " ")
            under[key]=value

        #If element is list (and non unique) append
        elif isinstance(value, list) and key in under:

            log.debug("Merged Successfully")
            
            #For each member of list
            for thing in value:
                #Not duplicate
                if not thing in under[key]:
                    under[key].append(thing)
        #No merge needed
        elif isinstance(value, list) and key not in under:

            log.debug("Merged Successfully")
            under.update(over)

        #If element is other, replace.      
        elif not (isinstance(value, list) or isinstance(value, dict)):

            if key not in under:
                under[key] = value
            elif under[key] == value:
                log.debug("No Merge Required")
            else:
                if value:
                    log.debug(str(under[key]) + ' replaced with ' + str(value))
                    under[key] = value

    return under