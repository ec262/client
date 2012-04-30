''' discovery_interface.py
    A module for dealing with all things related to the discovery service.
'''

import json
import base64
import requests
from Crypto.Cipher import AES

DISCOVERY_SERVICE_URL = "http://ec262discoveryservice.herokuapp.com"
DEFAULT_PORT = 2626
DEFAULT_TTL = 60

###############################################
################# Exceptions ##################
###############################################

class ServerError(Exception):
    def __init__(self, code=None, response=None):
        self.code = code
        self.response = response
        
    def __str__(self):
        return "Status: " + self.code + "\n" + self.response

class InsufficientCredits(Exception):
    def __init__(self, available=None, needed=None):
        self.available_credits = available_credits
        self.needed_credits = needed_credits
        
    def __str__(self):
        return "Available credits: %s; Needed credits: %s" % \
                (self.available_credits, self.needed_credits)
        
class UnknownTask(Exception):
    def __init__(self, task_id=None):
        self.task_id = task_id
        
    def __str__(self):
        return "Task ID: %s" % (self.task_id)
    
####################################################
################# Helper methods ###################
####################################################

def _to_json_list(data):
    ''' Data is a dictionary, but unfortunately keys can be in arbitrary
        order. To ensure that data hashes deterministically, we first produce
        a list of [key, value] lists, sorted by key. Then we serialize the
        data to JSON, and pad it until it it a multiple of 8 bytes so that
        encryption is easier.
    '''
    # Turn {"b": 1, "a": 2} into [["b", 2], ["a", 2]]
    list_data = list(data.iteritems())
    
    # Turn [["b", 1], ["a", 2]] into [["a", 2], ["b", 1]]
    list_data.sort(key=lambda x: x[0])
    
    # Convert to JSON and pad with whitespace
    json_str = json.dumps(list_data)
    bytes_needed = len(json_str) % 8
    json_str += " " * bytes_needed
    
    return json_str

def _from_json_list(data):
    ''' Returns a dict from data encoded as a JSON list. '''
    return dict(json.loads(data))

# Test JSON list conversion
data = {"b": 1, "a": 2}
json_list_data = _to_json_list(data)

print str(json_list_data)
print expected_str
assert expected_str == '[["a", 2], ["b", 1]]'
assert str(json_list_data) == expected_str
assert _from_json_list(json_list_data) == data

def _crypt_data(data, task_id, encrypt=True):
    ''' Requests a key from the server and returns encrypted or decrypted
        data. Data should be encoded as a dictionary.
    '''
    url = DISCOVERY_SERVICE_URL + "/tasks/" + task_id
    method = 'get' if encrypt else 'delete'
    payload = {"valid": 1} # Only necessary for encryption, but whatever
    response = requests.request(method, url, data=payload)
    
    if response.status_code == requests.codes.ok:
        key_dict = json.loads(request.text)
        key = base64.b64decode(key_dict["key"])
        encryptor = AES.new(key, AES.MODE_CBC)
        if encrypt:
            encrypted_data = encryptor.encrypt(_to_json_list(data))
            return encrypted_data
        else:
            decrypted_data = encryptor.decrypt(_from_json_list(data))
            return decrypted_data
        
    elif response.status_code == 404:
        raise UnknownTask(task_id=task_id)
    else:
        raise ServerError(code=response.status_code, response=response.text)

###################################################
################# Public methods ##################
###################################################

def register_worker(port=DEFAULT_PORT, ttl=DEFAULT_TTL):
    ''' Registers a worker with the discovery service. Registrations last for
        1m by default; workers should periodically re-register and 
        need to register after completing a task. Returns a dictionary with
        all known info about the worker.
    '''
    url = DISCOVERY_SERVICE_URL + "/workers"
    payload = {"port": port, "ttl": ttl}
    response = requests.post(url, data=payload)
    if response.status_code == requests.codes.ok:
        return json.loads(request.text)
    else:
        raise ServerError(code=response.status_code, response=response.text)

def get_tasks(num_tasks):
    ''' Get a list of tasks and workers from the discovery service. Returns a
        dictionary of the form:
        { "1": ["worker1:port", "worker2:port", "worker3:port"],
          "2": ["worker4:port", ... ], ... }
    '''
    url = DISCOVERY_SERVICE_URL + "/tasks"
    payload = {"n": num_tasks}
    response = requests.post(url, data=payload)
    
    if response.status_code == requests.codes.ok:
        return json.loads(request.text)
    elif response.result == 406:
        info = json.loads(request.text)
        raise InsufficientCredits(available=info["available_credits"],
                                  needed=info["needed_credits"])
    else:
        raise ServerError(code=response.status_code, response=response.text)

def encrypt_data(data, task_id):
    ''' Encrypts the data (encoded as a dictionary) corresponding to a given
        task so that it can be sent over the wire.
    '''
    return _crypt_data(data, task_id, encrypt=True)
        
def decrypt_data(data, task_id):
    ''' Decrypts the data with the given task ID. Only use this if the
        encrypted data returned by all three workers is the same; foreman will
        not be able to get credits back once they call it. If the data does
        not check out and the foreman wants a refund, use invalidate_data().
    '''
    return _crypt_data(data, task_id, encrypt=False)
    
def invalidate_data(task_id):
    ''' Get a refund for the given task ID. Once this is used, the data cannot
        be decrypted. Returns the number of credits the caller now has.
    '''
    url = DISCOVERY_SERVICE_URL + "/tasks" + task_id
    payload = {"valid": 0}
    response = requests.delete(url, data=payload)
    if response.status_code == requests.codes.ok:
        credits_dict = json.loads(request.text)
        return credits_dict["credits"]
    else:
        raise ServerError(code=response.status_code, response=response.text)
    
    