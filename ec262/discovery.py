''' discovery.py
    A module for dealing with all things related to the discovery service.
'''

import json
import requests

from base64 import b64decode
from Crypto.Cipher import AES
from settings import DISCOVERY_SERVICE_URL, DEFAULT_PORT, DEFAULT_TTL

###############################################
################# Exceptions ##################
###############################################

class ServerError(Exception):
    def __init__(self, response=None):
        self.code = response.status_code
        self.content = response.content
        
    def __str__(self):
        return "Status: " + str(self.code) + "\n" + self.response

class InsufficientCredits(Exception):
    def __init__(self, response=None):
        self.available_credits = response["available_credits"]
        self.needed_credits = response["needed_credits"]
        
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
    byte_len = len(bytearray(json_str, 'utf-8'))
    bytes_needed = 16 - (byte_len % 16)
    json_str += " " * bytes_needed
    
    return json_str

def _from_json_list(data):
    ''' Returns a dict from data encoded as a JSON list. '''
    return dict(json.loads(data))

def _get_key(task_id, encryption=True):
    ''' Requests a key from the server for encryption/decrption'''
    
    url = DISCOVERY_SERVICE_URL + "/tasks/" + task_id
    method = 'get' if encryption else 'delete'
    payload = {"valid": 1} # Only necessary for encryption, but whatever
    response = requests.request(method, url, data=payload)
    
    if response.status_code == requests.codes.ok:
        key_dict = json.loads(response.content)
        return b64decode(key_dict["key"])
    elif response.status_code == 404:
        raise UnknownTask(task_id)
    else:
        raise ServerError(response)

def _crypt_data(data, key, encryption=True):
    ''' Encrypts/decrypts data encoded as a dictionary '''

    encryptor = AES.new(key, AES.MODE_CBC)
    if encryption:
        result = encryptor.encrypt(_to_json_list(data))
    else:
        result = _from_json_list(encryptor.decrypt(data))

    return result

###################################################
################# Public methods ##################
###################################################

def register_worker(port=DEFAULT_PORT, ttl=DEFAULT_TTL):
    ''' Registers a worker with the discovery service. Registrations last for
        1m by default; workers should periodically re-register and 
        need to register after completing a task. Returns a dictionary with
        all known info about the worker.
        Throws ServerError
    '''
    url = DISCOVERY_SERVICE_URL + "/workers"
    payload = {"port": port, "ttl": ttl}
    response = requests.post(url, data=payload)
    if response.status_code == requests.codes.ok:
        return json.loads(response.content)
    else:
        raise ServerError(response)

def get_tasks(num_tasks):
    ''' Get a list of tasks and workers from the discovery service. Returns a
        dictionary of the form:
        { "1": ["worker1:port", "worker2:port", "worker3:port"],
          "2": ["worker4:port", ... ], ... }
        Throws ServerError, InsufficientCredits
    '''
    url = DISCOVERY_SERVICE_URL + "/tasks"
    payload = {"n": num_tasks}
    response = requests.post(url, data=payload)
    
    if response.status_code == requests.codes.ok:
        return json.loads(response.content)
    elif response.result == 406:
        raise InsufficientCredits(json.loads(response.content))
    else:
        raise ServerError(response)

def encrypt_data(data, task_id):
    ''' Encrypts the data (encoded as a dictionary) corresponding to a given
        task so that it can be sent over the wire.
        Throws ServerError, UnknownTask
    '''
    encryption = True
    key = _get_key(task_id, encryption)
    return _crypt_data(data, key, encryption)
        
def decrypt_data(data, task_id):
    ''' Decrypts the data with the given task ID. Only use this if the
        encrypted data returned by all three workers is the same; foreman will
        not be able to get credits back once they call it. If the data does
        not check out and the foreman wants a refund, use invalidate_data().
        Throws ServerError, UnknownTask
    '''
    encryption = False
    key = _get_key(task_id, encryption)
    return _crypt_data(data, key, encryption)
    
def invalidate_data(task_id):
    ''' Get a refund for the given task ID. Once this is used, the data cannot
        be decrypted. Returns the number of credits the caller now has.
        Throws ServerError
    '''
    url = DISCOVERY_SERVICE_URL + "/tasks/" + task_id
    payload = {"valid": 0}
    response = requests.delete(url, data=payload)
    if response.status_code == requests.codes.ok:
        credits_dict = json.loads(response.content)
        return credits_dict["credits"]
    else:
        raise ServerError(response)
  
  
#######################################################
#################### Tests ############################
####################################################### 

# You can test this module by simplying running it. (Maybe not ideal.)
# DANGER: Testing this code seeds the production DB (also possibly not ideal)

if __name__ == '__main__':

    data = {"b": 1, "a": 2}
    
    # Test JSON list conversion
    json_list_data = _to_json_list(data)
    assert str(json_list_data) == '[["a", 2], ["b", 1]]            '
    assert _from_json_list(json_list_data) == data
    
    # Seed DB
    seed_response = requests.get(DISCOVERY_SERVICE_URL + "/seed")
    assert seed_response.status_code == requests.codes.ok
    tasks = json.loads(seed_response.content)
    
    # Test registration
    register_worker()
    
    # Test encryption
    task_id = tasks.keys()[0]
    encrypted_data = encrypt_data(data, task_id)
    key = _get_key(task_id, encryption=True)
    assert data == _crypt_data(encrypted_data, key, encryption=False)
    
    # Test getting tasks
    tasks = get_tasks(3)
    
    # Test invalidation
    task_id = tasks.keys()[0]
    assert invalidate_data(task_id) == 6
    
    # Test that you actually decrypt things
    task_id = tasks.keys()[1]
    key = _get_key(task_id, encryption=False)
    encrypted_data = _crypt_data(data, key, encryption=True)
    assert data == _crypt_data(encrypted_data, key, encryption=False)

    # Test decrypt_data by making sure it throws the right exception (?)
    task_id = tasks.keys()[2]
    try:
        decrypt_data(encrypted_data, task_id)
    except ValueError as err:
        # In principle, this just means that we (obviously) didn't encode
        # the data correctly, which should impossible...
        assert str(err) == "No JSON object could be decoded"
        
    