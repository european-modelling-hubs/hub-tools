import os
import json
import shutil
import argparse 
import requests
import hmac
import hashlib


#
#
class BodyDigestSignature(object):
    def __init__(self, secret, header='x-hub-signature-256', algorithm=hashlib.sha256):
        self.secret = secret
        self.header = header
        self.algorithm = algorithm
        
    def __call__(self, request):
        print("Calling call")
        body = request.body

        print ("Body: {}".format(body))

        if not isinstance(body, bytes):  
            body = body.encode('utf-8')  

        signature = hmac.new(self.secret.encode('utf-8'), body, digestmod=self.algorithm)
        hex_sig = signature.hexdigest()
        request.headers[self.header] = hex_sig
        return request


#
#
class Sender () :
  def __init__(self, webhook_url):
    
    self.webhook_url = webhook_url
    print (f'sending data to {self.webhook_url}')


  def send (self, payload, secret):
    # send POST request 
    r = requests.post(self.webhook_url, data=payload, auth=BodyDigestSignature(secret))

    return r


#
#
def handleResponseOK():
    # everything ok
    res = {}
    res["status"] = 'success'
    res["message"] = 'Import completed'
    res["failed_ingestions"] = "NA"

    return res


#
#
def handleServerError():
    print('Handle server error')
    
    res = {}
    res["status"] = "error"
    res["message"] = "Server Error"
    res["failed_ingestions"] = "NA"
    
    return res


#
#
def handleResponseError(http_code, j_response):
    print ("Handling error response")

    res = {}
    res["status"] = j_response["status"]
    res["message"] = j_response["message"]
    
    if http_code == 400:    
        # Bad request
        #'status': 'error', 'message': 'Invalid input JSON'
        print ("Bad request")
        res["failed_ingestions"] = "NA"


    elif http_code == 401:
        # Unauthorized
        #'status': 'error', 'message': 'Signature authentication failed'
        print("Unauthorised")
        res["failed_ingestions"] = "NA"
       


    elif http_code == 500:

        print("Internal Server Error")
        res["failed_ingestions"] = "NA" if j_response["status"] == "error" else j_response["failed_ingestions"]

    else:
       print("Unknown Error")
       res["status"] = "error"
       res["message"] = "Unknown Error occured"
       res["failed_ingestions"] = "NA"


    for key in j_response:
        print (f"Key {key} -> data: {j_response[key]}")

    return res


#
# 
def read_json(json_path):

    """Reads the JSON file and extracts the list of CSV file paths."""
    with open(json_path, 'r') as file:
        data = json.load(file)
    return data.get("changes", [])


def response_gen ():
    response = requests.get('https://api.github.com')
    return response
    

#
#
def post_files(changes, whurl, whsecret, disease_name):
    
    """Posts file names to the server and returns success status."""

    jpayload = {}
    jpayload["disease"] = disease_name

    # order input 
    changes.sort()

    for change in changes:
      
        jpayload["changes"] = [change]
        sender_obj = Sender (whurl)
        response = sender_obj.send(json.dumps(jpayload), whsecret)
        # response = response_gen()

        if not response.headers["content-type"].strip().startswith("application/json"):
            return handleServerError()

        if response.status_code != 200:
            print(f"Failed to post {change}, Status Code: {response.status_code}")
            return handleResponseError(response.status_code, response.json())
       
    return handleResponseOK()


#
#
def clear_temporary (changes, hub_path, tmp_dir):
    
    # Delete temp files if all posts were successful
    for file_path in changes:
        os.remove(os.path.join(hub_path, file_path))

    # Remove temporary jdb
    os.remove(os.path.join(hub_path, tmp_dir, "changes.json"))

    # Remove temporary folder
    os.rmdir(os.path.join(hub_path, tmp_dir))
    print("All files processed and deleted successfully.")



#
#-----------------------------------
def main (hub_path, tmp_dir, whurl, whsecret, disease_name):
    # code here
    print ("Upload and clear")

    # get changes to upload
    json_path = os.path.join(hub_path, tmp_dir, "changes.json")
    changes = read_json(json_path=json_path)
    
    print (f'Changes from {changes}')

    run_results = post_files(changes, whurl, whsecret, disease_name)

    print (f'Post results: {run_results}')
        
    clear_temporary(changes=changes, hub_path=hub_path, tmp_dir=tmp_dir)

    return run_results


if __name__ == "__main__":
  
    # Arguments 
    parser = argparse.ArgumentParser()
    parser.add_argument('--hub_path', default="./repo")
    parser.add_argument('--tmp_folder', default="./github/tmp")

    args = parser.parse_args()

    hub_path = str(args.hub_path)
    tmp_folder = str(args.tmp_folder)

    # Env parameters
    wh_url = os.getenv("webhook_url")
    env_file = os.getenv('GITHUB_OUTPUT')    
    wh_secret = os.getenv("webhook_secret")
    disease_name = os.getenv("disease_name")

    run_results = {}

    # verify  configuration
    if wh_url is None or wh_secret is None:
        print(f"invalid request. Skip")            
        run_results["status"] = "invalid"

    else:     
        run_results = main(hub_path = hub_path, tmp_dir = tmp_folder, whurl  = wh_url + "evaluation/", whsecret = wh_secret, disease_name = disease_name)

    with open(env_file, "a") as outenv:
        outenv.write (f"run_results={json.dumps(run_results)}")

    print(f'Upload and clear completed!')
