import os
import re
import json
import sys
import requests
import hmac
import hashlib



class BodyDigestSignature(object):
    def __init__(self, secret, header='x-hub-signature-256', algorithm=hashlib.sha256):
        self.secret = secret
        self.header = header
        self.algorithm = algorithm
        print("Calling constructor - Secret: {}".format(secret))

    def __call__(self, request):
        print("Calling call")
        body = request.body

        print ("Body: {}".format(body))

        if not isinstance(body, bytes):  
            body = body.encode('utf-8')  

        signature = hmac.new(self.secret.encode('utf-8'), body, digestmod=self.algorithm)
        hex_sig = signature.hexdigest()
        print (f"GEN SIG {hex_sig}")
        request.headers[self.header] = hex_sig
        #request.headers[self.header] = signature.hexdigest()
        return request



class Sender () :
  def __init__(self, webhook_url):
    
    self.webhook_url = webhook_url


  def send (self, payload, secret):
    # send POST request 
    r = requests.post(self.webhook_url, data=payload, auth=BodyDigestSignature(secret))

    return r



def handleResponseOk(j_response):
    print ("Handling successful response")
    # JsonResponse({'status': 'success', 'message': 'Import completed'}, status=http.client.OK)

    for key in j_response:
      print (f"Key {key} -> data: {j_response[key]}")

    res = {}
    res["status"] = 'success'
    res["message"] = j_response["message"]
    res["failed_ingestions"] = "NA"

    return res


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



def handleResponse (response):

    http_code = response.status_code
    j_response = response.json()

    print (f"Response http code: {http_code}")

    if http_code == 200:
       handleResponseOk(j_response)
    else:
       handleResponseError(http_code, j_response)


   
   


#
def run ():
    
    # get env parameters
    env_file = os.getenv('GITHUB_OUTPUT')    
    json_data = os.getenv("data")
    data_type = os.getenv("data_type")
    disease_name = os.getenv("disease_name")
    wh_url = os.getenv("webhook_url") + ("forecast/" if data_type == 'forecast' else "model-metadata/" )
    wh_secret = os.getenv("webhook_secret")
        
    
    # debug only, to be removed
    jdata = json.loads(json_data)
        
    jpayload = {}
    jpayload["disease"] = disease_name
    
    if data_type == 'forecast':
        jpayload["forecasts"] = jdata
    else:
        jpayload["changes"] = jdata["changes"]

    print (f"### sending: \n{jpayload}\n")
    
    
    if wh_url is None or wh_secret is None or json_data is None:
      return False
    
    sender_obj = Sender (wh_url)
    response = sender_obj.send(json.dumps(jpayload), wh_secret)

    run_results = handleResponse(response)


    with open(env_file, "a") as outenv:
        print(f"Writing to out: validate: {run_results}")
        outenv.write (f"run_results={json.dumps(run_results)}")


if __name__ == "__main__":
    print ("### Testing WebHook tool script")
    run()

