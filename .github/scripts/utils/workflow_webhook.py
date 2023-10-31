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
    r = requests.post(self.webhook_url, data=payload, auth=BodyDigestSignature(secret))
    print ("Response: {}".format(r))
    # print(f"Status Code: {r.status_code}, Response: {r.json()}")



#
def run ():
    
    # get env parameters
    env_file = os.getenv('GITHUB_OUTPUT')
    
    wh_url = os.getenv("webhook_url")
    wh_secret = os.getenv("webhook_secret")
    custom_json_data = os.getenv("data")
    
    # debug only, to be removed
    print ("### Url: {}".format(wh_url))
    print ("### Secret: {}".format(wh_secret))
    print ("### Data: {}".format(custom_json_data))
    
    
    if isinstance(custom_json_data, dict):
        print ('### Data is a dictionary')

    if isinstance(custom_json_data, str):
        print ('### Data is a string')
    
    
    # debug only, to be removed
    
    
    if wh_url is None or wh_secret is None or custom_json_data is None:
      return False
    
    sender_obj = Sender (wh_url)
    sender_obj.send(custom_json_data, wh_secret)
    
    return True




if __name__ == "__main__":
    print ("### Testing WebHook tool script")

    passed = run()

    if passed : 
        print ("### >>>>>>>>>> SENT")
    else:
        print ('### >>>>>>>>>> INVALID')
