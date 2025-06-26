import os
import argparse 

from FH_utils import get_latest_origin_dates

if __name__ == "__main__":
    print ('FH_current_baseline_exists')

    out_res = "missing"

    # Arguments 
    parser = argparse.ArgumentParser()
    parser.add_argument('--hub_path', default="./repo")
    
    args = parser.parse_args()

    hub_path = str(args.hub_path)
    
    filename = f"{get_latest_origin_dates(hub_path)}-respicast-quantileBaseline.csv"
    
    filepath = os.path.join(hub_path, 'model-output/respicast-quantileBaseline', filename)

    
    if os.path.exists(filepath): 
        out_res = "exists"


    # write results to output
    env_file = os.getenv('GITHUB_OUTPUT')        

    with open(env_file, "a") as outenv:
        print (f"Writing results to output: {out_res}")
        outenv.write (f"bl_exists={out_res}\n")
