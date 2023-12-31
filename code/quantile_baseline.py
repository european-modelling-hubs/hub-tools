import pandas as pd 
import numpy as np 
import os 
from datetime import timedelta, datetime
import argparse

# list of ground truth data sources
data_sources = ["ERVISS", "FluID"]

parser = argparse.ArgumentParser()
parser.add_argument('--hub_path')
parser.add_argument('--target_name')
parser.add_argument('--filename')
parser.add_argument('--symmetrize', default=True)
parser.add_argument('--nsamples', default=10000)
parser.add_argument('--horizon', default=4)
parser.add_argument('--team_abbr', default="respicast")
parser.add_argument('--model_abbr', default="quantileBaseline")
parser.add_argument('--submission_end_weekday', default=2)

args = parser.parse_args()


def import_forecasting_weeks(path): 
    forecasting_weeks = pd.read_csv(f"./{path}/supporting-files/forecasting_weeks.csv")
    forecasting_weeks = forecasting_weeks.loc[forecasting_weeks.is_latest == True]
    return forecasting_weeks
    

def quantile_baseline(data : np.ndarray, 
                      nsamples : int, 
                      horizon : int, 
                      symmetrize : bool = True, 
                      include_training : bool = True) -> np.ndarray:
    
    """
    Compute baseline forecasts

    Parameters:
    - data (np.ndarray): training data 
    - nsamples (int): number of forecasting samples
    - horizon (int): forecasting horizon in steps 
    - symmetrize (bool): if True one-step differences are symmetrized. (Defaults to True).
    - include_training (bool): if True includes also training data in returned array. (Defaults to True).

    Returns:
    -  np.ndarray: forecast samples.
    """

    # compute one step changes 
    diffs = np.diff(data)

    if symmetrize  == True: 
        diffs = np.concatenate((diffs, -diffs))

    # resample forecasts
    if include_training == True:
        forecast_samples = np.zeros((nsamples, len(data) + horizon))
    else:
        forecast_samples = np.zeros((nsamples, horizon))

    for i in range(nsamples): 
        sampled_diffs = diffs[np.random.randint(0, len(diffs), size=horizon)]
        forecasts = np.cumsum(sampled_diffs)
        forecasts += data[-1]

        # fix negative values
        forecasts[forecasts<0] = 0 

        if include_training:
            forecast_samples[i] = np.concatenate((data, forecasts))
        else:
            forecast_samples[i] = forecasts

    return forecast_samples


def compute_quantiles(samples : np.ndarray, 
                      quantiles: np.ndarray = np.arange(0.01, 1.0, 0.01)) -> pd.DataFrame:
    """
    Compute quantiles and aggregated measures from the given samples.

    Parameters:
    - samples (np.ndarray): Array of samples.
    - quantiles (np.ndarray): Array of quantiles to compute. Default is np.arange(0.01, 1.0, 0.01).

    Returns:
    - pd.DataFrame: DataFrame containing the computed quantiles and aggregated measures.
    """

    df_samples = pd.DataFrame() 
    for q in quantiles:
        df_samples[str(np.round(q, 2))] = np.quantile(samples, axis=0, q=np.round(q, 2))
    
    # additional quantiles and aggregated measures
    df_samples["0.025"] = np.quantile(samples, axis=0, q=0.025)
    df_samples["0.975"] = np.quantile(samples, axis=0, q=0.975)
    df_samples["min"] = np.min(samples, axis=0)
    df_samples["max"] = np.max(samples, axis=0)

    return df_samples


def format_data(df_quantile, 
                location, 
                target,
                last_date,
                origin_date, 
                quantiles=[0.010, 0.025, 0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.350, 0.400, 0.450, 0.500, 
                           0.550, 0.600, 0.650, 0.700, 0.750, 0.800, 0.850, 0.900, 0.950, 0.975, 0.990]):
    
    data_formatted = dict(origin_date=[],
                        target=[],
                        horizon=[],
                        target_end_date=[],
                        location=[],
                        output_type=[],
                        output_type_id=[],
                        value=[])

    for index, row in df_quantile.iterrows(): 
        # add quantiles
        for quantile in quantiles: 
            data_formatted["origin_date"].append(origin_date)
            data_formatted["target"].append(target)
            data_formatted["horizon"].append(index + 1)
            data_formatted["target_end_date"].append(pd.to_datetime(last_date) + timedelta(days=7 * int(index + 1)))
            data_formatted["location"].append(location)
            data_formatted["output_type"].append("quantile")
            data_formatted["output_type_id"].append("{:.3f}".format(quantile))
            data_formatted["value"].append(row[str(quantile)])

        # add median 
        data_formatted["origin_date"].append(origin_date)
        data_formatted["target"].append(target)
        data_formatted["horizon"].append(index + 1)
        data_formatted["target_end_date"].append(pd.to_datetime(last_date) + timedelta(days=7 * int(index + 1)))
        data_formatted["location"].append(location)
        data_formatted["output_type"].append("median")
        data_formatted["output_type_id"].append("")
        data_formatted["value"].append(row[str(0.5)])

    df_quantile_formatted = pd.DataFrame(data=data_formatted)
    return df_quantile_formatted


def generate_baseline_forecast_fullpipeline(truth_data, 
                                            forecasting_weeks,
                                            target_name="ILI incidence", 
                                            nsamples=10000,
                                            horizon=4,
                                            symmetrize=True): 
    
    # get last truth date
    last_date = datetime.strptime(forecasting_weeks.target_end_date.min(), "%Y-%m-%d") - timedelta(days=7)
    
    # import forecasting weeks 
    origin_date = forecasting_weeks.origin_date.values[0]

    quantile_baseline_forecasts = pd.DataFrame()

    for location in truth_data.location.unique(): 
        truth_data_loc = truth_data.loc[truth_data.location == location]
        # sort data
        truth_data_loc = truth_data_loc.sort_values(by="truth_date", ascending=True, ignore_index=True)

        # compute extra steps if truth data is missing
        extra_horizon = int((last_date - datetime.strptime(truth_data_loc.truth_date.max(), "%Y-%m-%d")).days / 7)
        
        # generate baseline forecast samples
        samples = quantile_baseline(data=truth_data_loc.value.values, nsamples=nsamples, horizon=horizon + extra_horizon, symmetrize=symmetrize, include_training=False)

        # remove extra horizons data
        samples = samples[:, -horizon:]
        
        # generate quantiles
        df_quantile = compute_quantiles(samples)

        # format data 
        df_quantile_formatted = format_data(df_quantile, location=location, target=target_name, last_date=last_date, origin_date=origin_date)
        
        quantile_baseline_forecasts = pd.concat((quantile_baseline_forecasts, df_quantile_formatted))
        
    return quantile_baseline_forecasts, origin_date


# import forecasting weeks
forecasting_weeks = import_forecasting_weeks(args.hub_path)

# import target data from all sources
target_data = pd.DataFrame()
for source in data_sources:
    if os.path.exists(os.path.join(args.hub_path, f"target-data/{source}/latest-{args.filename}.csv")):
        target_source = pd.read_csv(os.path.join(args.hub_path, f"target-data/{source}/latest-{args.filename}.csv"))
        target_data = pd.concat((target_data, target_source), ignore_index=True)

# cut historical data
target_data = target_data.loc[target_data.year_week >= "2023-W42"].reset_index(drop=True)

quantile_baseline_forecasts, origin_date = generate_baseline_forecast_fullpipeline(target_data, 
                                                                      target_name=str(args.target_name), 
                                                                      nsamples=int(args.nsamples),
                                                                      horizon=int(args.horizon),
                                                                      symmetrize=bool(args.symmetrize), 
                                                                      forecasting_weeks=forecasting_weeks)

model_id = f"{str(args.team_abbr)}-{str(args.model_abbr)}"
file_name = f"{origin_date}-{model_id}.csv"
quantile_baseline_forecasts.to_csv(os.path.join(args.hub_path, f"model-output/{model_id}/{file_name}"), index=False)

env_file = os.getenv('GITHUB_OUTPUT')
with open(env_file, "a") as outenv:
   outenv.write (f"baseline_file=model-output/{model_id}/{file_name}")
