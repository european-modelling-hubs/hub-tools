import pandas as pd 
import numpy as np 
import os 
from datetime import timedelta
import argparse
from datetime import date

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

def get_next_day(test_date, weekday_idx): 
    return test_date + timedelta(days=(weekday_idx - test_date.weekday() + 7) % 7)
 

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
                                            origin_date,
                                            target_name="ILI incidence", 
                                            nsamples=10000,
                                            horizon=4,
                                            symmetrize=True): 
    
    # get last date
    last_date = truth_data.truth_date.max()
    quantile_baseline_forecasts = pd.DataFrame()

    for location in truth_data.location.unique(): 
        truth_data_loc = truth_data.loc[truth_data.location == location]
        
        # generate baseline forecast samples
        samples = quantile_baseline(data=truth_data_loc.value.values, nsamples=nsamples, horizon=horizon, symmetrize=symmetrize, include_training=False)

        # generate quantiles
        df_quantile = compute_quantiles(samples)

        # format data 
        df_quantile_formatted = format_data(df_quantile, location=location, target=target_name, last_date=last_date, origin_date=origin_date)
        
        quantile_baseline_forecasts = pd.concat((quantile_baseline_forecasts, df_quantile_formatted))
        
    return quantile_baseline_forecasts

# import target data 
origin_date = get_next_day(date.today(), int(args.submission_end_weekday))
target_data = pd.read_csv(os.path.join(args.hub_path, f"target-data/ERVISS/{args.filename}_latest.csv"))
quantile_baseline_forecasts = generate_baseline_forecast_fullpipeline(target_data, 
                                                                      target_name=str(args.target_name), 
                                                                      nsamples=int(args.nsamples),
                                                                      horizon=int(args.horizon),
                                                                      symmetrize=bool(args.symmetrize), 
                                                                      origin_date=origin_date)

model_id = f"{str(args.team_abbr)}-{str(args.model_abbr)}"
file_name = f"{origin_date.strftime('%Y-%m-%d')}-{model_id}.csv"
quantile_baseline_forecasts.to_csv(os.path.join(args.hub_path, f"model-output/{model_id}/{file_name}"), index=False)