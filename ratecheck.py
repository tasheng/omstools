# from oms.omstools.util.oms import get_hltlist_by_run
from util.oms import omsapi, get_hltlist_by_run
import util.utility as u
import argparse
import util.oms as oms

from datetime import datetime, timedelta
import pytz

import numpy as np
import pygsheets

def find_matching_strings(strings, substring):
    return [s for s in strings if substring in s]

def get_rate_by_runls_range(name, run, ls = None, category = "hlt"):
    if "hlt" in category:
        name_filter = "path_name"
        if not ls:
            q = omsapi.query("hltpathinfo")
        else:
            q = omsapi.query("hltpathrates")
    else:
        name_filter = "name"
        q = omsapi.query("l1algorithmtriggers")
    q.set_verbose(False)
    q.set_validation(False)
    q.filter(name_filter, name)\
     .filter("run_number", run)
    if not ls:
        if "hlt" not in category:
            q.custom("group[granularity]", "run")
    else:
        q.custom("group[granularity]", "lumisection")
        q.filter("first_lumisection_number",ls[0],"GE")\
         .filter("last_lumisection_number",ls[1],"LE")

    datas = []
    ipage = 1
    while True:
        u.progressbars()
        q.paginate(page = ipage, per_page = 100)
        qjson = q.data().json()
        data = qjson["data"]
        # print(data)
        datas.extend(data)
        if qjson["links"]["next"] is None:
            break;
        ipage = ipage+1
    u.progressbars_summary(ipage - 1)
    return datas

# fout = open("rate_monitoring.csv", "w")

data_taking = 'pp'
# data_taking = 'pbpb'

if data_taking == 'pp':
    hlt_paths = [
        "HLT_AK4CaloJet60",
        "HLT_AK4CaloJet70",
    ]
    l1_paths = [
        "L1_ZeroBias",
        "L1_SingleJet35",
        "L1_SingleJet60",
    ]
else:
    hlt_paths = [
        "HLT_HIL1Centrality30_50",
        "HLT_HIPuAK4CaloJet100Eta5p1"
    ]
    l1_paths = [
        "L1_ZeroBias",
        "L1_Centrality_30_50_BptxAND",
        "L1_SingleJet60_BptxAND"
    ]



if __name__ == "__main__":
    # Authorize pygsheets with the credentials file
    gc = pygsheets.authorize(outh_file='credentials.json')
    # Open the Google Sheet by its URL
    spreadsheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1nkBI4aMjDM7XmFUuymKpOZGBpl5hy9gv--ZvRq_-Dz4/edit")

    worksheet = spreadsheet.sheet1  # Select the first worksheet (sheet)

    # parse arg
    parser = argparse.ArgumentParser(description = 'Print HLT counts in given lumi ranges of runs')
    parser.add_argument('--pathnames', required = False, help = 'e.g. HLT_ZeroBias_v8,HLT_PPRefL1SingleMu7_v1')
    parser.add_argument('--timerange', help = '(option 3) <start_time>,<end_time>')
    parser.add_argument('--l1preps', required = False, help = 'Optional store L1 pre PS rate instead of post DT rate', action = "store_true")
    parser.add_argument('--count', required = False, help = 'Optional store count instead of rate', action = "store_true")
    args = parser.parse_args()

    # specify time range in cmd argument
    if args.timerange:
        print('\033[36mExtracting lumisections with \033[4mstable beams\033[0m\033[36m...\033[0m')
        timebs = args.timerange.split(",")
    # otherwise use 1 hr from now
    else:
        # Current time in UTC
        now = datetime.now(pytz.utc)

        # Get the time an hour earlier
        one_hour_earlier = now - timedelta(hours=1)

        # Format the times in the specified format
        timebs = (one_hour_earlier.strftime('%Y-%m-%dT%H:%M:%S'),
                  now.strftime('%Y-%m-%dT%H:%M:%S'))
    print("query time ranges", timebs)
    datas = oms.get_by_range(category = "lumisections",
                            var = "start_time", var2 = "end_time",
                            lmin = timebs[0], lmax = timebs[1],
                            per_page = 100)
    # print(datas)
    datas = oms.filter_data_list(datas, "beams_stable", True)
    runlumi = oms.get_json_by_lumi(datas)
    print("Summing up lumi sections: \033[4;32m", end = "")
    print(runlumi, end = "")
    print("\033[0m")

    rls_list = sorted(runlumi.items())
    # test time range
    # rls_list = [['387528', [[284, 286]]]]

    lumi_values = {}
    bunch_values = {}
    # Lumisection info
    for rls in rls_list:
        run = rls[0]
        q = omsapi.query("lumisections")
        q.paginate(per_page = 1000)
        q.set_verbose(False)
        # q.custom("group[granularity]", "lumisection")
        q.filter("run_number", rls[0])
        q.filter("first_lumisection_number",rls[1][0][0],"GE")
        q.filter("last_lumisection_number",rls[1][0][1],"LE")
        lumi_data = q.data().json()["data"]
        lumi_values[run] = np.average(
            [l["attributes"]["init_lumi"] for l in lumi_data] +
            [l["attributes"]["end_lumi"] for l in lumi_data]
        )
        fill = lumi_data[0]["attributes"]["fill_number"]
        q_fill = omsapi.query("fills")
        q_fill.filter("fill_number", fill)
        fill_data = q_fill.data().json()["data"]
        bunch_values[run] = fill_data[0]["attributes"]["bunches_colliding"]

    key_var = "rate"
    if args.count:
        key_var = "counter"
        print("count", end = "")
    else:
        print("rate", end = "")

    key_l1 = "post_dt_" + key_var
    if args.l1preps:
        key_l1 = "pre_dt_before_prescale_" + key_var
        print("Pre-DT before PS", end = "")
    else:
        print("Post-DT after PS", end = "")


    rate_results={};
    maxlen = 0

    # L1 and HLT rate
    for rls in rls_list:
        # assign hlt versions from the current run
        hltlist = get_hltlist_by_run(rls[0])
        for i, hltpath in enumerate(hlt_paths):
            hlt_paths[i] = find_matching_strings(hltlist, hltpath)[0]


    # query both pre-PS and post-PS rate
    extended_l1_paths = [st for path in l1_paths for st in (path, path + " before PS")]
    pathnames = hlt_paths + extended_l1_paths
    for p in pathnames:
        rate_results[p] = {}
        if len(p) > maxlen: maxlen = len(p)


    l1s = []
    hlts = []
    for rls in rls_list:
        run = rls[0]
        for path in pathnames:
            rate_results[path][run] = np.zeros(rls[1][0][1] - rls[1][0][0])

        for l1seed in l1_paths:
            query_lumis = get_rate_by_runls_range(l1seed, rls[0], rls[1][0], "l1")
            # print(query_lumis)

            key_l1 = "pre_dt_before_prescale_" + key_var
            rate_results[l1seed + " before PS"][run] = np.average([
                l["attributes"][key_l1] for l in query_lumis])
            key_l1 = "post_dt_" + key_var
            rate_results[l1seed][run] = np.average([
                l["attributes"][key_l1] for l in query_lumis])

        for hltpath in hlt_paths:
            print(hltpath)
            query_lumis = get_rate_by_runls_range(hltpath, rls[0], rls[1][0], "hlt")
            rate_results[hltpath][run] = np.average([
                rate["attributes"][key_var] for rate in query_lumis])

        print(rate_results)


        slope_35 = 32215.30085
        intercept_35 = 21.52018581
        slope_60 = 3706.827046
        intercept_60 = 2.478517322

        est_35 = lambda lum: slope_35 * lum + intercept_35
        est_60 = lambda lum: slope_60 * lum + intercept_60

        ## |time start|time end|run|lumi start| lumi end| bunch | ave lumi|
        row = ["time start","time end","run","lumi start"," lumi end", "bunch", "ave lumi"]
        row += pathnames
        row += ['est. jet35', 'est. jet60', 'ratio jet35', 'ratio jet60']
        worksheet.append_table(values=row)
        row = [timebs[0], timebs[1], run, rls[1][0][0], rls[1][0][1],
               bunch_values[run], lumi_values[run]]
        row += [f"{rate_results[path][run]:.3f}" for path in pathnames]
        row += [est_35(lumi_values[run]), est_60(lumi_values[run])]
        row += [ f"{rate_results['L1_SingleJet35 before PS'][run] / est_35(lumi_values[run]):.3f}" ]
        row += [ f"{rate_results['L1_SingleJet60 before PS'][run] / est_60(lumi_values[run]):.3f}" ]
        worksheet.append_table(values=row)
