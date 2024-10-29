from util.oms import omsapi
import util.utility as u
import argparse
import util.oms as oms
from datetime import datetime, timedelta


# fout = open("rate_monitoring.csv", "w")

data_taking = 'pp'
data_taking = 'pbpb'

if data_taking == 'pp':
    hlt_paths = [
        "HLT_AK4CaloJet60_v5",
        "HLT_AK4CaloJet70_v5",
    ]
    l1_paths = [
        "L1_ZeroBias",
        "L1_SingleJet35",
        "L1_SingleJet60",
    ]
else:
    hlt_paths = [
        "HLT_HIL1Centrality30_50_v6",
        "HLT_HIPuAK4CaloJet100Eta5p1_v13"
    ]
    l1_paths = [
        "L1_ZeroBias",
        "L1_Centrality_30_50_BptxAND",
        "L1_SingleJet60_BptxAND"
    ]

def get_rate_by_runls_range(run, ls = None, category = "hlt"):
    if "hlt" in category:
        if not ls:
            q = omsapi.query("hltpathinfo")
        else:
            q = omsapi.query("hltpathrates")
    else:
        q = omsapi.query("l1algorithmtriggers")
        q.set_verbose(False)
        q.set_validation(False)
        q.filter("run_number", run)
    if not ls:
        if "hlt" not in category:
            q.custom("group[granularity]", "run")
    else:
        q.filter("first_lumisection_number",ls[0],"GE")\
         .filter("last_lumisection_number",ls[1],"LE")

    datas = []
    ipage = 1
    while True:
        u.progressbars()
        q.paginate(page = ipage, per_page = 100)
        qjson = q.data().json()
        data = qjson["data"]
        datas.extend(data)
        if qjson["links"]["next"] is None:
            break;
        ipage = ipage+1
        u.progressbars_summary(ipage - 1)
    return datas


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Print HLT counts in given lumi ranges of runs')
    parser.add_argument('--pathnames', required = False, help = 'e.g. HLT_ZeroBias_v8,HLT_PPRefL1SingleMu7_v1')
    parser.add_argument('--outcsv', required = False, help = 'Optional csv output file')
    parser.add_argument('--timerange', help = '(option 3) <start_time>,<end_time>')
    args = parser.parse_args()

    # specify time range in cmd argument
    if args.timerange:
        print('\033[36mExtracting lumisections with \033[4mstable beams\033[0m\033[36m...\033[0m')
        timebs = args.timerange.split(",")
    # otherwise use 1 hr from now
    else:
        # Get the current time
        now = datetime.now()

        # Get the time an hour earlier
        one_hour_earlier = now - timedelta(hours=1)

        # Format the times in the specified format
        timebs = (one_hour_earlier.strftime('%Y-%m-%dT%H:%M'),
                  now.strftime('%Y-%m-%dT%H:%M'))
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
for rls in rls_list:
    print(rls)
    print('l1')
    print(rls, type(rls))
    l1s = get_rate_by_runls_range(rls[0], rls[1][0], "l1")
    print('hlt')
    hlts = get_rate_by_runls_range(rls[0], rls[1][0], "hlt")
exit(1)

counts = {}
maxlen = 0
with open(outputfile, 'w') as f:
    print("HLT Path, Counts", file = f)
    for p in pathnames:
        totalcount = getcount(runlumi, p)
        print(p + ", " + f'{totalcount}', file = f)
        counts[p] = totalcount
        if len(p) > maxlen: maxlen = len(p)

nl = 21 + maxlen
print('-' * nl)
print('| {:<{width}} |{:>15} |'.format("HLT Path", "Count", width = maxlen))
print('-' * nl)
for p in counts:
    print('| {:<{width}} |{:>15} |'.format(p, counts[p], width = maxlen))
print('-' * nl)
print()
exit(1)

ls_starts = [45,80,108] # 0 if we want to start at stable beam.
ls_num_to_avgs = [20,20,20] # How many lumisections to be averaged.

runnums = [375252,375256,375259] # Run numbers to be used


path_txt = "Run,"

for path in l1_paths:
	path_txt += path
	path_txt += ","

for path in hlt_paths:
	path_txt += path
	path_txt += ","

fout.write(path_txt + '\n')

q_lumi = omsapi.query("lumisections")
q_lumi.paginate(per_page = 3000)
q_lumi.set_verbose(False)

q_l1 = omsapi.query("l1algorithmtriggers")
q_l1.paginate(per_page = 3000)
q_l1.set_verbose(False)

q_hlt = omsapi.query("hltpathrates")
q_hlt.paginate(per_page = 3000)
q_hlt.set_verbose(False)

r_id = 0

for runnum in runnums:
	print('run:',runnum)

	q_lumi.clear_filter()
	q_lumi.filter("run_number", runnum).filter("beams_stable", 'true')
	ls_stable_start = q_lumi.data().json()["data"][0]['attributes']['lumisection_number']
	#ls_start=1
	#https://cmsoms.cern.ch/agg/api/v1/runs/373710/lumisections?filter[beams_stable]=true

	ls_start = ls_starts[r_id]
	ls_num_to_avg = ls_num_to_avgs[r_id]

	print('Stable beam start at lumisection',ls_stable_start)
	if(ls_start==0):
		ls_start = ls_stable_start

	print('We start at lumisection',ls_start)
	print('Number of lumisections used to average:',ls_num_to_avg)

	fout.write(str(runnum) + " (LS " + str(ls_start) + "-"  + str(ls_start+ls_num_to_avg-1) + "),")

	for l1_path in l1_paths:
		q_l1.clear_filter()
		q_l1.filter("run_number", runnum).filter("name", l1_path).filter("first_lumisection_number",ls_start,"GE").filter("last_lumisection_number",ls_start+ls_num_to_avg,"LE")

		#https://cmsoms.cern.ch/agg/api/v1/l1algorithmtriggers?filter[run_number][EQ]=373710&&filter[name][EQ]=L1_ZeroBias&&filter[first_lumisection_number][GE]=7&&filter[last_lumisection_number][LE]=27

		data=q_l1.data().json()["data"]
		rate = 0
		for i in range(ls_num_to_avg):
			if "MinimumBias" in l1_path:
				rate += data[i]['attributes']['pre_dt_before_prescale_rate']
			else:
				rate += data[i]['attributes']['post_dt_rate']

		rate/=ls_num_to_avg
		print(l1_path,"rate:",rate)

		fout.write(str(rate) + ',')

	for hlt_path in hlt_paths:
		q_hlt.clear_filter()
		q_hlt.filter("run_number", runnum).filter("path_name", hlt_path).filter("first_lumisection_number",ls_start,"GE").filter("last_lumisection_number",ls_start+ls_num_to_avg,"LE")

		#https://cmsoms.cern.ch/agg/api/v1/hltpathrates?filter[run_number][EQ]=373345&&filter[path_name][EQ]=HLT_PPRefGEDPhoton10_v1&&filter[first_lumisection_number][GE]=7&&filter[last_lumisection_number][LE]=27

		data=q_hlt.data().json()["data"]
		rate = 0
		for i in range(ls_num_to_avg):
			rate += data[i]['attributes']['rate']

		rate/=ls_num_to_avg
		print(hlt_path,"rate:",rate)

		fout.write(str(rate)+ ',')

	fout.write('\n')
	r_id += 1

print('Done')
