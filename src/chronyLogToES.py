import os
import click
import es_pandas
import pandas

@click.command()
@click.option("--path", help="Path to where the logfiles exist",default='/var/log/chrony',show_default=True)

def main(path):
    filenames = getLogFilenames(path)
    ep = es_pandas.es_pandas("https://its.project.li/es", http_auth=("xxx","yyy"),use_ssl=True, verify_certs=True)
    for i in filenames:
        filename = i.split("/")
        if filename[-1] == "tracking.log":
            df = parseChronyLog(i,tracking)
            sendToEs(ep,df,"chrony_tracking")
        elif filename[-1] == "statistics.log":
            df = parseChronyLog(i,statistics)
            sendToEs(ep,df,"chrony_statistics")
        elif filename[-1] == "measurements.log":
            df = parseChronyLog(i,measurements)
            sendToEs(ep,df,"chrony_measurements")
    #parseChronyLog("/var/log/chrony/measurements.log",measurements)

def getLogFilenames(path):
    """
    Takes a directory (path) and returns a list of all files within that directory that ends with .log 
    """
    listOfFiles = []
    for filename in os.listdir(path):
        if filename.endswith('.log'):
            listOfFiles.append(os.path.join(path, filename))
    return listOfFiles

def sendToEs(ep,data,indexname):
    doc_type = indexname 
    ep.init_es_tmpl(data,doc_type) #create an elastic template if it not already exist
    ep.to_es(data,indexname,show_progress=True,doc_type=doc_type)

def parseChronyLog(filename,headers):
    df = pandas.read_csv(filename, sep='\s+',names=[*headers],parse_dates=[["Date","Hour"]]) # sep='\s+' is a nice feature that uses multiple whitespaces as one delimiter
    # print(df.dtypes)
    # print(df.shape[0])
    df.rename(columns={"Date_Hour":"timestamp"}, inplace=True)
    df["timestamp"] = pandas.to_datetime(df["timestamp"],errors='coerce')
    df.dropna(subset=["timestamp"], inplace=True)
    # print(df.dtypes)
    # print(df.shape[0])

    return df

tracking = {"Date": "",
    "Hour": "",
    "IP_Address": "",
    "Stratum":"",
    "Freq_ppm":"",
    "Skew_ppm":"",
    "Offset":"",
    "Leap_status":"",
    "Combined_sources":"",
    "Combined_offset_StdDev":"",
    "Remaining_offset_correction": "",
    "Root_delay":"",
    "Root_dispersion":"",
    "Max_error_system_clock":""
    }

statistics = {"Date": "",
    "Hour": "",
    "IP_Address": "",
    "StdDev":"",
    "Estimated_offset":"",
    "Offset_StdDev":"",
    "Diff_freq":"",
    "Estimated_error_in_rate_value":"",
    "Stress":"",
    "Number_of_sources_used":"",
    "Bs":"",
    "Nr":"",
    "Asym":""}

measurements = {"Date": "",
    "Hour": "",
    "IP_Address": "",
    "leap_status": "",
    #"empty_0":"",
    "Remote_stratum": "",
    "123Test":"",
    "567Test":"",
    "ABCDTest":"",
    #"empty_1":"",
    #"empty_2":"",
    "local_poll":"",
    #"empty_3":"",
    "Remote_poll":"",
    "Score":"",
    "Offset":"",
    #"empty_4":"",
    "Peer_delay":"",
    "Peer_dispersion":"",
    #"empty_5":"",
    "Root_delay":"",
    "Root_dispersion":"",
    #"empty_6":"",
    "Refid":"",
    "NTP_mode":"",
    #"empty_7":"",
    "Source_of_Transmit":"",
    "Source_of_recieve":""
    }
if __name__ == '__main__':
    main() # pylint: disable=unexpected-keyword-arg
