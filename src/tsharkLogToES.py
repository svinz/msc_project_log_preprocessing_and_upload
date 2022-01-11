from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticHelpers import generate_lines, remove_index_lines_and_fields_from_file
import click
import tqdm
import yaml
import sys
import os

@click.command()
@click.option("-logfile", help="logfile to upload to Elasticsearch", required=True, type=click.Path(exists=True))
@click.option("-config", help="Configfile containing necessary parameters",default='src/config.yaml',show_default=True,  required=True, type=click.Path(exists=True))
def placeLogToEs(logfile,config):
    try:
        with open(config,'r') as f:
            cfg = yaml.safe_load(f) #Open file and load content to cfg
    except yaml.scanner.ScannerError as e:
        print("Error reading file: {}".format(config))
        sys.exit(1)    

    filename = logfile
    
    filename = filename.split(sep="/")
    filename = filename[-1].split(sep=".")
    newIndex = filename[0].split(sep="_")
    newIndex = "packets-" + newIndex[-1]
    print(newIndex)
    logfile = remove_index_lines_and_fields_from_file(logfile)

    es = Elasticsearch(cfg["ES_URL"], http_auth=(cfg["ES_user"],cfg["ES_password"]),use_ssl=True,
    verify_certs=True)
    es.indices.create(index=newIndex, ignore=400) # pylint: disable=unexpected-keyword-arg
    success = 0
    number_of_lines = sum(1 for _ in open(logfile))
    print("Upload docs to elastic: ")
    progressbar = tqdm.tqdm(unit="docs",total=number_of_lines)
    for ok, action in streaming_bulk(es,index=newIndex, actions=generate_lines(logfile),max_retries=10,chunk_size=100):
        progressbar.update(1)
        success += ok
    
    os.remove(logfile)

if __name__ == '__main__':
    placeLogToEs()