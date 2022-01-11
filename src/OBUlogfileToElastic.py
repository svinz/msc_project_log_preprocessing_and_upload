from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import click
import tqdm
import json

def generate_lines(filename):
    """
    A simple generator that opens a file and return line by line
    """
    with open(filename) as f:
        for line in f:
            line = line.strip('\n')
            message = json.loads(line)
            if "cam" in message:
                message["cam"]["camParameters"]["highFrequencyContainer"].pop(0)
                message["cam"]["camParameters"]["highFrequencyContainer"] = message["cam"]["camParameters"]["highFrequencyContainer"][0]
                line = json.dumps(message)
            yield line
@click.command()
@click.option("-logfile", help="logfile to upload to Elasticsearch", required=True, type=click.Path(exists=True))

def placeLogToEs(logfile):
    filename = logfile
    
    filename = filename.split(sep="logs/")
    newIndex = filename[1].split(sep=".")
    newIndex = "nwixn" + newIndex[0]
    print(newIndex)
    number_of_lines = sum(1 for _ in open(logfile))
    progressbar = tqdm.tqdm(unit="docs",total=number_of_lines)
    es = Elasticsearch("https://its.project.li/es", http_auth=("xxx","yyy"),use_ssl=True,
    verify_certs=True)
    es.indices.create(index=newIndex, ignore=400) # pylint: disable=unexpected-keyword-arg
    success = 0
    for ok, action in streaming_bulk(es,index=newIndex, actions=generate_lines(logfile),max_retries=10):
        progressbar.update(1)
        success += ok
    
    print("Uploaded: " + success.__str__())
# for hit in res['hits']['hits']:
#         print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
if __name__ == '__main__':
    placeLogToEs()