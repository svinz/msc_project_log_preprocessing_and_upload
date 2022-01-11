import uuid
import json
import tqdm

def generate_lines(filename):
    """
    A simple generator that opens a file and return line by line
    :arg filename: Filename (and/or path) to file
    :yield: a line from the file
    """
    with open(filename) as f:
        for line in f:
            line = line.strip('\n')
            yield line
            
def drop_index_line(line):
    """ Drops the bulk index line from the tshark packet output
    :arg line: Line with extra characters
    :return: line or None if the line contains Elasticsearxh
    """
    decoded_line = line
    if decoded_line.startswith('{\"index\":') is True:
        return None
    else:
        return decoded_line

def remove_index_lines_and_fields_from_file(filename):
    """"
    Add comments...
    """
    temp_file = uuid.uuid4().hex + ".json"
    number_of_lines = sum(1 for _ in open(filename))
    print("Generating temporary file:")
    progressbar = tqdm.tqdm(unit="lines",total=number_of_lines)
    with open(filename) as f, open(temp_file,"w") as w:
        for line in f:
            line = drop_index_line(line)
            if line != None:
                line = remove_fields(line)
                w.writelines(line)
            progressbar.update(1)
    return temp_file

def remove_fields(line):
    """
    Add some comments
    """
    line = line.strip('\n')
    line = json.loads(line)
    if "eth" in line["layers"]:
        line["layers"].pop("eth")
    if "tls" in line["layers"]:
        line["layers"].pop("tls")
    if "ip" in line["layers"]:
        line["layers"].pop("ip")
    if "mqtt" in line["layers"]:
        if "mqtt_mqtt_msg" in line["layers"]["mqtt"]:
            line["layers"]["mqtt"]["mqtt_mqtt_msg"] = bytes.fromhex(line["layers"]["mqtt"]["mqtt_mqtt_msg"].replace(":","")).decode('utf-8')
    line = json.dumps(line) + "\n"
    return line
