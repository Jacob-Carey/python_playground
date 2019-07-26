from requests import Request, Session
import json
import datetime
from urllib.parse import urlparse
import read_trackermap_json


def request_trackermap_scan(scan_region_id, scanurl, s):
    url = 'https://trackermapapi.evidon.com/api/TrackerMap/ScanUrl'
    data = {
        'ClientIdentifier': 'null',
        'CountryId': '1',
        'ProxyUrl': 'null',
        'ScanRegionId': scan_region_id,
        'TestDrive': 'false',
        'Url': scanurl,
        'UserId': ''
    }
    print('Requesting scan of %s' % scanurl)
    req = Request('POST', url, json=data)
    prepped = req.prepare()
    resp = s.send(prepped, timeout=180)
    response_content = resp.content
    json_formatted_response = json.loads(response_content.decode())
    return json_formatted_response


def retrieve_complete_trackermap_scan(scan_region_id, scan_id, s):
    url = 'https://trackermapapi.evidon.com/api/TrackerMap/GetNodeMap?'
    parameters = {
        'scanRegionId': scan_region_id,
        'scanId':     scan_id
    }
    req = Request('POST', url, params=parameters)
    prepped = req.prepare()
    resp = s.send(prepped, timeout=60)
    response_content = resp.content
    json_formatted_response = json.loads(response_content.decode())
    return json_formatted_response


def write_node_map_to_json_file(json_data, url):
    now = datetime.datetime.now()
    url_pieces = urlparse(url)
    trackermap_scan_file = './TrackerMap Data/' + \
        url_pieces.netloc + ' ' + now.strftime("%Y-%m-%d") + '.json'
    with open(trackermap_scan_file, 'w+') as open_file:
        trackermap_json_data = json.dumps(json_data, separators=(',', ':'))
        open_file.write(trackermap_json_data)
    print('File written to /TrackerMap Data as '
          + str(url_pieces.netloc + ' ' + now.strftime("%Y-%m-%d")))
    return trackermap_scan_file


def scan_handler(url):
    s = Session()
    scan_region_id = '11'
    data = request_trackermap_scan(scan_region_id, url, s)
    scan_id = data['UniqueScanId']
    trackermap_node_map = retrieve_complete_trackermap_scan(
        scan_region_id, scan_id, s)
    scan_file = write_node_map_to_json_file(trackermap_node_map, url)
    return scan_file


def execution_function(runtype, url_list=None):
    file_path_list = []
    if url_list:
        for url in url_list:
            file_path_list.append(scan_handler(url))
        return file_path_list
    else:
        url = input("What url would you like to scan? ")
        scan_file = scan_handler(url)
    if runtype == 0:
        read_trackermap_json.master_executer()
    elif runtype == 1:
        return scan_file


if __name__ == '__main__':
    execution_function(0)
