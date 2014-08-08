'''Given video id, output the FLV URLs

A video ID should look something like a12345.
'''
import requests
import re
import gzip
import json
import sys


CHUNK_URL = 'http://api.twitch.tv/api/videos/{0}?as3=t'


def main():
    video_id = sys.argv[1]
    output_filename = sys.argv[2]

    urls = list(fetch(video_id))

    gzip_file = gzip.open(output_filename, 'w')

    json.dump({
        'id': video_id,
        'urls': urls
    }, gzip_file, indent=2)

    gzip_file.close()


def fetch(video_id):
    video_id_num = re.search(r'([\d]+)', video_id).group(1)

    doc = None

    for video_type in ['a', 'b', 'c']:
        url = CHUNK_URL.format(video_type + video_id_num)
        print('Get', url)
        response = requests.get(url)

        print(response.status_code)

        if response.status_code == 200:
            doc = response.json()
            break

    if not doc:
        raise Exception('No results!')

    for chunk in doc['chunks']['live']:
        yield chunk['url']


if __name__ == '__main__':
    main()
