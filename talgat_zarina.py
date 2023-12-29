import pandas as pd
import requests
import gzip
from io import BytesIO


def convert_to_wet_url(warc_url):
    wet_url = warc_url.replace('/warc/', '/wet/').replace('.warc.gz', '.warc.wet.gz')
    return wet_url


def download_and_decompress_wet(wet_url):
    wet_url='https://data.commoncrawl.org/' + wet_url
    response = requests.get(wet_url)

    if response.status_code == 200:
        with gzip.GzipFile(fileobj=BytesIO(response.content), mode='rb') as f:
            wet_content = f.read()
        return wet_content
    else:
        print(f"Failed to download {wet_url}")
        return None


def find_text_before_warc(content, url):
    url_index = content.find(url)

    if url_index != -1:
        tmp_txt = content[url_index:]
        content_index = tmp_txt.find('Content-Length:')
        tmp_txt = tmp_txt[content_index:]
        text = tmp_txt
        for i in range(len(text)):
            if text[i] == '\n':
                tmp_txt = text[i + 1:]
                break
        warc_index = tmp_txt.find('WARC/1.0', 0, url_index)

        if warc_index != -1:
            text_before_warc = tmp_txt[:warc_index]
            return text_before_warc
        else:
            print("WARC/1.0 not found before the URL.")
    else:
        print(f"URL {url} not found in the WET content")
    return None


# Read CSV file
file_path = '/Users/admin/Downloads/first_one.csv'
df = pd.read_csv(file_path)

url_list = df['url'].tolist()
file_names = df['warc_filename'].tolist()

# List to store the text_before_warc
context_list = []

# Iterate over the URL list and file names
for warc_url, url_to_search in zip(file_names,url_list):
    wet_url = convert_to_wet_url(warc_url)
    wet_content = download_and_decompress_wet(wet_url)

    if wet_content:
        text_before_warc = find_text_before_warc(wet_content.decode('utf-8'), url_to_search)
        context_list.append(text_before_warc)

# Create a new DataFrame
result_df = pd.DataFrame({'Context': context_list})

# Save the DataFrame to a new CSV file
result_df.to_csv('/Users/admin/Downloads/context_results.csv', index=False)
