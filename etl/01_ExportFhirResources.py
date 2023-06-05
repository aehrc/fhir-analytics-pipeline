# Databricks notebook source
"""
Bulk export specified FHIR resources from the provided FHIR server and save the data in the 
specified directory of a file/object store.

By default bulk export the 100 patients database from https://bulk-data.smarthealthit.org/.

:param FHIR_ENDPOINT_URL: the URL to the FHIR API endpoint with bulk export capabilities
:param EXPORT_RESOURCES: coma separated list of resource names to export
:param STAGING_DIR_URL: the URL to the directory to save the exported resource files
"""

dbutils.widgets.text('FHIR_ENDPOINT_URL', 'https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir')
dbutils.widgets.text('EXPORT_RESOURCES', 'Patient,Condition,Observation,Immunization')
dbutils.widgets.text('STAGING_DIR_URL', 'dbfs:/tmp/DevDays/demo-etl')

FHIR_ENDPOINT_URL = dbutils.widgets.get('FHIR_ENDPOINT_URL')
EXPORT_RESOURCES = dbutils.widgets.get('EXPORT_RESOURCES').split(',')
STAGING_DIR_URL = dbutils.widgets.get('STAGING_DIR_URL')

print(f"""Exporting: 
 resources: {EXPORT_RESOURCES}
 from: `{FHIR_ENDPOINT_URL}`
 to: `{STAGING_DIR_URL}`
""")

# COMMAND ----------

# 
# Run bulk export 
#

import requests
import time
import urllib
from os import path

def bulk_export(fhir_url, resources):
    """
    Performs the bulk export of the specified resources from the given FHIR server. 
    Initiates the bulk export and waits for its completion.

    :param fhir_url: the FHIR API endpoint URL
    :param resources: the list of named of the resources to export 
    :return: the final bulk export response
    """
    url = path.join(fhir_url, '$export')
    # Set the required headers for the FHIR bulk export API
    headers = {
        'Accept-Encoding': 'gzip',
        'Accept': 'application/fhir+json',
        'Content-Type': 'application/fhir+json',
        'Prefer': 'respond-async'
    }

    # Set the export parameters for a system-level export
    params = {
        '_outputFormat': 'ndjson',
        '_type': ",".join(resources),
    #    '_since': '2022-01-01T00:00:00Z'
    }

    # Make the HTTP request to the FHIR bulk export API
    response = requests.get(url, headers=headers, params=params)

    # Check the response status code
    if response.status_code == 202:
        # The request was accepted, so retrieve the Content-Location header to poll for the response
        content_location = response.headers['Content-Location']
        print(f'Export request accepted. Polling for response at {content_location}...\n')

        # Poll for the response until the export is complete
        while True:
            time.sleep(5)
            poll_response = requests.get(content_location)

            # Check the response status code
            if poll_response.status_code == 202:
                # The export is still in progress, so wait for a few seconds before polling again
                continue
            elif poll_response.status_code == 200:
                # The export is complete, so retrieve the JSON body containing the URLs to access the exported files
                return poll_response.json()
            else:
                # The request failed
                raise(Exception(f'Error in pool request: {poll_response.status_code}'))
    else:
        # The request failed
        raise Exception(f'Error in kick-off request: {response.status_code}')



export_response = bulk_export(FHIR_ENDPOINT_URL, EXPORT_RESOURCES)

# DEBUG: bulk export response JSON
print(f"""Server side export successful with response:
{export_response}""")


# COMMAND ----------

#
# Download exported files
#

from tempfile import TemporaryDirectory

def dowload_export(export_response, destination_url, clean = True):
    """
    Downloads the results for a bulk export operation to the specified directory.

    :param export_response: the response from a successful bulk export operation
    :param destination_url: the URL to the directory to save the exported files in
    :param clean: whether existing files in the specified directory should be removed  
    """

    if clean:
        dbutils.fs.rm(destination_url, True)

    output = export_response.get('output', [])
    index = 0
    with TemporaryDirectory() as tmpdirname:
        for entry in output:
            input_url = entry['url']
            input_type = entry['type']
            filename = path.basename(input_url)
            output_path = path.join(tmpdirname, filename)
            urllib.request.urlretrieve(input_url, output_path)
            temp_url= 'file://' + output_path
            output_url = path.join(destination_url, f'{input_type}.{index:04d}.ndjson')
            dbutils.fs.mv(temp_url, output_url)
            index = index + 1

dowload_export(export_response, STAGING_DIR_URL)

#DEBUG: List downloaded files and their contents
print("Downloaded files:")
files = dbutils.fs.ls(STAGING_DIR_URL)
for f in files: 
    print("`%s`: %s" % (f.path, dbutils.fs.head(f.path, 100)))

