"""This code is retrieving the latest report in the window specified.

DBM users typically create scheduled reports, where the advertiser user either
the UI or API to create a query that will be used to generate reports based on
a daily, weekly, or monthly schedule.

In this module, we check to see if a daily query has run in the last 24 hours.
If it has, we download the data to a local CSV file. If not we run the query
then download it.

If no queryId is provided, this will print a list of existing queries.

Id  Name
1467803708563	adzai_lld_daily
1467803955654	adzai_browser
1467804062778	adzai_exchange
1467804147912	adzai_inventories
1467804281246	adzai_time
1467804380518	adzai_creative-device
1467804556342	adzai_geography
1467804792294	adzai_audience
1467852346443	adzai_creative_extended
1467852656135	adzai_page_categories
1467866936785	adzai_creative_features

In first run if the oAuth2 credentials are not saved
"""
import os
import argparse
import shutil
import timeit
from dbmutil import reportDownloadProgress, get_service
from datetime import datetime
from datetime import timedelta
from urllib.request import urlopen, urlretrieve
from urllib.error import ContentTooShortError
from time import sleep, strftime

# Optional filtering arguments.
parser = argparse.ArgumentParser(description='Downloads a report if it has '
                                 'been created in the given timeframe.')
parser.add_argument('--client_id', required=False,
                    help=('Your client ID from the Google Developers Console.'
                          'This should be provided along with the '
                          'client_secret the first time you run an example.'))
parser.add_argument('--client_secret', required=False,
                    help=('Your client secret from the Google Developers '
                          'Console. This should be provided along with the '
                          'client_id the first time you run an example.'))
parser.add_argument('--output_directory', default=(os.path.dirname(
    os.path.realpath(__file__))), help=('Path to the directory you want to '
                                        'save the report to.'))
parser.add_argument('--query_id', default=0, type=int,
                    help=('The id of a query used to generate a report.'))
parser.add_argument('--report_window', default=24, type=int,
                    help=('The age a report must be in hours at a maximum to '
                          'be considered fresh.'))
parser.add_argument('--report_daterange', default='LAST_7_DAYS',
                    help=('If the report is not fresh execute the report using '
                          'date range provided. Check the valid values in '
                          'https://developers.google.com/bid-manager/v1/queries/runquery'))
parser.add_argument('--download_method', default='URLRETRIEVE',
                    help=('You can change the method of download. '
                          'Legal values: URLRETRIEVE, COPYFILEOBJ, READCHUNK'))

def main(doubleclick_bid_manager, output_dir, query_id, report_window, report_daterange, download_method):
  if query_id:
    # Call the API, getting the latest status for the passed queryId.
    response = dbm_getquery_safe(doubleclick_bid_manager,query_id)

    try:
      # If it is recent enough...
      if (is_in_report_window(response['metadata']['latestReportRunTimeMs'],
                              report_window)):
        # Grab the report and write contents to a file.
        save_report_to_file(output_dir, response['queryId'],
                            (response['metadata']
                             ['googleCloudStoragePathForLatestReport']),download_method)
        print ('Download complete.')
      else:
        request_body = {}
        request_body['dataRange'] = report_daterange
        # Call the API, getting the latest status for the passed queryId.
        response = (doubleclick_bid_manager.queries().runquery(queryId=query_id,body=request_body)
                    .execute())

        # print('No reports for queryId "%s" in the last %s hours. The query is executed now.' %
        #       (response['queryId'], report_window))
    except KeyError:
      print('No report found for queryId "%s".' % query_id)
  else:
    # Call the API, getting a list of queries.
    response = doubleclick_bid_manager.queries().listqueries().execute()
    # Print queries out.
    print ('Id\t\tName')
    if 'queries' in response:
      for q in response['queries']:
        print ('%s\t%s' % (q['queryId'], q['metadata']['title']))
    else:
      print ('No queries exist.')

def dbm_getquery_safe(doubleclick_bid_manager, query_id, wait_time=60, dont_wait=False):
    """Determines if the given query is running waits for next
       wait_time seconds and check again until it is not running.

    Args:
      doubleclick_bid_manager: Object The DBM object
      query_id: str Query ID.
      wait_time: int interval in seconds between checking status (Default; 60s)
    Returns:
      A boolean indicating whether the given query's report run time is within
      the report window.
    """
    query = (doubleclick_bid_manager.queries()\
        .getquery(queryId=query_id).execute())
    while query['metadata']['running']:
        print('Query is still running ( %s ). Unable to start download now. '
              'Download will be resumed automatically.' %
              strftime('%a, %d %b %Y %H:%M:%S +0000'))
        if dont_wait:
            raise
            break
        else:
            sleep(wait_time)
        query = (doubleclick_bid_manager.queries()\
            .getquery(queryId=query_id).execute())
    return query

def is_in_report_window(run_time_ms, report_window):
  """Determines if the given time in milliseconds is in the report window.

  Args:
    run_time_ms: str containing a time in milliseconds.
    report_window: int identifying the range of the report window in hours.
  Returns:
    A boolean indicating whether the given query's report run time is within
    the report window.
  """
  report_time = datetime.fromtimestamp(int((run_time_ms))/1000)
  earliest_time_in_range = datetime.now() - timedelta(hours=report_window)
  return report_time > earliest_time_in_range

def save_report_to_file(output_dir, query_id, report_url, download_method):
  """Saves the contents of the report_url to the given output directory.

  Args:
    output_dir: str containing the path to the directory you want to save to.
    query_id: str containing the Id of the query that generated the report.
    report_url: str containing the url to the generated report.
    download_method: str containing the downloading method
        (URLRETRIEVE, COPYFILEOBJ, READCHUNKREADCHUNK)
  """
  size = 0
  # Create formatter for output file path.
  if not os.path.isabs(output_dir):
    output_dir = os.path.expanduser(output_dir)
  output_fmt = output_dir + '/%s.csv'

  if download_method == 'URLRETRIEVE':
      # First method is using urlretrieve
      start_time = timeit.default_timer()
      try:
        local_filename, headers = urlretrieve(report_url, output_fmt % query_id, reportDownloadProgress)
      except ContentTooShortError as e:
          print('Erroe in urlretrieve: %s' % str(e))

      if "content-length" in headers:
          size = int(headers["Content-Length"])
      end_time = timeit.default_timer()
      elapsed_time = end_time - start_time
      print('Download time urlretrive: %s in [%s]s' % (size,elapsed_time))

  if download_method == 'COPYFILEOBJ':
      # Second method is using copyfileobj
      start_time = timeit.default_timer()
      # Download the file from `url` and save it locally under `file_name`:
      try:
        with urlopen(report_url) as response, open(output_fmt % query_id, 'wb') as handle:
            shutil.copyfileobj(response, handle, 16000)
      except shutil.Error as e:
          print('Error in copying the file: %s' % str(e))
      except IOError as e:
          print('Error IO problem: %s' % str(e))

      end_time = timeit.default_timer()
      elapsed_time = end_time - start_time
      print('Download time copyfileobj: %s in [%s]s' % (size,elapsed_time))

  if download_method == 'READCHUNK':
      # Third method is using read chunk
      start_time = timeit.default_timer()
      blocknum = 0

      # Initialize the block size to 100MB
      block_size = 100 * 1024 * 1024

      try:
          with(open(output_fmt % query_id, 'wb')) as handle:
            response = urlopen(report_url)
            headers = response.info()
            if "content-length" in headers:
                size = int(headers["Content-Length"])
                # Reset the block size based on the file size
                # The file will be downloaded in 100 chunks
                block_size = int(size / 100)+1

            reportDownloadProgress(blocknum, block_size, size)
            while True:
                chunk = response.read(block_size)
                if chunk == ''.encode('utf-8'):
                    break
                blocknum += 1
                reportDownloadProgress(blocknum, block_size, size)
                handle.write(chunk)
      except Exception as e:
          print('Error: %s' % str(e))

      end_time = timeit.default_timer()
      elapsed_time = end_time - start_time
      print('\nDownload time read chunk: %s' % elapsed_time)


if __name__ == '__main__':
  valid_download_methods = ['URLRETRIEVE', 'COPYFILEOBJ', 'READCHUNK']
  args = parser.parse_args()
  # Retrieve the query id of the report we're downloading, or set to 0.
  QUERY_ID = args.query_id
  if not QUERY_ID:
    try:
      QUERY_ID = int(input('Enter the query id or press enter to '
                               'list queries: '))
    except ValueError:
      QUERY_ID = 0

  if args.download_method:
      if args.download_method in valid_download_methods:
          print('Download using %s...\n' % args.download_method)
      else:
          raise ValueError('Invalid download method. Acceptable values: %s' %
                           valid_download_methods)
  try:
    main(get_service(
          client_id=args.client_id, client_secret=args.client_secret),
        args.output_directory, QUERY_ID, args.report_window, args.report_daterange, args.download_method)
  except Exception as e:
      print('Error: %s' % str(e))

