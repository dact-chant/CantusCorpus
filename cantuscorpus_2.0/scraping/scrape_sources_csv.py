#!/usr/bin/env python
"""
This is a script that scrapes source information for all sources mentioned in
a CantusCorpus-format chants CSV file. Relies on the 'srclink' field of the
CSV file.
"""

import argparse
import logging
import requests
import time

import pandas as pd

from db_scrapers import UniversalSourceScraper

__version__ = "0.0.2"
__author__ = "Jan Hajic jr."
__changelog__ = {
    "0.0.2": {"updated_by": "Anna Dvorakova", "date": "2025-02-27", "changes": "adapt to new DB web pages"},
    "0.0.3": {"updated_by": "Anna Dvorakova", "date": "2026-03-04", "changes": "clean old code"},
}



def build_argument_parser():
    parser = argparse.ArgumentParser(description=__doc__, add_help=True,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-i', '--input_csv', action='store', required=True,
                        help='A CantusCorpus-format CSV file with chants.')
    parser.add_argument('-o', '--output_csv', action='store', required=True,
                        help='A CSV file with the scraped source information.')

    parser.add_argument('--require_century', action='store_true',
                        help='If set, will discard sources for which century information'
                             ' cannot be ascertained from its page.')
    
    parser.add_argument('--require_provenance', action='store_true',
                    help='If set, will discard sources for which provenance information'
                            ' cannot be ascertained from its page.')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Turn on INFO messages.')
    parser.add_argument('--debug', action='store_true',
                        help='Turn on DEBUG messages.')

    return parser


def main(args):
    logging.info('Starting main...')
    _start_time = time.process_time()

    chants = pd.read_csv(args.input_csv)
    logging.info('Loaded {0} chants from {1}'.format(len(chants), args.input_csv))

    # Get list of values for the 'srclink' field
    try:
        source_urls = chants['srclink'].unique()
    except:
        try:
            source_urls = chants['source_id'].unique()
        except:
            raise ValueError('No source_id or srclink filed in chants file.')
    logging.info('Found {0} unique source URLs'.format(len(source_urls)))

    scraper = UniversalSourceScraper(require_century=args.require_century, require_provenance=args.require_provenance)
    logging.info('Scraper required fields: {}'.format(scraper.required_fields))


    sources = []
    for i, source_url in enumerate(source_urls):
        logging.info('Processing source {}/{} at URL: {}'.format(i+1, len(source_urls), source_url))
        try:
            source = scraper.scrape_source_url(source_url)
            sources.append(source)
        except NotImplementedError:
            logging.warning('Scraper for URL {} not available'.format(source_url))
        except requests.RequestException as e:
            logging.warning('Source at URL {} not available: {}'.format(source_url, e))
        except ValueError as e:
            logging.warning('Could not process URL {}. Error message:\n{}'.format(source_url, e))
        except TypeError as e:
            logging.warning('Could not process URL {}. Error message:\n{}'.format(source_url, e))

    # Write the sources to a CSV file.
    logging.info('Writing {0} sources to {1}'.format(len(sources), args.output_csv))

    with open(args.output_csv, 'w') as f:
        f.write(sources[0].csv_header_row() + '\n')
        for source in sources:
            f.write(source.to_csv_row() + '\n')

    _end_time = time.process_time()
    logging.info('scrape_cantus_db_sources.py done in {0:.3f} s'.format(_end_time - _start_time))


if __name__ == '__main__':
    parser = build_argument_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    main(args)
