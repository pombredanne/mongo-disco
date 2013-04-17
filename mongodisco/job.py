# Copyright 2012 10gen, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

'''
File: DiscoJob.py
Author: NYU ITP team
Description: Disco Job Wrapper

'''
from disco.job import Job
from disco.core import classic_iterator
from mongodisco.mongodb_io import mongodb_output_stream, mongodb_input_stream
from mongodisco.bsonfile_io import bsonfile_output_stream, bsonfile_input_stream
from mongodisco.splitter import calculate_splits
import logging


class MongoJob(Job):

    # DEFAULT_CONFIG = {
    #     "job_output_key" : "_id",
    #     "job_output_value" : "value",
    #     # "input_uri" : "mongodb://localhost/test.in",
    #     # "output_uri" : "mongodb://localhost/test.out",
    #     "bson_input" : False, # Format input is bson files (i.e. mongodump not mongodb)
    #     "bson_output" : False, # Format output as bson files (i.e. mongodump not mongodb)
    #     "add_action" : "insert", # Action to use, insert/save
    #     "add_upsert" : False, # Upsert/instead of update
    #     "base_doc": {}, # Base document to insert
    #     "print_to_stdout": False,
    #     "job_wait": True,
    #     "split_size" : 8,
    #     "split_key" : {"_id" : 1},
    #     "create_input_splits" : True,
    #     "splits_use_shards" : False,
    #     "splits_use_chunks" : True,
    #     "slave_ok" : False,
    #     "limit" : 0,
    #     "skip" : 0,
    #     "inputKey" : None,
    #     "sort" : None,
    #     "timeout" : False,
    #     "fields" : None,
    #     "query" : {}
    # }

    def run(self, map=None, reduce=None, **jobargs):
        """Run a map-reduce job with either ``input_uri`` or ``output_uri``
        as a "mongodb://..." URI.

        .. todo:

            parameter docs
            consider "input" and "output" (sans _uri)
        """

        if not any(uri in jobargs for uri in ('input_uri', 'output_uri', 'bson_input', 'bson_output')):
            logging.info('You did not specify "input_uri" or "output_uri" '
                         'with MongoJob. This may be in error.')

        if 'mongodb://' in jobargs.get('input_uri', ''):
            jobargs['map_input_stream'] = mongodb_input_stream
            jobargs.setdefault('input', calculate_splits(jobargs))
        elif jobargs.get('bson_input', False):
            jobargs['map_input_stream'] = bsonfile_input_stream

        if 'mongodb://' in jobargs.get('output_uri', ''):
            jobargs['reduce_output_stream'] = mongodb_output_stream
            output_params = {
                'output_uri': jobargs['output_uri'],
                'job_output_key': jobargs.get('job_output_key', '_id'),
                'job_output_value': jobargs.get('job_output_value', 'value'),
                'add_action': jobargs.get('add_action', 'insert'),
                'add_upsert': jobargs.get('add_upsert', False),
                'base_doc': jobargs.get('base_doc', {})
            }

            params = jobargs.get('params', {})
            if not isinstance(params, dict):
                raise Exception('params option must be a dict')
            params['mongodb'] = output_params
            jobargs['params'] = params

        elif jobargs.get('bson_output', False):
            jobargs['reduce_output_stream'] = bsonfile_output_stream

        if map:
            jobargs['map'] = map
        if reduce:
            jobargs['reduce'] = reduce


        jobargs.setdefault('required_modules', []).extend([
            'mongodisco.mongodb_io',
            'mongodisco.mongodb_input',
            'mongodisco.mongodb_output',
            'mongodisco.mongo_util',
            'mongodisco.bsonfile_io',
            'mongodisco.bsonfile_input',
            'mongodisco.bsonfile_output'
        ])

        MongoJob.run(self, **jobargs)

        if jobargs.get('print_to_stdout'):
            for key, value in classic_iterator(self.wait(show=True)):
                print key, value

        elif jobargs.get('job_wait',False):
            self.wait(show=True)

        return self

