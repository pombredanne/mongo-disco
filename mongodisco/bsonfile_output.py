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
File: bsonfile_output.py
Description: Implementation of :class:`disco.worker.classic.func.OutputStream` 
An output stream for writing bson-formatted output files.

To use, call MongoJob.run(bson_output=True)
Note that option output_uri will push to a mongodb uri and override a bson file output.
'''

from bson import BSON

class BsonFileOutput(object):
    '''
    Output stream for bson files
    '''

    def __init__(self, stream, params):
        self.stream = stream

        config = {}
        for key, value in params.__dict__.iteritems():
            config[key] = value

        self.key_name = config.get('job_output_key','_id')
        self.value_name = config.get('job_output_value', 'value')

    def add(self, key, val):
        result_dict = {}
        result_dict[self.key_name] = key
        result_dict[self.value_name] = val
        bytes = BSON.encode(result_dict)

        self.stream.write(bytes)

    def close(self):
        pass


def output_stream(stream,partition,url,params):
    # This looks like a mistake, but it is intentional.
    # Due to the way that Disco imports and uses this
    # function, we must re-import the module here.
    from mongodisco.bsonfile_output import BsonFileOutput
    return BsonFileOutput(stream, params)
