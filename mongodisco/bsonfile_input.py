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
File: bsonfile_input.py
Description: Implementation of :class:`disco.worker.classic.func.InputStream` 
An input stream for reading bson-formatted input files.

To use, call MongoJob.run(bson_input=True, input=urls, create_input_splits=False)
Note that option input_uri will pull from a mongodb uri and override a bson file input.
'''
from bson import decode_all

class BsonInputStream(object):
    """Input stream of bson-formatted files"""

    def __init__(self, stream, params):
        input_key = (params or {}).get('bson_input_key', '_id')
        docs = decode_all(stream.read())
        self.length = len(docs)
        self.docs = ((str(obj.get(input_key, 'no_input_key')), obj) for obj in docs)

    def __len__(self):
        return self.length

    def __iter__(self):
        #most important method
        return self.docs.__iter__()

    def read(self, size=-1):
        #raise a non-implemented error to see if this ever pops up
        raise Exception("read is not implemented- investigate why this was called")

def input_stream(stream, size, url, params):
    # This looks like a mistake, but it is intentional.
    # Due to the way that Disco imports and uses this
    # function, we must re-import the module here.
    from mongodisco.bsonfile_input import BsonInputStream
    return BsonInputStream(stream, params)