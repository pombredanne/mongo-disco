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

from mongodisco.mongodb_output import mongodb_output
from mongodisco.mongodb_input import input_stream
from disco.worker.classic.func import task_output_stream

mongodb_input_stream = (input_stream,)

# Params object is deprecated. We have to pass output_uri in somehow.
def mongodb_output_stream(uri):
  def output_stream(stream, partition, url, params):
    params['output_uri'] = uri
    mongodb_output(stream, partition, url, params)

  return (task_output_stream, output_stream)
