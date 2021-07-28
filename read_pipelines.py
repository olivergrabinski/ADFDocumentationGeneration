"""Reads a single pipeline file from Azure Data Factory and appends the relevant parts 
  of its contents to an existing Markdown file. 

This file is part of ADF Documentation Generation.

ADF Documentation Generation is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

# Module: read_datasets.py

import logging
import json


def read_pipeline(pipeline_file_name, markdown_file_name):
    """Reads a single pipeline file from Azure Data Factory and appends the relevant parts
    of its contents to an existing Markdown file.

    The following information will be contained in the resulting markdown file:

    * the name of the pipeline
    * the description of the pipeline if available
    * a list of its activities along with their descriptions if available
    * the activity's query if supported and available
    * a list of its dependencies together with the dependency condition

    Parameters
    ----------
    pipeline_file_name : str
      The full path of the ADF json file to be read
    markdown_file_name : str
      The full path of the markdown file to which the contents will be appended
    """
    pipelines_file = open(markdown_file_name, 'a')
    logging.info('\t reading %s' % (pipeline_file_name))
    with open(pipeline_file_name) as json_file:
        pipelines_data = json.load(json_file)
        pipelines_file.write('\n\n ## %s \n' % pipelines_data['name'])
        if 'description' in pipelines_data['properties']:
            pipelines_file.write('\n Description: {0} \n'.format(pipelines_data['properties']['description']))
        pipelines_file.write('\n\n ### Steps \n')
        for act in pipelines_data['properties']['activities']:
            read_activity(pipelines_file, act)
    pipelines_file.close()


def read_activity(pipelines_file, act):
    """ Reads an activity and writes its information to the Markdown file. """
    pipelines_file.write('\n * Name: __{0}__, Type: {1}  \n'.format(act['name'], act['type']))
    read_activity_description(pipelines_file, act)
    read_query(pipelines_file, act)
    read_activity_dependencies(pipelines_file, act)


def read_activity_description(pipelines_file, act):
    """ Reads the activity description if available and
    writes it to the Markdown file. """
    if 'description' in act:
        description = act['description']
        pipelines_file.write('Description: {0}\n'.format(description))


def read_activity_dependencies(pipelines_file, act):
    """ Reads the activity dependencies and writes them to the Markdown file. """
    if len(act['dependsOn']) > 0:
        pipelines_file.write('\n   Dependencies:')
        for dep in act['dependsOn']:
            pipelines_file.write(
                '\n   * [{0}]({1}) ({2}) \n'.format(dep['activity'], '#' + dep['activity'].replace(' ', '-'),
                                                    dep['dependencyConditions'][0]))


def read_query(pipelines_file, act):
    """ Reads the query in the activity (if available) and writes
    it to the file in a dropdown. """
    supported_types = {
        'SqlServerSource': {
            'query_field_name': 'sqlReaderQuery'
        },
        'OracleSource': {
            'query_field_name': 'oracleReaderQuery'
        }
    }
    if 'source' in act['typeProperties']:
        source_type = act['typeProperties']['source']['type']
        if source_type in supported_types:
            query_field_name = supported_types[source_type]['query_field_name']
            query = get_value(act['typeProperties']['source'][query_field_name])
            pipelines_file.write('\n<details>\n')
            pipelines_file.write('\n<summary>Query</summary>\n')
            pipelines_file.write('\n``` sql\n{0}\n```\n'.format(query))
            pipelines_file.write('\n</details>\n')


def get_value(input):
    return get_expression_value(input) if is_expression(input) else input


def get_expression_value(expr):
    """ Given an expression returns its value.
     e.g. {"value": "some", "type": "Expression"} returns "some". """
    return expr['value']


def is_expression(input):
    """ Determines whether the input is a data factory expression.
    e.g. {"value": "some", "type": "Expression"} """
    if 'type' in input:
        return input['type'] is 'Expression'
