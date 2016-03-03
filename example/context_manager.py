#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

from os.path import dirname, basename, abspath
from itertools import chain
from datetime import datetime
import logging

import git

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from example.load import create_git_index, parse_commits, load_repo, REPO_ACTIONS

if __name__ == '__main__':
    # get trace logger and set level
    tracer = logging.getLogger('elasticsearch.trace')
    tracer.setLevel(logging.INFO)
    tracer.addHandler(logging.FileHandler('/tmp/es_trace.log'))

    # instantiate es client as context manager, connects to localhost:9200 by default
    with Elasticsearch() as es:
        # we load the repo and all commits
        load_repo(es)

        # run the bulk operations
        success, _ = bulk(es, REPO_ACTIONS, index='git', raise_on_error=True)
        print('Performed %d actions' % success)

        # now we can retrieve the documents
        es_repo = es.get(index='git', doc_type='repos', id='elasticsearch')
        print('%s: %s' % (es_repo['_id'], es_repo['_source']['description']))

        # update - add java to es tags
        es.update(
            index='git',
            doc_type='repos',
            id='elasticsearch',
            body={
              "script" : "ctx._source.tags += tag",
              "params" : {
                "tag" : "java"
              }
            }
        )

        # refresh to make the documents available for search
        es.indices.refresh(index='git')

        # and now we can count the documents
        print(es.count(index='git')['count'], 'documents in index')