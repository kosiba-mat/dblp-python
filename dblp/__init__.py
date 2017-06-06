import json
import concurrent.futures
import xml.etree.ElementTree

from collections import namedtuple

import requests

from lxml import etree

DBLP_BASE_URL = 'http://dblp.uni-trier.de/'
DBLP_AUTHOR_SEARCH_URL = DBLP_BASE_URL + 'search/author'
DBLP_PUBLICATION_SEARCH_URL = DBLP_BASE_URL + 'search/publ'

DBLP_PERSON_URL = DBLP_BASE_URL + 'pers/xx/{}/{}'
DBLP_PUBLICATION_URL = DBLP_BASE_URL + 'rec/bibtex/{key}.xml'


class LazyAPIData(object):
    def __init__(self, lazy_attrs):
        self.lazy_attrs = set(lazy_attrs)
        self.data = None

    def __getattr__(self, key):
        if key in self.lazy_attrs:
            if self.data is None:
                self.load_data()
            return self.data[key]
        raise (AttributeError, key)

    def load_data(self):
        pass


def make_dict_from_tree(element_tree):
    """Traverse the given XML element tree to convert it into a dictionary.
    :param element_tree: An XML element tree
    :type element_tree: xml.etree.ElementTree
    :rtype: dict
    """
    def internal_iter(tree, accum):
        """Recursively iterate through the elements of the tree accumulating
        a dictionary result.
        :param tree: The XML element tree
        :type tree: xml.etree.ElementTree
        :param accum: Dictionary into which data is accumulated
        :type accum: dict
        :rtype: dict
        """
        if tree is None:
            return accum
        if tree.getchildren():
            accum[tree.tag] = {}
            for each in tree.getchildren():
                result = internal_iter(each, {})
                if each.tag in accum[tree.tag]:
                    if not isinstance(accum[tree.tag][each.tag], list):
                        accum[tree.tag][each.tag] = [
                            accum[tree.tag][each.tag]
                        ]
                    accum[tree.tag][each.tag].append(result[each.tag])
                else:
                    accum[tree.tag].update(result)
        else:
            accum[tree.tag] = tree.text
        return accum
    return internal_iter(element_tree, {})


class Author(LazyAPIData):
    """
    Represents a DBLP author. All data but the author's key is lazily loaded.
    Fields that aren't provided by the underlying XML are None.

    Attributes:
    name - the author's primary name record
    publications - a list of lazy-loaded Publications results by this author
    homepages - a list of author homepage URLs
    homonyms - a list of author aliases
    """
    def __init__(self, urlpt):
        self.urlpt = urlpt
        self.xml = None
        super(Author, self).__init__(['name', 'publications', 'homepages',
                                      'affiliation'])

    def load_data(self):
        resp = requests.get(DBLP_PERSON_URL.format(*self.urlpt))
        # TODO error handling
        self.xml = resp.content
        root = etree.fromstring(self.xml)
        data = {
            'name': root.attrib['name'],
            'affiliation': root.xpath("/dblpperson/person/note[@type='affiliation']/text()"),
            'publications': [make_dict_from_tree(x) for x in root.xpath("/dblpperson/r/*[text()]")],
            'homepages': root.xpath("/dblpperson/person/url/text()")
        }
        self.data = data

def first_or_none(seq):
    try:
        return next(iter(seq))
    except StopIteration:
        pass

Publisher = namedtuple('Publisher', ['name', 'href'])
Series = namedtuple('Series', ['text', 'href'])
Citation = namedtuple('Citation', ['reference', 'label'])

class Publication(LazyAPIData):
    """
    Represents a DBLP publication- eg, article, inproceedings, etc. All data but
    the key is lazily loaded. Fields that aren't provided by the underlying XML
    are None.

    Attributes:
    type - the publication type, eg "article", "inproceedings", "proceedings",
    "incollection", "book", "phdthesis", "mastersthessis"
    sub_type - further type information, if provided- eg, "encyclopedia entry",
    "informal publication", "survey"
    title - the title of the work
    authors - a list of author names
    journal - the journal the work was published in, if applicable
    volume - the volume, if applicable
    number - the number, if applicable
    chapter - the chapter, if this work is part of a book or otherwise
    applicable
    pages - the page numbers of the work, if applicable
    isbn - the ISBN for works that have them
    ee - an ee URL
    crossref - a crossrel relative URL
    publisher - the publisher, returned as a (name, href) named tuple
    citations - a list of (text, label) named tuples representing cited works
    series - a (text, href) named tuple describing the containing series, if
    applicable
    """
    def __init__(self, key):
        self.key = key
        self.xml = None
        super(Publication, self).__init__( ['type', 'sub_type', 'mdate',
                'authors', 'editors', 'title', 'year', 'month', 'journal',
                'volume', 'number', 'chapter', 'pages', 'ee', 'isbn', 'url',
                'booktitle', 'crossref', 'publisher', 'school', 'citations',
                'series'])

    def load_data(self):
        resp = requests.get(DBLP_PUBLICATION_URL.format(key=self.key))
        xml = resp.content
        self.xml = xml
        root = etree.fromstring(xml)
        publication = first_or_none(root.xpath('/dblp/*[1]'))
        if publication is None:
            raise ValueError
        data = {
            'type': publication.tag,
            'sub_type': publication.attrib.get('publtype', None),
            'mdate': publication.attrib.get('mdate', None),
            'authors': publication.xpath('author/text()'),
            'editors': publication.xpath('editor/text()'),
            'title': first_or_none(publication.xpath('title/text()')),
            'year': int(first_or_none(publication.xpath('year/text()'))),
            'month': first_or_none(publication.xpath('month/text()')),
            'journal': first_or_none(publication.xpath('journal/text()')),
            'volume': first_or_none(publication.xpath('volume/text()')),
            'number': first_or_none(publication.xpath('number/text()')),
            'chapter': first_or_none(publication.xpath('chapter/text()')),
            'pages': first_or_none(publication.xpath('pages/text()')),
            'ee': first_or_none(publication.xpath('ee/text()')),
            'isbn': first_or_none(publication.xpath('isbn/text()')),
            'url': first_or_none(publication.xpath('url/text()')),
            'booktitle': first_or_none(publication.xpath('booktitle/text()')),
            'crossref': first_or_none(publication.xpath('crossref/text()')),
            'publisher': first_or_none(publication.xpath('publisher/text()')),
            'school': first_or_none(publication.xpath('school/text()')),
            'citations': [Citation(c.text, c.attrib.get('label',None))
                         for c in publication.xpath('cite') if c.text != '...'],
            'series': first_or_none(Series(s.text, s.attrib.get('href', None))
                      for s in publication.xpath('series'))
        }

        self.data = data


def resolve_url(url):
    try:
        x = requests.get(url)
        print(x.url)
        author = Author(x.url.split('/')[-2:])
        author.load_data()
        return author
    except:
        pass


def search_author(author_str, affiliation=None):
    query_param = author_str
    if affiliation:
        query_param += ' :affiliation:' + affiliation
    search_string = DBLP_AUTHOR_SEARCH_URL + '/api'
    resp = requests.get(search_string,
                        params={'q': query_param, 'format': 'json'})
    authors = json.loads(resp.text)
    print(authors)

    results = []
    executor = concurrent.futures.thread.ThreadPoolExecutor(max_workers=50)
    futures = []
    if 'hit' not in authors['result']['hits']:
        return
    for hit in authors['result']['hits']['hit']:
        url = hit['info']['url']
        futures.append(executor.submit(resolve_url, url))
    for future in futures:
        if future.result() is None:
            continue
        results.append(future.result())
    return results


def search_publication(publ_str, years, venue=None):
    def cleanup(pubs):
        clean = []
        for pub in pubs:
            clean.append(pub['info'])
        return clean

    r_executor = concurrent.futures.thread.ThreadPoolExecutor(max_workers=50)
    futures, respones = [], []
    query_string = publ_str
    if venue is not None:
        query_string += " venue:" + venue
    for year in years:
        new_qstring = query_string + " year:" + str(year)
        futures.append(r_executor.submit(requests.get,
                       DBLP_PUBLICATION_SEARCH_URL + '/api',
                       params={'q': new_qstring,
                               'format': 'json',
                               'h': '1000',
                               'f': '0'}))
    while futures:
        get_request = futures.pop().result()
        json_info = get_request.json()['result']['hits']
        first, sent = int(json_info['@first']), int(json_info['@sent'])
        total = int(json_info['@total'])
        if total == 0 or 'hit' not in json_info:
            continue
        respones.extend(cleanup(json_info['hit']))
        if first + sent < total:
            current_first = first + sent
            url = (get_request.url[:get_request.url.find("&f=")]
                   + "&f=" + str(current_first))
            futures.append(r_executor.submit(requests.get,
                                             url))
    return respones
