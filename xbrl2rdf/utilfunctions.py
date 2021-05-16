import os
import urllib.parse
from io import StringIO
from urllib.request import urlopen, urlparse
from lxml import etree
try:
    import regex as re
except ImportError:
    import re
from .const import predicates
from datetime import datetime
import logging

parentDirectory = None

def processAttribute(node, attr, attr_type=None,
                     text_prefix='    ', params=None):
    if text_prefix == '    ':
        line_end: str = ' ;\n'
    else:
        line_end: str = ' .\n'

    if isinstance(node, dict):
        attr_value = node.get(attr, None)
    else:
        attr_value = node.attrib.get(attr, None)
    if attr_value:
        attr_value = attr_value.replace("\\", "\\\\")
        if attr_type == bool:
            return text_prefix+predicates[attr]+' "'+attr_value+'"^^xsd:boolean'+line_end
        elif attr_type == str:
            return text_prefix+predicates[attr]+' """'+attr_value+'"""^^rdf:XMLLiteral'+line_end
        elif attr_type == int:
            return text_prefix+predicates[attr]+' "'+attr_value+'"^^xsd:integer'+line_end
        elif attr_type == float:
            return text_prefix+predicates[attr]+' "'+attr_value+'"^^xsd:decimal'+line_end
        elif attr_type == datetime:
            return text_prefix+predicates[attr]+' "'+attr_value+'"^^xsd:dateTime'+line_end
        else:
            name = attr_value.split("/")[-1]
            base = "/".join(attr_value.split("/")[0:-1])
            prefix = params['namespaces'].get(base, None)
            if prefix:
                attr_value = prefix+":"+name
            else:
                attr_value = "<"+attr_value+">"
            return text_prefix+predicates[attr]+' '+attr_value+line_end
    else:
        return ''


def prependDtsQueue(uri_type, uri, base, ns, force, params):
    """ put uri at start of dtsqueue
        an item in the DtsQueue consists of uri_type
        (linkbase, schema), uri and namespace
    """
    uri = expandRelativePath(uri, base)
    if force != 0:
        params['dts_processed'].remove(uri)
    for entry in params['dts_queue']:
        if entry[1] == uri:
            params['dts_queue'].remove(entry)
    params['dts_queue'].insert(0, (uri_type, uri, ns))
    return 0


def appendDtsQueue(uri_type, uri, base, ns, force, params):
    """ put uri at end of dtsqueue if not already present
    """
    uri = expandRelativePath(uri, base)
    if force != 0:
        params['dts_processed'].remove(uri)
    for entry in params['dts_queue']:
        if entry[1] == uri:
            params['dts_queue'].remove(entry)
    #     return -1

    params['dts_queue'].append((uri_type, uri, ns))
    return 0

def xmlFromFile(filename):
    '''takes a url (or local filename) and returns root XML object'''
    assert ('../' not in filename), 'garbage file ref got through: \n' + filename
    if 'http' in filename:
        filename=filename.replace('\\','/')
        return etree.parse(urlopen(filename)).getroot()
    return etree.parse(filename).getroot()

def getParentDirectory(filename):
    #print('from filename:\n', filename, '\ngot directory\n', os.path.dirname(filename) + os.sep)
    return os.path.dirname(filename) + os.sep

def fixFileReference(url, parentDirectory, first=True):
    '''tries to repair file reference, as they are often garbage'''
    #print('ffr url:\n',url,'\nparentDir\n')
    #see if it is a file, return normalized if so
    if os.path.isfile(url):
        return os.path.normpath(url)
    #check for relative locators
    parts = urlparse(url)
    #print(parts)
    if not parts.scheme:
        assert(first == True), 'bad times'
        recurse = parentDirectory + url
        return fixFileReference(recurse, parentDirectory, first=False)
    #clean up ../ and recombine
    normPath = os.path.normpath(parts.path)
    if normPath == '.':
        normPath = ''
    resultSeparator = '://'
    #special handling for windows os
    myScheme = parts.scheme
    if 'c' in parts.scheme:
        myScheme = 'C'
        resultSeparator = ':'
    result = myScheme + resultSeparator + parts.netloc + normPath
    if len(parts.fragment) > 0 :
        result = result + '#' + parts.fragment
    return result

def isHttpUrl(url):
    return isinstance(url, str) and (url.startswith("http://") or url.startswith("https://"))


def getLanguageCode():
    return "en"


def isAbsolute(url):
    if url is not None:
        scheme, sep, path = url.partition(":")
        if scheme in ("http", "https", "ftp"):
            return path.startswith("//")
        if scheme == "urn":
            return True
    return False


def loadXML(handler, uri, ns, params, do_downloads = True):
    #skip if already in completed_output
    target_output = ''.join(os.path.basename(uri).split(".")[0:-1]) + '.ttl'
    if target_output in params['completed_output']:
        print(target_output, ' has already been processed, skipping:' ,uri)
        return 0
    global parentDirectory
    res = 0
    xmlRoot = None
    if uri in params['dts_processed']:
        return 0  # already loaded
    else:
        params['dts_processed'].append(uri)

    if isHttpUrl(uri):
        if parentDirectory == None:
            parentDirectory = getParentDirectory(uri)
        mappedUri = os.path.abspath(params['xbrl_zipfile'].mappedUrl(uri))
        if mappedUri not in params['uri2file'].keys() and do_downloads:
            logging.info('xbrl uri "'+uri+'" not found in zip file, attempting download\n')
            print('processing: ' +uri)
            xmlRoot = xmlFromFile(fixFileReference(uri,parentDirectory))
        elif mappedUri in params['uri2file'].keys():
            filePath = params['uri2file'][mappedUri]
            try:
                fp = params['xbrl_zipfile'].fs.open(filePath, "r")
                content = fp.read()
            except:
                logging.info('Could not read '+uri+' from zip-file, even though file present\n')
                return -1
        else:
            logging.info(uri+' not in zip-file\n')
            return -1



    else:  # treat as local file

        if uri[0:6] == "file:/":
            filePath = uri[6:]
        else:
            filePath = uri
        try:
            fp = open(filePath, "rb")
            content = fp.read()
            fp.close()
        except:
            params['log'].write("Error: "+uri+" is malformed\n")
            params['errorCount'] += 1
            return -1
    if xmlRoot is not None:
        root = xmlRoot
    else:
        root = etree.fromstring(content,
                            parser=etree.XMLParser(remove_comments=True))
    if root is None:
        params['log'].write("Error: document has no root element.\n")
        params['errorCount'] += 1
        return -1
    #add a ns for the instance, or a numbered dts namespace
    if handler.__name__ == 'processInstance':
        addNamespace("instance", os.path.basename(uri), params)
        handlerPrefix = 'instance'
    elif handler.__name__ == 'processDtsFile':
        params['dtsCount'] = params['dtsCount'] + 1
        dtsCount = str(params['dtsCount'])
        currentDts = 'dts'+dtsCount
        #safeUri = urllib.parse.quote(uri, safe='')
        #full https filenames are too long for OS sometimes
        simpleUri = ''.join(os.path.basename(uri).split(".")[0:-1])
        addNamespace(currentDts, uri, params)
        params['urlfilename'][currentDts] = simpleUri
        params['pagedata'][currentDts] = StringIO()
        params['sources'][currentDts] = uri
        handlerPrefix = currentDts
    else:
        assert(False), 'unregistered handler: '+ handler.__name__

    #print('in loadXML, handler:', handler.__name__, 'prefix', handlerPrefix)

    res = handler(root, uri, ns, params, handlerPrefix)

    params['fileCount'] += 1

    return res


def registerNamespaces(root, base, params):
    nsmap = root.nsmap
    for prefix in nsmap.keys():
        uri = nsmap[prefix]
        if uri not in params['namespaces_to_skip']:
            addNamespace(prefix, uri, params)
    return 0


def addNamespace(prefix, uri, params):
    namespaces = params['namespaces']
    found = namespaces.get(uri, None)
    if found:
        if prefix != found:
            # print("error!!! prefix with different uris")
            # print(prefix+ ", " + found + ", " + uri)
            return -1
        del namespaces[uri]
    namespaces[uri] = prefix
    # params['prefixes'].write("@prefix "+prefix+": <"+uri+">.\n")
    return 0


def printNamespaces(params):
    namespaces = params['namespaces']
    res: str = ''
    for uri in namespaces:
        if uri[-1] != "#":
            res += "@prefix "+namespaces[uri]+": <"+uri+"#>.\n"
        else:
            res += "@prefix "+namespaces[uri]+": <"+uri+">.\n"
    return res

def expandRelativePath(relPath, base):

    # if relPath[0:7]=="http://":
    if isHttpUrl(relPath):
        return relPath
    elif relPath[0] == '#':
        return base+relPath
    else:
        return urllib.parse.urljoin(base, relPath)


xmlEncodingPattern = re.compile(r"\s*<\?xml\s.*encoding=['\"]([^'\"]*)['\"].*\?>")


def encoding_type(xml, default="utf-8"):
    if isinstance(xml, bytes):
        s = xml[0:120]
        if s.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'
        if s.startswith(b'\xff\xfe'):
            return 'utf-16'
        if s.startswith(b'\xfe\xff'):
            return 'utf-16'
        if s.startswith(b'\xff\xfe\x00\x00'):
            return 'utf-32'
        if s.startswith(b'\x00\x00\xfe\xff'):
            return 'utf-32'
        if s.startswith(b'# -*- coding: utf-8 -*-'):
            return 'utf-8'  # python utf=encoded
        if b"x\0m\0l" in s:
            str = s.decode("utf-16")
        else:
            str = s.decode("latin-1")
    else:
        str = xml[0:80]
    match = xmlEncodingPattern.match(str)
    if match and match.lastindex == 1:
        return match.group(1)
    return default
