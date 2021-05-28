"""Main module."""
import tkinter as tk
from tkinter import filedialog
from tkinter import ttk
from tkinter import messagebox
import json
import sys
import click
from io import StringIO, BytesIO
import logging
import os
from os import listdir
from os.path import join, isfile, abspath
from datetime import datetime
import rdflib
import time
from pathlib import Path

from .PackageManager import Taxonomies
from .FileSource import openFileSource
from .InstanceProcessor import processInstance
from .DtsProcessor import dispatchDtsQueue
from .utilfunctions import addNamespace, printNamespaces, \
                        expandRelativePath, isHttpUrl, loadXML

TAXONOMY_PATH = join("data", "taxonomies")
taxonomies: list = [f for f in listdir(TAXONOMY_PATH) if isfile(join(TAXONOMY_PATH, f)) and f[-3:] == 'zip']
manager = Taxonomies(TAXONOMY_PATH)
for taxonomy in taxonomies:
    manager.addPackage(join(TAXONOMY_PATH, taxonomy))
manager.rebuildRemappings()
manager.save()
taxo_choices: str = "\n".join([str(idx)+": "+str(item['name']) for idx, item in enumerate(manager.config['packages'])])


#@click.command()
#@click.option('--url', default=join("data", "instances", "qrs_240_instance.xbrl"), prompt="input file")
#@click.option('--taxo', default=2, prompt=taxo_choices)
#@click.option('--output', default=join("data", "rdf"), prompt="output directory")
#@click.option('--output_format', default=1, prompt="1: rdf-turtle\n2: rdf-star-turtle\n")

def main():
    #list of the files already processed
    completed_output = list()
    extensions_to_process = ['.xbrl']
    directory = tk.filedialog.askdirectory(title = 'Select input directory')
    output = tk.filedialog.askdirectory(title = 'Select output directory')
    #setup output directories
    Path(output + "/data").mkdir(parents=True, exist_ok=True)
    Path(output + "/taxonomies").mkdir(parents=True, exist_ok=True)
    #check to see if json file exists
    for filename in os.listdir(output):
        if filename == 'preloads.json':
            with open(join(output, 'preloads.json'), 'r') as infile:
                completed_output=json.load(infile)
    for filename in os.listdir(directory):
        extension = os.path.splitext(filename)[1]
        if extension in extensions_to_process:
            url = os.path.join(directory,filename)
            #setting the default taxo since isn't used with local files
            #and ttl rathern than ttl* since those are the options we need
            go(2, 1, url, output, completed_output)

def go(taxo: int, output_format: int, url, output, completed_output) -> int:
    log_file: str = join(output, "".join(os.path.basename(url).split(".")[0:-1])+".log")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, filemode="w")

    fp_taxo_zipfile: FileSource = openFileSource(manager.config['packages'][taxo]['URL'])
    fp_taxo_zipfile.mappedPaths = manager.config['packages'][taxo]["remappings"]
    fp_taxo_zipfile.open()

    params: dict = dict()

    params['out']: StringIO = StringIO()
    params['facts']: StringIO = StringIO()
    params['prefix']: StringIO = StringIO()

    params['xbrl_zipfile']: FileSource = fp_taxo_zipfile
    params['uri2file']: dict = {abspath(join(params['xbrl_zipfile'].url, file)): file for file in params['xbrl_zipfile'].dir}

    params['package_name']: str = manager.config['packages'][taxo]['name']
    params['package_uri']: str = manager.config['packages'][taxo]['URL']
    params['output_format']: int = output_format

    params['namespaces']: dict = dict()
    params['dts_processed']: list = list()
    params['id2elementTbl']: dict = dict()
    params['dts_queue']: list = list()
    params['factCount']: int = 0
    params['conceptCount']: int = 0
    params['xlinkCount']: int = 0
    params['arcCount']: int = 0
    params['locCount']: int = 0
    params['resCount']: int = 0
    params['linkCount']: int = 0
    params['fileCount']: int = 0
    params['errorCount']: int = 0
    params['provenanceNumber']: int = 0
    params['arcroleNumber']: int = 0
    params['roleNumber']: int = 0
    params['resourceCount']: int = 0
    params['dtsCount']: int = 0
    #dict: key: namespace, value (safe) url for filename
    params['urlfilename']: dict = dict()
    #dict: key: namespace, value stringIO containing document data
    params['pagedata']: dict = dict()
    #dict namespace -> source href
    params['sources']: dict = dict()
    #don't process instance docs that are already done
    #target_output = ''.join(os.path.basename(url).split(".")[0:-1]) + '.ttl'
    if url in completed_output:
        print(url, ' has already been processed, skipping.')
        return 0
    print('processing:', url)

    addNamespace("xbrli", "http://www.xbrl.org/2003/instance", params)
    addNamespace("link", "http://www.xbrl.org/2003/linkbase", params)
    addNamespace("xl", "http://www.xbrl.org/2003/XLink", params)
    addNamespace("arcrole", "http://www.xbrl.org/2003/arcrole/", params)
    addNamespace("arcroledim", "http://xbrl.org/int/dim/arcrole/", params)
    addNamespace("role", "http://www.xbrl.org/2003/role/", params)
    addNamespace("xsd", "http://www.w3.org/2001/XMLSchema", params)
    addNamespace("xlink", "http://www.w3.org/1999/xlink", params)
    addNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", params)
    addNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#", params)
    addNamespace("eurofiling", "http://www.eurofiling.info/xbrl/role", params)

    addNamespace("enum", "http://xbrl.org/2014/extensible-enumerations", params)
    addNamespace("gen", "http://xbrl.org/2008/generic", params)
    addNamespace("iso4217", "http://www.xbrl.org/2003/iso4217", params)
    addNamespace("label", "http://xbrl.org/2008/label", params)
    addNamespace("nonnum", "http://www.xbrl.org/dtr/type/non-numeric", params)
    addNamespace("num", "http://www.xbrl.org/dtr/type/numeric", params)
    addNamespace("table", "http://xbrl.org/2014/table", params)
    addNamespace("variable", "http://xbrl.org/2008/variable", params)
    addNamespace("xbrldi", "http://xbrl.org/2006/xbrldi", params)
    addNamespace("xbrldt", "http://xbrl.org/2005/xbrldt", params)
    addNamespace("xbrli", "http://www.xbrl.org/2003/instance", params)
    addNamespace("xs", "http://www.w3.org/2001/XMLSchema", params)

    addNamespace("cf", "http://xbrl.org/2008/filter/concept", params)
    addNamespace("tf", "http://xbrl.org/2008/filter/tuple", params)
    addNamespace("df", "http://xbrl.org/2008/filter/dimension", params)
    addNamespace("acf", "http://xbrl.org/2010/filter/aspect-cover", params)
    addNamespace("mf", "http://xbrl.org/2008/filter/match", params)
    addNamespace("gf", "http://xbrl.org/2008/filter/general", params)

    addNamespace("va", "http://xbrl.org/2008/assertion/value", params)
    addNamespace("ea", "http://xbrl.org/2008/assertion/existence", params)
    addNamespace("xbrl2rdf", "https://github.com/wjwillemse/xbrl2rdf", params)


    # schemas not to include
    params['namespaces_to_skip'] = ["http://www.xbrl.org/2003/instance",
                                    "http://xbrl.org/2005/xbrldt",
                                    "http://www.xbrl.org/2003/XLink",
                                    "http://xbrl.org/2008/variable",
                                    "http://www.xbrl.org/2003/linkbase"]

    # utilfunctions.printNamespaces(params)
    #setup filename and stringIO for instance doc
    params['urlfilename']['instance'] = '/data/'+''.join(os.path.basename(url).split(".")[0:-1])
    params['pagedata']['instance'] = StringIO()
    params['sources']['instance'] = os.path.basename(url)
    res = parse_xbrl(url, params, completed_output)
    if res:
        logging.warning("WARNING: "+str(params['errorCount'])+" error(s) found when importing "+url)

    params['prefix'] = printNamespaces(params)
    for namespace, data in params['pagedata'].items():
        file_content: StringIO = StringIO()
        file_content.write("#Source HREF: " + params['sources'][namespace]+ "\n\n")
        file_content.write("# RDF triples (turtle syntax)\n\n")
        file_content.write(params['prefix'])
        file_content.write("\n\n")
        file_content.write(params['pagedata'][namespace].getvalue().replace('\u2264', ''))
        url = params['urlfilename'][namespace]
        strtime = str(time.time())
        output_file: str = output + "".join(url) + '-' + strtime +".ttl"
        #print('writing:', namespace, 'to:', output_file)
        assert (output_file), 'unable to open ' + output_file + ' for writing!'
        fh = open(output_file, "w", encoding='utf-8')
        fh.write(file_content.getvalue())
        fh.close()
    #write preloads
    with open(join(output, 'preloads.json'), 'w') as outfile:
        json.dump(completed_output, outfile, indent=4)
    params['xbrl_zipfile'].close()

    return 0


def parse_xbrl(uri: str, params: dict, completed_output) -> int:
    assert(" " not in uri), uri + ': whitespace is not allowed in instance filenames, remove and try again'
    started = datetime.now()

    if (isHttpUrl(uri)) and (uri[0] != '/'):
        base = os.getcwd()
        if base[-1] != os.sep:
            base += os.sep
        uri = expandRelativePath(uri, base)

    if loadXML(processInstance, uri, None, params, completed_output):
        return -1

    # process taxonomy files
    res = dispatchDtsQueue(params, completed_output)

    finished = datetime.now()

    logging.info("turtle generation took " + str(finished - started) + " seconds\nfound:\n" +
                 str(params['factCount']) + " facts, \n" +
                 str(params['conceptCount']) + " concepts, \n" +
                 str(params['linkCount']) + " links, \n" +
                 str(params['xlinkCount']) + " xlinks, \n" +
                 str(params['arcCount']) + " arcs, \n" +
                 str(params['locCount']) + " locators and \n" +
                 str(params['resCount']) + " resources \nfrom processing "+str(params['fileCount'])+" files.")

    if params['errorCount'] > 0:
        res = 1

    return res


if __name__ == "__main__":
    sys.exit(main())
