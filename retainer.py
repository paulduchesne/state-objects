from cryptography.fernet import Fernet
import base64
import hashlib
import json
import pathlib
import rdflib
import tqdm
import uuid

def checksummer(file_path):

    ''' Boring MD5 checksumming function. '''

    with open(file_path, 'rb') as item:
        hash = hashlib.md5()
        for buff in iter(lambda: item.read(65536), b''):
            hash.update(buff)
        checksum = hash.hexdigest().lower()
        return(checksum)

def file_graph(file_path):

    ''' Convert incoming file into private graph. '''

    if not file_path.exists():
        raise Exception('File does not exist in expected location.')

    with open(file_path, 'rb') as file_data:
        file_data = file_data.read()

    file_uuid = str(uuid.uuid4())
    file_base64 = base64.b64encode(file_data).decode('utf-8')
    file_hash = checksummer(file_path)

    graph = rdflib.Graph()
    graph.add((rdflib.URIRef(f'paul://resource/{file_uuid}'), rdflib.RDF.type, rdflib.URIRef('paul://ontology/file')))
    graph.add((rdflib.URIRef(f'paul://resource/{file_uuid}'), rdflib.URIRef('paul://ontology/filename'), rdflib.Literal(file_path)))
    graph.add((rdflib.URIRef(f'paul://resource/{file_uuid}'), rdflib.URIRef('paul://ontology/filesize'), rdflib.Literal(file_path.stat().st_size)))
    graph.add((rdflib.URIRef(f'paul://resource/{file_uuid}'), rdflib.URIRef('paul://ontology/filedata'), rdflib.Literal(file_base64)))
    graph.add((rdflib.URIRef(f'paul://resource/{file_uuid}'), rdflib.URIRef('paul://ontology/filehash'), rdflib.Literal(file_hash)))

    return graph

def write_statements(graph):

    ''' Convert private graph into public instance. '''

    key_dict = dict()

    for s,p,o in graph.triples((None, None, None)):

        statement_uuid = str(uuid.uuid4())
        statement_uri = rdflib.URIRef('web://'+statement_uuid)
        statement = rdflib.Graph().add((s, p, o)).serialize(format='nt')

        key = str(uuid.uuid4()).replace('-', '')
        fernet = Fernet(base64.urlsafe_b64encode(key.encode()))

        public_graph = rdflib.Graph()
        public_graph.add((statement_uri, rdflib.RDF.type, rdflib.URIRef('state://ontology/statement')))
 
        state_literal = rdflib.Literal(fernet.encrypt(statement.encode()).decode())
        public_graph.add((statement_uri, rdflib.URIRef('state://ontology/content'), state_literal))

        graph_path = pathlib.Path.cwd() / 'turtle' / statement_uuid[:2] / f'{statement_uuid}.ttl'
        graph_path.parents[0].mkdir(exist_ok=True, parents=True)
        public_graph.serialize(destination=str(graph_path), format='turtle')

        key_dict[statement_uuid] = key

    return key_dict

def pull_predicate(graph, subj, pred):

    ''' Pull single statement by predicate from graph. '''

    obj = [c for a,b,c in graph.triples((subj, pred, None))]
    if len(obj) != 1:
        raise Exception('Multiple statements should not be possible.')

    return obj

def contribute_files(directory):

    ''' Pass in a local directory containing files to contribute. '''

    # convert incoming media to public triples.

    incoming_files = [x for x in directory.rglob('*') if x.is_file() == True]

    for f in tqdm.tqdm(incoming_files):

        private_graph = file_graph(f)
        private_keys = write_statements(private_graph)

        # unencryption keys. keep safe.

        keys_path = pathlib.Path.cwd() / 'keys.json'
        if not keys_path.exists():
            with open(keys_path, 'w') as keys_out:
                json.dump(private_keys, keys_out, indent=4)
        else:
            with open(keys_path) as keys_in:
                keys_in = json.load(keys_in)
            
            with open(keys_path, 'w') as keys_out:
                json.dump(keys_in | private_keys, keys_out, indent=4)

def recreate_files(directory):

    ''' 
    
    Explicitly regenerate files from existing graph. 
    
    This currently has understandable problems holding the entire graph in memory,
    which would only get worse with GBs of data. Solution A) is to generate the map,
    which lists all nodes and can be used to pull URIs on all files or B) run an
    initial pass to pull all URIs which apply to files.
    
    '''

    # build the public graph.

    public_graph = rdflib.Graph()   
    public_triples = [x for x in (pathlib.Path.cwd() / 'turtle').rglob('*') if x.suffix == '.ttl']
    for x in public_triples:
        public_graph += rdflib.Graph().parse(x)

    # grab the private keys.
    
    private_keypath = pathlib.Path.cwd() / 'keys.json'
    if not private_keypath.exists():
        raise Exception('Keys could not be found.')
    else:
        with open(private_keypath) as private_keys:
            private_keys = json.load(private_keys)

    # build the private graph.

    private_graph = rdflib.Graph()
    for s,p,o in public_graph.triples((None, rdflib.RDF.type, rdflib.URIRef('state://ontology/statement'))):
        for a,b,c in public_graph.triples((s, rdflib.URIRef('state://ontology/content'), None)):
            if pathlib.Path(a).name in private_keys:
                key = private_keys[pathlib.Path(a).name]
                fernet = Fernet(base64.urlsafe_b64encode(key.encode()))
                private_statement = fernet.decrypt(c.encode()).decode()
                private_graph += rdflib.Graph().parse(data=private_statement)

    # extract files back to disk.

    for s,p,o in private_graph.triples((None, rdflib.RDF.type, rdflib.URIRef('paul://ontology/file'))):

        filename = pull_predicate(private_graph, s, rdflib.URIRef('paul://ontology/filename'))
        filehash = pull_predicate(private_graph, s, rdflib.URIRef('paul://ontology/filehash'))
        filedata = pull_predicate(private_graph, s, rdflib.URIRef('paul://ontology/filedata'))

        without_path = pathlib.Path(filename[0]).name
        output_path = pathlib.Path.cwd() / 'recreate' / without_path
        output_path.parents[0].mkdir(exist_ok=True)
        with open(output_path, 'wb') as output:
            output.write(base64.decodebytes(filedata[0].encode('utf-8')))

        test = checksummer(output_path)
        if test != str(filehash[0]):
            raise Exception('Hash does not match.')

# # add files to public graph.
# contribute_files(pathlib.Path.home() / 'media')

# # pull files from public graph.
# recreate_files(pathlib.Path.cwd() / 'recreate')

