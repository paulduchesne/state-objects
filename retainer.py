from cryptography.fernet import Fernet
import base64
import hashlib
import json
import pandas
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

        keys_path = pathlib.Path.cwd() / 'private.json'
        if not keys_path.exists():
            with open(keys_path, 'w') as keys_out:
                json.dump(private_keys, keys_out, indent=4)
        else:
            with open(keys_path) as keys_in:
                keys_in = json.load(keys_in)
            
            with open(keys_path, 'w') as keys_out:
                json.dump(keys_in | private_keys, keys_out, indent=4)

def file_attributes(df, subj, pred):

    ''' Pull specific attribute about an entity and return object. '''

    filename = df.loc[df.subject.isin([subj]) & df.predicate.isin([pred])]
    if len(filename) != 1:
        raise Exception('Expected exactly one response.')

    state_stub = filename.reset_index().at[0, 'source']
    state_id = pathlib.Path(state_stub).name
    res_triple = pull_triple(state_id)
    for a,b,c in res_triple.triples((None, None, None)):         
        return c

def pull_triple(statement_id):

    ''' Retrieve triple against provided state id. '''

    private_keypath = pathlib.Path.cwd() / 'private.json'
    if not private_keypath.exists():
        raise Exception('Keys could not be found.')
    else:
        with open(private_keypath) as private_keys:
            private_keys = json.load(private_keys)

    statement_path = pathlib.Path.cwd() / 'turtle' / statement_id[:2] / f'{statement_id}.ttl'
    public_statememt = rdflib.Graph().parse(statement_path)
    for s,p,o in public_statememt:
        if p == rdflib.URIRef('state://ontology/content'):
            if pathlib.Path(s).name in private_keys:
                key = private_keys[pathlib.Path(s).name]
                fernet = Fernet(base64.urlsafe_b64encode(key.encode()))
                private_statement = fernet.decrypt(o.encode()).decode()
                private_graph = rdflib.Graph().parse(data=private_statement)

    if len(private_graph) == 1:
        return private_graph
    else:
        return Exception('Statement not retrieved.')

def recreate_files(directory):

    ''' Regenerate files from existing graph. '''

    # build a map of the private graph. this is currently being performed on-the-fly, 
    # but an alternate model would be that a local copy exists to be consulted.

    map_df = pandas.DataFrame(columns=['source', 'subject', 'predicate', 'object'])
    public_statements = [x.stem for x in (pathlib.Path.cwd() / 'turtle').rglob('*') if x.suffix == '.ttl']
    for x in public_statements:

        res_triple = pull_triple(x)

        for a,b,c in res_triple.triples((None, None, None)):
            if type(c) == type(rdflib.URIRef('')):
                map_df.loc[len(map_df)] = [(x),(a), (b), (c)]
            elif type(c) == type(rdflib.Literal('')):
                map_df.loc[len(map_df)] = [(x),(a), (b), (rdflib.Literal(''))]
            else:
                raise Exception('Unknown object type.')

    file_list = map_df.loc[map_df.object.isin([rdflib.URIRef('paul://ontology/file')])]
    for f in file_list.subject.unique():
        filename = file_attributes(map_df, f, rdflib.URIRef('paul://ontology/filename'))
        filehash = file_attributes(map_df, f, rdflib.URIRef('paul://ontology/filehash'))
        filedata = file_attributes(map_df, f, rdflib.URIRef('paul://ontology/filedata'))

        without_path = pathlib.Path(filename).name
        output_path = pathlib.Path.home() / 'recreated_media' / without_path
        output_path.parents[0].mkdir(exist_ok=True)
        with open(output_path, 'wb') as output:
            output.write(base64.decodebytes(filedata.encode('utf-8')))

        test = checksummer(output_path)
        if test != str(filehash):
            raise Exception('Hash does not match.')

# contribute_files(pathlib.Path.home() / 'media') # add files to public graph.

recreate_files(pathlib.Path.cwd() / 'recreate') # pull files from public graph.

