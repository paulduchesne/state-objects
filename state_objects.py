import base64
import hashlib
import pathlib
import rdflib
import state
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

def file_attributes(df, subj, pred):

    ''' Pull specific attribute about an entity and return object. '''

    filename = df.loc[df.subject.isin([subj]) & df.predicate.isin([pred])]
    if len(filename) != 1:
        raise Exception('Expected exactly one response.')

    state_stub = filename.reset_index().at[0, 'source']
    state_id = pathlib.Path(state_stub).name
    res_triple = state.read_statement(state_id)
    for a,b,c in res_triple.triples((None, None, None)):         
        return c

def send_files(directory):

    ''' Send files into public graph. '''

    incoming_files = [x for x in directory.rglob('*') if x.is_file() == True]
    for x in incoming_files:
        private_graph = file_graph(x)
        state.write_statements(private_graph)

def retrieve_files(directory):

    ''' Retrieve files from public graph. '''

    graph_map = state.map_statements()
    file_list = graph_map.loc[graph_map.object.isin([rdflib.URIRef('paul://ontology/file')])]
    for f in file_list.subject.unique():
        filename = file_attributes(graph_map, f, rdflib.URIRef('paul://ontology/filename'))
        filehash = file_attributes(graph_map, f, rdflib.URIRef('paul://ontology/filehash'))
        filedata = file_attributes(graph_map, f, rdflib.URIRef('paul://ontology/filedata'))

        file_suffix = pathlib.Path(filename).suffix
        output_path = directory / filehash[:2] / f'{filehash}{file_suffix}'
        output_path.parents[0].mkdir(exist_ok=True, parents=True)
        with open(output_path, 'wb') as output:
            output.write(base64.decodebytes(filedata.encode('utf-8')))

        test = checksummer(output_path)
        if test != str(filehash):
            raise Exception('Hash does not match.')

# send_files(pathlib.Path.home() / 'media') # send to public graph.
# retrieve_files(pathlib.Path.home() / 'recreated') # recreate files from the public graph.
