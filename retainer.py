from cryptography.fernet import Fernet
import base64
import hashlib
import json
import pathlib
import rdflib
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

# convert source to public triples.

test_file = pathlib.Path.cwd() / 'image.jpg'
private_graph = file_graph(test_file)
private_keys = write_statements(private_graph)

# unencryption keys. keep safe.

print(json.dumps(private_keys, indent=4))

# mission then is to reverse this process.

# ...

