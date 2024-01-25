def is_dataframe_streaming(df,ref):
    if df.isStreaming:
        return True 
    else:
        raise ValueError(f"Streaming falhou: {ref}")
        
def is_dataframe_empty(df,ref):
    if df.rdd.isEmpty():
        raise ValueError(f"Tabela vazia: {ref}")
    return True

def is_collection_empty(db, ref):
    collection = db[ref]
    print(ref)
    if len(collection.find_one()) > 1:
        return True
    else:
        raise ValueError(f"Collection vazia: {ref} \n {dict(db.client.nodes)}")

def is_there_cosmos(client):
    return len(client.topology_description.has_server) >=1

def is_there_cosmos_readable(client):
    return len(client.topology_description.readable_servers) >=1

def is_there_cosmos_writable(client):
    return len(client.topology_description.has_writable_server) >=1
  
def run_parallel(funcs: dict):
    """Executa múltiplos processos em paralelo

    Args:
        funcs (dict): Dicionário onde a chave é a função e o valor são os argumentos da função.
    """
    from multiprocessing import Process
    processes = []
    for func, arg in funcs.items():
        proc = Process(target = func, args = (arg,))
        proc.start()
        processes.append(proc)
    
    for proc in processes:
        proc.join()
