import threading
import Bio
from Bio import Entrez
from Bio import SeqIO
from concurrent.futures import ThreadPoolExecutor

def synchronize(ncid,lock):
    handle = Entrez.efetch(db='protein', id=ncid,rettype="gb", retmode="text")
    try:
        records = list(SeqIO.parse(handle, "genbank"))[0]
        gene_id = records.annotations['db_source']
        gene_name = records.annotations['source']
        taxon = records.annotations['taxonomy']
    except:
        print(f'Error {ncid}')
        return
    gene_id = gene_id.split(' ')[-1]
    order_family = ['NA','NA']
    for t in taxon:
        if t.endswith('ales'):
            order_family[0] = t
        elif t.endswith('dae'):
            order_family[1] = t
    taxo = '\t'.join(order_family)
    lock.acquire()
    with open('ncid_taxo_table.txt','a') as f:
        f.write(f'{ncid}\t{gene_id}\t{gene_name}\t{taxo}\n')
    with open('OK.list','a') as f:
        f.write(f'{ncid}\n')
    lock.release()

if __name__ == '__main__':
    lock = threading.Lock()
    Entrez.email = '1019171694@qq.com'
    ncid_list = [i.strip() for i in open('ncid.list.txt')]
    with ThreadPoolExecutor(max_workers=5) as executor:
        for ncid in ncid_list.txt:
            executor.submit(synchronize,ncid,lock)
    
