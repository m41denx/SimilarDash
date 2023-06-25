import asyncio
import gzip
import pickle
import time
import hashlib

from numba import njit

import gd

import numpy as np

client = gd.Client(url="http://rugd.gofruit.space/001X/db/")

def extract_objects(data):
    l = data.split(";")[1:]
    k = []
    for entry in l:
        if len(entry) > 0:
            # print(int(hashlib.sha1(entry.encode('ascii')).hexdigest()[:8], 16))
            k.append(int(hashlib.sha1(entry.encode('ascii')).hexdigest()[:8], 16))
            # k.append(int(binascii.hexlify(entry.encode('ascii')), 16))
    return k
    # return data.split(";")[1:]

async def vectorize_one_level(id):
    try:
        lvl = await client.get_level(id)
    except gd.MissingAccess:
        print(f"[{id}] NOT FOUND")
        return None
    except Exception as e:
        print(f"[{id}] TIMEOUT. EXHAUSTED FOR 5s")
        time.sleep(5)
        return vectorize_one_level(id)
    else:
        # return extract_objects(lvl.data)
        # return np.array(extract_objects(lvl.data))
        return np.array(extract_objects(lvl.data))

def loadchunk(chunkn):
    with gzip.open(f"lvl_vectors/{chunkn}.rgzpickle","rb", compresslevel=6) as f:
        return pickle.load(f)

@njit
def compare_vectors_cst(v1, v2):
    inter = list(set(v1) & set(v2))
    if len(v1)<=1 or len(v2)<=1:
        return 0,0
    return len(inter)/len(v1), len(inter)/len(v2)


def compare_vectors(v1, v2):
    inter = np.intersect1d(v1,v2)
    if len(v1)<=1 or len(v2)<=1:
        return 0,0
    return len(inter)/len(v1), len(inter)/len(v2)



def compare_chunk(v1, chunk: dict):
    probs = {}
    for i, k in chunk.items():
        val1, val2 = compare_vectors(v1,k)
        val = max(val1, val2)
        probs[str(i)]=val
    return probs

async def main():
    chunk = loadchunk(1)
    print(len(chunk))
    while True:
        id = int(input("Lvl ID:"))
        v1 = await vectorize_one_level(id)
        tim = time.time()
        probs = compare_chunk(v1, chunk)
        print(time.time()-tim,"elapsed")
        l=0
        for i in sorted(probs, key=probs.get, reverse=True):
            print(f"[{i}]: {probs[i]}")
            l+=1
            if l==5:
                break


print("Warming up...")

asyncio.run(main())