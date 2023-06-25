import gzip
import random
import time

import numpy as np
# import cupy as cp
import gd, asyncio, pickle, binascii, hashlib

client = gd.Client(url="http://rugd.gofruit.space/001X/db/")


def extract_objects(data):
    l = data.split(";")[1:]
    k=[]
    for entry in l:
        if len(entry)>0:
            # print(int(hashlib.sha1(entry.encode('ascii')).hexdigest()[:8], 16))
            k.append(int(hashlib.sha1(entry.encode('ascii')).hexdigest()[:8], 16))
            # k.append(int(binascii.hexlify(entry.encode('ascii')), 16))
    return k
    # return data.split(";")[1:]


async def vectorize_one_level(id):
    global timeout_ctl, client
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
        return np.array(extract_objects(lvl.data))


async def vectorize_level(id, chunk, isnp):
    global timeout_ctl, client
    try:
        print(f"Using: {client.session.http.proxy}")
        lvl = await client.get_level(id)
    except gd.MissingAccess:
        print(f"[{id}] NOT FOUND")
    except Exception as e:
        print(f"[{id}] TIMEOUT. EXHAUSTED FOR 5s")
        time.sleep(5)
        await vectorize_level(id,chunk, isnp)
    else:
        pk = extract_objects(lvl.data)
        if isnp:
            arr = np.array(pk)
            # arr = cp.array(pk)
        else:
            arr = pk
        if len(arr)<1:
            return
        chunk[str(id)] = arr
        print(f"[{id}] LOADED")


async def startchunk(chunkn, chunksize=1000, slow=True, releasepool=5, np=True):
    startid = chunksize*(chunkn-1)+1
    chunk = {}
    print(f"====[CHUNK {startid}-{startid+chunksize}]====")
    if slow:
        for i in range(startid, startid + chunksize):
            # await asyncio.sleep(15+random.randint(-5,5)/10)
            await vectorize_level(i, chunk, np)
    else:
        tasks = [vectorize_level(i, chunk, np) for i in range(startid, startid+chunksize)]
        while len(tasks)>0:
            await asyncio.wait(tasks[:releasepool])
            tasks = tasks[releasepool:]
    if len(chunk)==0:
        print("EMPTY CHUNK")
        return
    print(len(chunk))
    with gzip.open(f"lvl_vectors/{chunkn}.gzpickle","wb", compresslevel=6) as f:
        pickle.dump(chunk, f)


async def main():
    for i in range(1, 1000):
        await startchunk(i, slow=True, np=True)
        input("end")

asyncio.run(main())