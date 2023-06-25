import gd, asyncio

client = gd.Client(url="http://rugd.gofruit.space/00Pb/db/")

async def main():
    lvl = await client.get_level(128)
    with open('a.txt','w') as f:
        f.write(lvl.data)


asyncio.run(main())