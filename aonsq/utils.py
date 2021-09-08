import aiohttp


async def public_ip():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://whatismyip.akamai.com") as response:
                return await response.text()
    except:
        return "127.0.0.1"
