import aiohttp

async def fetchSubstraitPlan(sqlQuery:str,queryPlanServerAddress:str):
    data={
        "query":sqlQuery
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{queryPlanServerAddress}/getSubstrait",json=data) as response:
            if response.status ==200:
                response_json= await response.json()
                return response_json

