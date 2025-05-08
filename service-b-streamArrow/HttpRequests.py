import aiohttp

async def fetchSubstraitPlan(sqlQuery:str,queryPlanServerAddress:str,clientSession:aiohttp.ClientSession):
    data={
        "query":sqlQuery
    }
    async with clientSession as session:
        async with session.post(queryPlanServerAddress,json=data) as response:
            if response.status ==200:
                response_json= await response.json()
                return response_json

