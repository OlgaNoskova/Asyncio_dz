import asyncio
import aiohttp
from more_itertools import chunked
from models import init_orm, close_orm, DbSession, SwapiPeople


MAX_CHUNK = 5


async def count_people(http_session):
    response = await http_session.get(f"https://swapi.py4e.com/api/people/")
    json_data = await response.json()
    count_people = json_data['count']
    return count_people


async def get_film(http_session, people_info):
    films_list = []
    for film in people_info['films']:
        films_get = await http_session.get(film)
        films_list_json = await films_get.json()
        films_list.append(films_list_json['title'])
    films_str = ', '.join(films_list)
    return films_str


async def get_homeworld(http_session, people_info):
    homeworld_get = await http_session.get(people_info['homeworld'])
    homeworld_json = await homeworld_get.json()
    homeworld = homeworld_json['name']
    return homeworld


async def get_specie(http_session, people_info):
    specie_list = []
    for specie in people_info['species']:
        specie_get = await http_session.get(specie)
        specie_list_json = await specie_get.json()
        specie_list.append(specie_list_json['name'])
    specie_str = ', '.join(specie_list)
    return specie_str


async def get_starship(http_session, people_info):
    starship_list = []
    for starship in people_info['starships']:
        starship_get = await http_session.get(starship)
        starship_list_json = await starship_get.json()
        starship_list.append(starship_list_json['name'])
    starship_str = ', '.join(starship_list)
    return starship_str


async def get_vehicle(http_session, people_info):
    vehicle_list = []
    for vehicle in people_info['vehicles']:
        vehicle_get = await http_session.get(vehicle)
        vehicle_list_json = await vehicle_get.json()
        vehicle_list.append(vehicle_list_json['name'])
    vehicle_str = ', '.join(vehicle_list)
    return vehicle_str


async def get_people(http_session, people_id):
    response = await http_session.get(f"https://swapi.py4e.com/api/people/{people_id}/")
    return await response.json()


async def parameter_in_json_data(http_session, json_data, params, function):
    if params in json_data:
        result_json = await function(http_session, json_data)
    else:
        result_json = 'unknown'
    return result_json


async def insert_people(http_session, json_list):
    async with DbSession() as session:
        orm_objects = []
        for json_data in json_list:
            films = await parameter_in_json_data(http_session, json_data, 'films', get_film)
            homeworld = await parameter_in_json_data(http_session, json_data, 'homeworld', get_homeworld)
            species = await parameter_in_json_data(http_session, json_data, 'species', get_specie)
            starships = await parameter_in_json_data(http_session, json_data, 'starships', get_starship)
            vehicles = await parameter_in_json_data(http_session, json_data, 'vehicles', get_vehicle)
            res = SwapiPeople(birth_year=json_data.get('birth_year', 'unknown'),
                              eye_color=json_data.get('eye_color', 'unknown'),
                              films=films,
                              gender=json_data.get('gender', 'unknown'),
                              hair_color=json_data.get('hair_color', 'unknown'),
                              height=int(json_data.get('height', 'unknown')) if
                              json_data.get('height','unknown').isdigit() else 0,
                              homeworld=homeworld,
                              mass=int(json_data.get('mass', 'unknown'))
                              if json_data.get('mass','unknown').isdigit() else 0,
                              name=json_data.get('name', 'unknown'),
                              skin_color=json_data.get('skin_color', 'unknown'),
                              species=species,
                              starships=starships,
                              vehicles=vehicles)
            orm_objects.append(res)
        session.add_all(orm_objects)
        await session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        count = await count_people(http_session)
        for chunk_i in chunked(range(1, count), MAX_CHUNK):
            coros = [get_people(http_session, i) for i in chunk_i]
            result = await asyncio.gather(*coros)
            await insert_people(http_session, result)
    await close_orm()


asyncio.run(main())