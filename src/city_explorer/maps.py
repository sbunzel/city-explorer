import hashlib
import itertools
import json
import os
import time
from typing import Any, Dict, List

import googlemaps

PLACE_TYPES = [
    "Gym",
    "Park",
    "Café",
    "Supermarket",
    "Restaurant",
    "Vegetarian Restaurant",
    "Burger Restaurant",
]
DISTRICT_NAMES = [
    "Neuehrenfeld, Cologne, Germany",
    "Sülz, Cologne, Germany",
    "Raderthal, Cologne, Germany",
    "Müngersdorf, Cologne, Germany",
    "Südstadt, Cologne, Germany",
    "Zollstock, Cologne, Germany",
]

TEST_TRIGGER = {
    "place_types": ["Restaurant"],
    "district_names": ["Neuehrenfeld, Cologne, Germany"],
}
FIVETRAN_TRIGGER = {"agent": "Test", "state": {}, "secrets": {}}


class MapsData:
    def __init__(self, api_key: str) -> None:
        self.gmaps = googlemaps.Client(key=api_key)

    def get_places_table(
        self, place_type: str, district_name: str
    ) -> List[Dict[str, Any]]:
        """Requests places in a district from googlemaps API and assemples results in a flattened dictionary

        Args:
            place_type (str): Place type to search for, e.g. "Restaurant" or "Gym"
            district_name (str): Name of the district to search for, e.g. "Neuhrenfeld, Cologne, Germany"

        Returns:
            List[Dict]: List of places in or near the district
        """
        query = f"{place_type} near {district_name}"
        places = self.gmaps.places(query=query)

        places_table = []
        while "next_page_token" in places:

            for place in places["results"]:
                place_record = self._create_place_record(
                    place, place_type, district_name
                )
                places_table.append(place_record)

            time.sleep(2)
            places = self.gmaps.places(
                query=query, page_token=places["next_page_token"]
            )

        places_table = self._add_distances_from_center(places_table, district_name)

        return places_table

    @staticmethod
    def _create_place_record(
        place: Dict[str, Any], place_type: str, district_name: str
    ) -> Dict[str, Any]:
        place_record = {
            k: v
            for k, v in place.items()
            if k
            in [
                "place_id",
                "name",
                "formatted_address",
                "rating",
                "user_ratings_total",
            ]
        }
        place_id = place_record.pop("place_id")
        id_string = (
            (place_id + place_type + district_name).replace(",", "").replace(" ", "")
        )
        place_record["id"] = hashlib.md5(id_string.encode("utf-8")).hexdigest()
        place_record["gmaps_place_id"] = place_id
        place_record["location_lat"] = place["geometry"]["location"]["lat"]
        place_record["location_lng"] = place["geometry"]["location"]["lng"]
        place_record["query_place_type"] = place_type
        place_record["query_district_name"] = district_name
        return place_record

    def _add_distances_from_center(
        self, places_table: Dict[str, Any], district_name: str
    ) -> Dict[str, Any]:
        """Adds walking distances from district center from googlemaps Distance Matrix API to places in a district

        Args:
            places_table (Dict[str, Any]): Places in a district
            district_name (str): Name of the district to determine the center of

        Returns:
            Dict[str, Any]: Updated places table with distances from district center
        """
        location = self.gmaps.find_place(
            input=district_name, input_type="textquery", fields=["geometry"]
        )
        location_coordinates = tuple(
            location["candidates"][0]["geometry"]["location"].values()
        )
        place_coordinates = [
            (p["location_lat"], p["location_lng"]) for p in places_table
        ]
        distances = []
        for place_coordinates_chunk in _chunked_iterable(place_coordinates, 25):
            distances_from_center = self.gmaps.distance_matrix(
                origins=location_coordinates,
                destinations=list(place_coordinates_chunk),
                mode="walking",
            )["rows"][0]["elements"]
            for d in distances_from_center:
                distances.append(d["distance"]["value"])

        for i in range(len(places_table)):
            places_table[i]["distance_from_center"] = distances[i]

        return places_table


def main(request):
    # Get API key from trigger event
    config = request.get_json()
    place_types = config.get("place_types")
    district_names = config.get("district_names")

    # Use default values if place types and district names are not provided via the trigger event
    place_types = place_types if place_types else PLACE_TYPES
    district_names = district_names if district_names else DISTRICT_NAMES

    # Get data from maps API for all combinations of place type and district name
    maps_data = MapsData(api_key=os.environ["API_KEY"])
    places_table = []
    for place_type, district_name in itertools.product(place_types, district_names):
        places_table += maps_data.get_places_table(
            place_type=place_type, district_name=district_name
        )

    # Create response
    insert = {"places": places_table}
    response = _assemble_response_json(insert)

    return response, 200, {"Content-Type": "application/json"}


# Taken from https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
def _chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def _assemble_response_json(insert):
    response_dict = {
        "state": {},
        "schema": {"places": {"primary_key": ["id"]}},
        "insert": insert,
        "hasMore": False,
    }
    return json.dumps(response_dict, ensure_ascii=False)
