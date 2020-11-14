import sys
import csv
from google.transit import gtfs_realtime_pb2
from dataclasses import dataclass
import requests

# MTA API URLs for each line
GTFS_RT_URLS = {
    **dict.fromkeys(["A", "C", "E"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"),
    **dict.fromkeys(["B", "D", "F", "M"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm"),
    **dict.fromkeys(["G"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g"),
    **dict.fromkeys(["J", "Z"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz"),
    **dict.fromkeys(["N", "Q", "R", "W"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw"),
    **dict.fromkeys(["L"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l"),
    **dict.fromkeys(["1", "2", "3", "4", "5", "6"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"),
    **dict.fromkeys(["7"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-7"),
    **dict.fromkeys(["SI"], "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si"),
}

# Should normally be in .env file and not committed to repo
API_KEY = "XZXsZSejgP1xMkLlr027o2a5c4Q8tTU04Zed7xxz"


def main():
    stops_data = read_static_stops("stops.txt")
    chosen_route = query_route()

    print(f"Stops of route {chosen_route}:")

    line_url = GTFS_RT_URLS[chosen_route]
    trip_updates = fetch_trip_updates(line_url)
    route_stops = extract_parent_stops(trip_updates, stops_data)

    if chosen_route in route_stops:
        for stop_id in sorted(route_stops[chosen_route]):
            if stop_id in stops_data:
                stop = stops_data[stop_id]
                print(f"- {stop.stop_name} ({stop_id}): {stop.stop_lat}, {stop.stop_lon}")
            else:
                # Some stops aren't contained in stops.txt file, so no location data can be provided for it
                print(f"- No name or location data for ({stop_id})")
    else:
        # Some routes don't seem to have real time data even if a URL is provided for it
        print(f"No real time data found for route {chosen_route}")


@dataclass
class Stop:
    """MTA Subway stop information"""

    stop_id: int
    stop_name: str
    stop_lat: float
    stop_lon: float
    location_type: str
    parent_station: str

    def parent_id(self):
        return self.parent_station or self.stop_id


def read_static_stops(filename):
    """Reads the CSV file and returns a dict mapping stop_id -> Stop"""
    with open(filename) as csvfile:
        return {row["stop_id"]: Stop(**row) for row in csv.DictReader(csvfile)}


def fetch_trip_updates(url):
    """Fetches and returns trip update messages from MTA real time data feed as protobuf object"""
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url, headers={"x-api-key": API_KEY}, allow_redirects=True)
    feed.ParseFromString(response.content)
    return [entity.trip_update for entity in feed.entity if entity.HasField("trip_update")]


def extract_parent_stops(trip_updates, stops_data):
    """
    Extract parent stop id's and associated route from trip updates protobuf object.
    If stop not found in static data, just keep the stop_id.

    Returns dict mapping route_id -> set of parent stop_id's
    """
    route_stops = {}

    for trip_update in trip_updates:

        route_id = trip_update.trip.route_id

        for stop_time in trip_update.stop_time_update:
            stop_id = stop_time.stop_id

            if stop_id in stops_data:
                stop_id = stops_data[stop_id].parent_id()

            if route_id in route_stops:
                route_stops[route_id].add(stop_id)
            else:
                route_stops[route_id] = {stop_id}

    return route_stops


def query_route():
    print("Please choose a line among the following ones: ")

    valid_choices = sorted(GTFS_RT_URLS.keys())

    print(", ".join(valid_choices))

    user_choice = input().upper()

    while user_choice not in valid_choices:
        user_choice = query_route()

    return user_choice


if __name__ == "__main__":
    sys.exit(main())
