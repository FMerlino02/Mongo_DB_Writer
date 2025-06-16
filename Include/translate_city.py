CITY_TRANSLATION = {
    "Milan": "Milano",
    "Rome": "Roma",
    "Florence": "Firenze",
    "Venice": "Venezia",
    "Naples": "Napoli",
    "Turin": "Torino",
    "Genoa": "Genova",
    "Bologna": "Bologna",
    "Palermo": "Palermo",
    "Bari": "Bari",
    # Add more as needed
}

def translate_city(city_name: str) -> str:
    """
    Translate city names from English (or other languages) to Italian using a dictionary.
    If not found, returns the original name.
    """
    return CITY_TRANSLATION.get(city_name, city_name)