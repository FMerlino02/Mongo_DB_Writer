
def extract_accommodation_level(accommodation_type: str) -> str:
    """
    Extracts the AccommodationLevel from the AccommodationType string using a hashmap lookup.
    Handles both English and Italian types, including edge cases for "Junior Suite" and "studio room".
    If no match is found, returns "Other".
    """
    if not accommodation_type or not isinstance(accommodation_type, str):
        return "Other"

    level_map = {
        "camera": "Rooms",
        "camere": "Rooms",
        "room": "Rooms",
        "rooms": "Rooms",
        "studio room": "Rooms",
        "junior suite": "Junior Suite",
        "suite": "Suite",
        "appartamento": "Apartment",
        "appartamenti": "Apartment",
        "apartment": "Apartment",
        "apartments": "Apartment",
        "villa": "Villa",
        "ville": "Villa",
        "villetta": "Villa",
        "castello": "Villa",
        "castelli": "Villa",
        "castelletto": "Villa",
        "chalet": "Villa",
        "depandance": "Dependence",
        "studio": "Studio",
        "bungalow": "Bungalow",
        "dormitory": "Dormitory"
    }

    words = accommodation_type.lower().split()
    for n in range(len(words), 0, -1):
        phrase = " ".join(words[:n])
        if phrase in level_map:
            return level_map[phrase]
    for word in words:
        if word in level_map:
            return level_map[word]
    return "Other"