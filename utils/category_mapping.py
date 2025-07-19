GOOGLE_TO_ZARTICO_CATEGORY_MAPPING = {
    
    "/Arts & Entertainment/Events & Listings/Expos & Conventions" : "Conventions & Expos", 
    "/Arts & Entertainment/Events & Listings/" : "Events", 
    "/Arts & Entertainment/" : "Arts & Entertainment", 
    "/Autos & Vehicles/" : "Transportation",
    "/Beauty & Fitness/" : "Retail",
    "/Books & Literature/Book Retailers" : "Retail",
    "/Books & Literature/" : "Arts & Entertainment",
    "/Business & Industrial/Business Services/Corporate Events" : "Events",
    "/Business & Industrial/" : "Business & Professional",
    "/Finance/" : "Logistics & Planning",
    "/Food & Drink/" : "Food & Beverage",
    "/Hobbies & Leisure/Outdoors/" : "Outdoor Recreation",
    "/Hobbies & Leisure/Paintball" : "Outdoor Recreation",
    "/Hobbies & Leisure/Recreational Aviation" : "Outdoor Recreation",
    "/Hobbies & Leisure/Water Activities/" : "Outdoor Recreation",
    "/Hobbies & Leisure/Special Occasions/" : "Events",
    "/Hobbies & Leisure/" : "Attractions",
    "/Jobs & Education/Education/Study Abroad" : "Logistics & Planning",
    "/Jobs & Education/Education/" : "Education",
    "/Jobs & Education/" : "Business & Professional",
    "/News/Sports News" : "Sports",
    "/News/" : "Logistics & Planning",
    "/Real Estate/" : "Accommodations",
    "/Reference/Geographic Reference/Other" : "Attractions",
    "/Reference/Humanities/" : "Education",
    "/Reference/Libraries & Museums/" : "Attractions",
    "/Reference/" : "Logistics & Planning",
    "/Science/" : "Education",
    "/Shopping/" : "Retail",
    "/Sports/" : "Sports",
    "/Travel/Air Travel" : "Transportation", 
    "/Travel/Bus & Rail" : "Transportation",
    "/Travel/Car Rental & Taxi Services" : "Transportation",
    "/Travel/Cruises & Charters" : "Attractions",
    "/Travel/Hotels & Accommodations" : "Accommodations",

    "/Travel/Tourist Destinations/Beaches & Islands" : "Outdoor Recreation",
    "/Travel/Tourist Destinations/Mountain & Ski Resorts" : "Outdoor Recreation",
    "/Travel/Tourist Destinations/Regional Parks & Gardens" : "Outdoor Recreation",
    "/Travel/Tourist Destinations/Theme Parks" : "Attractions",
    "/Travel/Tourist Destinations/Zoos-Aquariums-Preserves" : "Attractions",
    "/Travel/Tourist Destinations" : "Attractions",

    "/Travel": "Logistics & Planning",

    "/Travel & Transportation/Hotels & Accommodations" : "Accommodations",
    "/Travel & Transportation/Luggage & Travel Accessories" : "Retail",
    "/Travel & Transportation/Specialty Travel/Adventure Travel" : "Attractions",
    "/Travel & Transportation/Specialty Travel/Business Travel" : "Business & Professional",
    "/Travel & Transportation/Specialty Travel/Honeymoons & Romantic Getaways": "Accommodations",
    "/Travel & Transportation/Specialty Travel/Low Cost & Last Minute Travel" : "Accommodations",
    "/Travel & Transportation/Specialty Travel/Luxury Travel" : "Accommodations",
    "/Travel & Transportation/Specialty Travel/" : "Attractions",
    "/Travel & Transportation/Tourist Destinations/Beaches & Islands" : "Outdoor Recreation",
    "/Travel & Transportation/Tourist Destinations/Mountain & Ski Resorts" : "Outdoor Recreation",
    "/Travel & Transportation/Tourist Destinations/Regional Parks & Gardens" : "Outdoor Recreation",
    "/Travel & Transportation/Tourist Destinations/" : "Attractions",

    "/Travel & Transportation/Transportation" : "Transportation",
    "/Travel & Transportation/Travel Agencies & Services/" : "Logistics & Planning", 
    "/Travel & Transportation/Travel Guides & Travelogues" : "Logistics & Planning",

    "/Travel & Transportation/" : "Logistics & Planning",

}

def map_to_zartico_category(content_topic: str) -> str:
    for prefix, z_cat in GOOGLE_TO_ZARTICO_CATEGORY_MAPPING.items():
        if content_topic.startswith(prefix):
            return z_cat
    return "Miscellaneous"  # fallback