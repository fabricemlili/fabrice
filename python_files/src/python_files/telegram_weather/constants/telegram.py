WEATHER_CODE: dict[int, dict[str, str]] = {
    0:  {"en": "☀️ Clear sky",                         "es": "☀️ Cielo despejado",                  "fr": "☀️ Ciel dégagé"},
    1:  {"en": "🌤️ Mainly clear",                      "es": "🌤️ Principalmente despejado",          "fr": "🌤️ Principalement dégagé"},
    2:  {"en": "⛅ Partly cloudy",                      "es": "⛅ Parcialmente nublado",              "fr": "⛅ Partiellement nuageux"},
    3:  {"en": "☁️ Overcast",                           "es": "☁️ Nublado",                           "fr": "☁️ Couvert"},
    45: {"en": "🌫️ Fog",                                "es": "🌫️ Niebla",                            "fr": "🌫️ Brouillard"},
    48: {"en": "🌫️ Depositing rime fog",                "es": "🌫️ Niebla con escarcha",               "fr": "🌫️ Brouillard givrant"},
    51: {"en": "🌦️ Drizzle: Light",                     "es": "🌦️ Llovizna: Ligera",                  "fr": "🌦️ Bruine: Légère"},
    53: {"en": "🌦️ Drizzle: Moderate",                  "es": "🌦️ Llovizna: Moderada",                "fr": "🌦️ Bruine: Modérée"},
    55: {"en": "🌧️ Drizzle: Dense",                     "es": "🌧️ Llovizna: Densa",                   "fr": "🌧️ Bruine: Dense"},
    56: {"en": "🌧️ Freezing drizzle: Light",            "es": "🌧️ Llovizna helada: Ligera",           "fr": "🌧️ Bruine verglaçante: Légère"},
    57: {"en": "🌧️ Freezing drizzle: Dense",            "es": "🌧️ Llovizna helada: Densa",            "fr": "🌧️ Bruine verglaçante: Dense"},
    61: {"en": "🌧️ Rain: Slight",                       "es": "🌧️ Lluvia: Ligera",                    "fr": "🌧️ Pluie: Légère"},
    63: {"en": "🌧️ Rain: Moderate",                     "es": "🌧️ Lluvia: Moderada",                  "fr": "🌧️ Pluie: Modérée"},
    65: {"en": "🌧️ Rain: Heavy",                        "es": "🌧️ Lluvia: Intensa",                   "fr": "🌧️ Pluie: Forte"},
    66: {"en": "🌧️ Freezing rain: Light",               "es": "🌧️ Lluvia helada: Ligera",             "fr": "🌧️ Pluie verglaçante: Légère"},
    67: {"en": "🌧️ Freezing rain: Heavy",               "es": "🌧️ Lluvia helada: Intensa",            "fr": "🌧️ Pluie verglaçante: Forte"},
    71: {"en": "🌨️ Snow fall: Slight",                  "es": "🌨️ Nevada: Ligera",                    "fr": "🌨️ Chute de neige: Légère"},
    73: {"en": "🌨️ Snow fall: Moderate",                "es": "🌨️ Nevada: Moderada",                  "fr": "🌨️ Chute de neige: Modérée"},
    75: {"en": "❄️ Snow fall: Heavy",                   "es": "❄️ Nevada: Intensa",                   "fr": "❄️ Chute de neige: Forte"},
    77: {"en": "❄️ Snow grains",                        "es": "❄️ Granos de nieve",                   "fr": "❄️ Grains de neige"},
    80: {"en": "🌦️ Rain showers: Slight",               "es": "🌦️ Chubascos: Ligeros",                "fr": "🌦️ Averses: Légères"},
    81: {"en": "🌧️ Rain showers: Moderate",             "es": "🌧️ Chubascos: Moderados",              "fr": "🌧️ Averses: Modérées"},
    82: {"en": "⛈️ Rain showers: Violent",              "es": "⛈️ Chubascos: Violentos",              "fr": "⛈️ Averses: Violentes"},
    85: {"en": "🌨️ Snow showers: Slight",               "es": "🌨️ Chubascos de nieve: Ligeros",       "fr": "🌨️ Averses de neige: Légères"},
    86: {"en": "❄️ Snow showers: Heavy",                "es": "❄️ Chubascos de nieve: Intensos",      "fr": "❄️ Averses de neige: Fortes"},
    95: {"en": "⛈️ Thunderstorm: Slight or moderate",  "es": "⛈️ Tormenta: Leve o moderada",         "fr": "⛈️ Orage: Léger ou modéré"},
    96: {"en": "⛈️ Thunderstorm with slight hail",     "es": "⛈️ Tormenta con granizo leve",         "fr": "⛈️ Orage avec légère grêle"},
    99: {"en": "⛈️ Thunderstorm with heavy hail",      "es": "⛈️ Tormenta con granizo intenso",      "fr": "⛈️ Orage avec forte grêle"},
}

HOUR_KEYBOARD: list[list[dict[str, str]]] = [
    [{"text": f"{h:02d}:00", "callback_data": f"forecast_hour_{h}"} for h in range(row_start, min(row_start + 4, 23))]
    for row_start in range(6, 23, 4)
]

COMMANDS_KEYBOARD: list[list[dict[str, str]]] = [
    [{"text": "🌤️ Weather"}, {"text": "📅 Daily forecast"}],
    [{"text": "❌ Cancel forecast"}],
]

LANGUAGE_KEYBOARD: list[list[dict[str, str]]] = [
    [
        {"text": "🇬🇧 English", "callback_data": "lang_en"},
        {"text": "🇪🇸 Español", "callback_data": "lang_es"},
    ],
    [
        {"text": "🇫🇷 Français", "callback_data": "lang_fr"},
    ],
]

TRADUCTIONS: dict[str, dict[str, str]] = {
    "lang_set": {
        "en": "✅ Language set.",
        "es": "✅ Idioma configurado.",
        "fr": "✅ Langue définie.",
    },
    "ask_location": {
        "en": "📍 What location would you like to get the weather for?",
        "es": "📍 ¿Para qué ubicación le gustaría obtener el clima?",
        "fr": "📍 Pour quel lieu souhaitez-vous obtenir la météo ?",
    },
    "error_location": {
        "en": "❌ The location '{location}' could not be found. Please try again.",
        "es": "❌ No se pudo encontrar la ubicación '{location}'. Por favor, inténtelo de nuevo.",
        "fr": "❌ Le lieu '{location}' n'a pas pu être trouvé. Veuillez réessayer.",
    },
    "error_weather": {
        "en": "❌ Could not retrieve weather data for '{location}'. Please try again.",
        "es": "❌ No se pudieron obtener los datos meteorológicos para '{location}'. Por favor, inténtelo de nuevo.",
        "fr": "❌ Impossible de récupérer les données météorologiques pour '{location}'. Veuillez réessayer.",
    },
    "weather_info": {
        "en": "🌤️ Current Weather\n\n📍 {location}\n🕐 {current_time}\n\n🌡️ {temperature}\n{weather}\n💨 {wind_speed}",
        "es": "🌤️ Clima actual\n\n📍 {location}\n🕐 {current_time}\n\n🌡️ {temperature}\n{weather}\n💨 {wind_speed}",
        "fr": "🌤️ Météo actuelle\n\n📍 {location}\n🕐 {current_time}\n\n🌡️ {temperature}\n{weather}\n💨 {wind_speed}",
    },
    "forecast_ask_location": {
        "en": "📍 For which city do you want to schedule a daily forecast?",
        "es": "📍 ¿Para qué ciudad quieres programar una previsión diaria?",
        "fr": "📍 Pour quelle ville souhaitez-vous programmer une prévision quotidienne ?",
    },
    "forecast_ask_hour": {
        "en": "🕐 At what time each day would you like to receive the forecast? (city local time)",
        "es": "🕐 ¿A qué hora cada día te gustaría recibir la previsión? (hora local de la ciudad)",
        "fr": "🕐 À quelle heure chaque jour souhaitez-vous recevoir la prévision ? (heure locale de la ville)",
    },
    "forecast_scheduled": {
        "en": "✅ Daily forecast scheduled for {location} at {hour:02d}:00 (local time).",
        "es": "✅ Previsión diaria programada para {location} a las {hour:02d}:00 (hora local).",
        "fr": "✅ Prévision quotidienne programmée pour {location} à {hour:02d}h00 (heure locale).",
    },
    "forecast_cancelled": {
        "en": "✅ Daily forecast cancelled.",
        "es": "✅ Previsión diaria cancelada.",
        "fr": "✅ Prévision quotidienne annulée.",
    },
    "forecast_no_active": {
        "en": "❌ You have no active forecast. Use /forecast to schedule one.",
        "es": "❌ No tienes ninguna previsión activa. Usa /forecast para programar una.",
        "fr": "❌ Vous n'avez pas de prévision active. Utilisez /forecast pour en programmer une.",
    },
    "forecast_message_location": {
        "en": "📅 Tomorrow's forecast\n\n📍 {location}",
        "es": "📅 Previsión de mañana\n\n📍 {location}",
        "fr": "📅 Prévision de demain\n\n📍 {location}",
    },
    "forecast_message_morning": {
        "en": "🌅 Morning\n🌡️ {temperature} (at {time})\n{weather}\n💨 {wind_speed}",
        "es": "🌅 Mañana\n🌡️ {temperature} (a las {time})\n{weather}\n💨 {wind_speed}",
        "fr": "🌅 Matin\n🌡️ {temperature} (à {time})\n{weather}\n💨 {wind_speed}",
    },
    "forecast_message_afternoon": {
        "en": "☀️ Afternoon\n🌡️ {temperature} (at {time})\n{weather}\n💨 {wind_speed}",
        "es": "☀️ Tarde\n🌡️ {temperature} (a las {time})\n{weather}\n💨 {wind_speed}",
        "fr": "☀️ Après-midi\n🌡️ {temperature} (à {time})\n{weather}\n💨 {wind_speed}",
    },
    "forecast_message_evening": {
        "en": "🌙 Evening\n🌡️ {temperature} (at {time})\n{weather}\n💨 {wind_speed}",
        "es": "🌙 Noche\n🌡️ {temperature} (a las {time})\n{weather}\n💨 {wind_speed}",
        "fr": "🌙 Soir\n🌡️ {temperature} (à {time})\n{weather}\n💨 {wind_speed}",
    },
    "error_forecast": {
        "en": "❌ Could not retrieve tomorrow's forecast. Please try again later.",
        "es": "❌ No se pudo obtener la previsión de mañana. Por favor, inténtalo más tarde.",
        "fr": "❌ Impossible de récupérer la prévision de demain. Veuillez réessayer plus tard.",
    },
    "error_occurred": {
        "en": "❌ An error occurred. Please try again later.",
        "es": "❌ Ocurrió un error. Por favor, inténtalo más tarde.",
        "fr": "❌ Une erreur s'est produite. Veuillez réessayer plus tard.",
    }
}
