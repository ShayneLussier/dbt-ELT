import requests

# -------------------- FUNCTIONS -------------------- #

# Retrieve exchange rates based on €EURO
def convert_price_to_cad(amount, date):
    url_eur_exchange_rates = f'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/eur.json'
    response = requests.get(url_eur_exchange_rates)

    if response.status_code == 200:
        exchange_rates = response.json()
        inr_rate = float(exchange_rates['eur']['inr'])
        cad_rate = float(exchange_rates['eur']['cad'])

        # Convert Euro rates to INR:CAD
        inr_to_cad_rate = cad_rate / inr_rate

        cad_amount = round((amount * inr_to_cad_rate), 2)
        return cad_amount