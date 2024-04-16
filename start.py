import requests


def get_public_ip():
    try:
        response = requests.get('https://api.ipify.org')
        if response.status_code == 200:
            return response.text
        else:
            return "Error: Unable to fetch IP address"
    except Exception as e:
        return f"Exception occurred: {e}"


print(get_public_ip())
