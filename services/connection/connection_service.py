import base64
import json
import requests

oauth_url = "http://10.0.90.79:8000"
client_id = "faird-client1"
client_secret = "tcqi54cnp3cewj94nd9uop2q"

def connect_server(username: str, password: str):
    url = f"{oauth_url}/oauth/token"
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }
    params = {
        "username": username,
        "password": password,
        "grantType": "password"
    }
    try:
        response = requests.post(url, headers=headers, json=params)
        response.raise_for_status()  # 检查请求是否成功
        response_json = response.json()
        # 解析JSON字符串
        token = response_json.get("data")
        return token
    except requests.RequestException as e:
        print(f"Error connecting server: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
    except KeyError as e:
        print(f"Error parsing response: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
