def generate_signature(params):

    sorted_params = collections.OrderedDict(sorted(params.items()))

    encoded_params = urllib.parse.urlencode(sorted_params)

    encoded_params = encoded_params.replace("%40", "@").replace("%2F", "/").replace("%25", "%")

    encoded_params = str(encoded_params)

    hash_object = hashlib.md5(encoded_params.encode())

    hash_str = hash_object.hexdigest()

    if "secret_key" in params:

    params.pop("secret_key")

    params["sign"] = hash_str

    return params



# login

# params = {"user_name":"*****","password":"****","company_id": "1","api_key": "****","secret_key":"****"}

def get_token(params):

    data = generate_signature(params)

    response_login = requests.post(url="https://api1.btloginc.com:9081//v1/user/login&quot;, data=data)

    response_login_data = response_login.json()

    assert "token" in response_login_data, response_login_data

    token = response_login.json()["token"]

    print(token)

    group_id = response_login.json()["account"]["account_groupid"]

    account_id = response_login.json()["account"]["id"]

    return token, group_id, account_id