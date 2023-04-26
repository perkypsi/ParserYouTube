import csv
import random
import zipfile
import time
import requests


def get_random_proxy():
    proxies = []
    clear_proxy = []

    with open('list_proxyseller.csv', 'r', encoding='utf8') as file:
        ready = csv.reader(file, delimiter=';')
        for row in ready:
            proxies.append(row)

    # proxy_new = random.choice(proxies)

    # manifest_json = """
    # {
    #     "version": "1.0.0",
    #     "manifest_version": 2,
    #     "name": "Chrome Proxy",
    #     "permissions": [
    #         "proxy",
    #         "tabs",
    #         "unlimitedStorage",
    #         "storage",
    #         "<all_urls>",
    #         "webRequest",
    #         "webRequestBlocking"
    #     ],
    #     "background": {
    #         "scripts": ["background.js"]
    #     },
    #     "minimum_chrome_version":"76.0.0"
    # }
    # """
    #
    # background_js = """
    # let config = {
    #         mode: "fixed_servers",
    #         rules: {
    #         singleProxy: {
    #             scheme: "http",
    #             host: "%s",
    #             port: parseInt(%s)
    #         },
    #         bypassList: ["localhost"]
    #         }
    #     };
    # chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
    # function callbackFn(details) {
    #     return {
    #         authCredentials: {
    #             username: "%s",
    #             password: "%s"
    #         }
    #     };
    # }
    # chrome.webRequest.onAuthRequired.addListener(
    #             callbackFn,
    #             {urls: ["<all_urls>"]},
    #             ['blocking']
    # );
    # """ % (proxy_new[0], proxy_new[1], proxy_new[2], proxy_new[3])
    #
    # plugin_file = 'proxy_auth_plugin.zip'
    #
    # with zipfile.ZipFile(plugin_file, 'w') as zp:
    #     zp.writestr('manifest.json', manifest_json)
    #     zp.writestr('background.js', background_js)

    for proxy in proxies:
        clear_proxy.append({
            'proxy': {
                'http': f'http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}',
                'https': f'https://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}',
            }})

    return random.choice(clear_proxy)


def get_random_account():
    accounts = []

    with open('accounts.csv', 'r', encoding='utf8') as file:
        ready = csv.reader(file, delimiter=':')
        for row in ready:
            accounts.append({'email': row[0], 'password': row[1], 'add_email': row[2]})

    return random.choice(accounts)


def bypass_captcha(key, googlekey, url):
    response = requests.get(
        f'http://2captcha.com/in.php?key={key}&method=userrecaptcha&googlekey={googlekey}&pageurl={url}')

    id = int(response.text.split('|')[1])

    ready_key = ''

    time.sleep(20)

    ready = False
    while not ready:
        response = requests.get(f'http://2captcha.com/res.php?key={key}&action=get&id={id}')
        if 'OK' not in response.text:
            time.sleep(5)
        else:
            ready_key = response.text.split('|')[1]
            break

    return ready_key