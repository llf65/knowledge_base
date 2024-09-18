# Api 配置
api_config = {
    # 'upload_wps_url': 'http://localhost:9082/file/upload',  # 上传到wps服务器的URL
    'upload_wps_url': 'http://192.168.9.250:9082/file/upload',  # 上传到wps服务器的URL
    'upload_headers': {
        'trustedprincipal': 'eyJsb2dpbk5hbWUiOiAiYWRtaW4iLCJ1c2VyTm8iOiAiYWRtaW4iLCJ1c2VyTmFtZSI6ICJhZG1pbiIsInVzZXJDb2RlIjogImhleXUxIiwiYXV0aFJvbGVzIjpbeyJhdXRob3JpdHkiOiI0MTAzMyIsInJvbGVOYW1lIjoi5Y2P5Yqe5Lq6Iiwicm9sZU5vIjoiNDEwMzMifV0sIm90aGVycyI6IHsiYXV0aFBvc2l0aW9uIjpbXSwidXNlcklkIjo4ODg4LCJsb2dpbk5hbWUiOiJhZG1pbiJ9fQ '
    },
    'upload_data': {
        'dir': '/',
        'teamId': 'default0831',
        'bizParam[isDraftUpload]': 'false',
        'bizParam[teamId]': 'knowledge'
    }
}
