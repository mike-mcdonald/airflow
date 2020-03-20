import os

from airflow.configuration import conf
from flask_appbuilder.security.manager import (
    AUTH_OID,
    AUTH_REMOTE_USER,
    AUTH_DB,
    AUTH_LDAP,
    AUTH_OAUTH,
)

# Flask AppBuilder issues a warning for this, although Airflow uses a different config variable
#   so, we equate them here
SQLALCHEMY_DATABASE_URI = conf.get('core', 'SQL_ALCHEMY_CONN')

# ----------------------------------------------------
# AUTHENTICATION CONFIG
# ----------------------------------------------------
# The authentication type
# AUTH_OID : Is for OpenID
# AUTH_DB : Is for database (username/password()
# AUTH_LDAP : Is for LDAP
# AUTH_REMOTE_USER : Is for using REMOTE_USER from web server
AUTH_TYPE = AUTH_OAUTH

OAUTH_PROVIDERS = [
    {
        "name": "azure",
        "icon": "fa-windows",
        "token_key": "access_token",
        "remote_app": {
            "consumer_key": os.environ.get("AZURE_APPLICATION_ID"),
            "consumer_secret": os.environ.get("AZURE_CLIENT_SECRET"),
            "base_url": f"https://login.microsoftonline.com/{os.environ.get('AZURE_TENANT_ID')}/oauth2",
            "request_token_params": {
                "scope": "User.read email profile"
            },
            "request_token_url": None,
            "access_token_url": f"https://login.microsoftonline.com/{os.environ.get('AZURE_TENANT_ID')}/oauth2/token",
            "authorize_url": f"https://login.microsoftonline.com/{os.environ.get('AZURE_TENANT_ID')}/oauth2/authorize",
        },
    },
]

# Uncomment to setup Full admin role name
# AUTH_ROLE_ADMIN = 'Admin'

# Uncomment to setup Public role name, no authentication needed
# AUTH_ROLE_PUBLIC = 'Public'

# Will allow user self registration
AUTH_USER_REGISTRATION = False

# The default user self registration role
AUTH_USER_REGISTRATION_ROLE = "Public"
