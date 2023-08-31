#!/bin/bash

# Generate static env.config.js for UI app
envfile=/usr/share/nginx/html/env-config.js
echo "window.environment = {" > $envfile

if [[ -z "${REACT_APP_AZURE_CLIENT_ID}" ]]; then
    echo "Environment variable REACT_APP_AZURE_CLIENT_ID is not defined, skipping"
else
    echo "  \"azureClientId\": \"${REACT_APP_AZURE_CLIENT_ID}\"," >> $envfile
fi

if [[ -z "${REACT_APP_AZURE_TENANT_ID}" ]]; then
    echo "Environment variable REACT_APP_AZURE_TENANT_ID is not defined, skipping"
else
    echo "  \"azureTenantId\": \"${REACT_APP_AZURE_TENANT_ID}\"," >> $envfile
fi

if [[ -z "${REACT_APP_ENABLE_RBAC}" ]]; then
    echo "Environment variable REACT_APP_ENABLE_RBAC is not defined, skipping"
else
    echo "  \"enableRBAC\": \"${REACT_APP_ENABLE_RBAC}\"," >> $envfile
fi

echo "}" >> $envfile

echo "Successfully generated ${envfile} with following content"
cat $envfile

# Start nginx
nginx

if [ "$REACT_APP_ENABLE_RBAC" == "true" ]; then
    echo "RBAC flag configured and set to true, launch both rbac and registry apps"
    if [ "x$PURVIEW_NAME" == "x" ]; then
        echo "Purview flag is not configured, run SQL registry"
        python -m feathr_registry &
    else
        echo "Purview flag is configured, run Purview registry"
        cd purview-registry &&  python main.py &
    fi
    echo "Run RBAC app"
    python -m feathr_rbac
else
    echo "RBAC flag not configured or not equal to true, only launch registry app"
    if [ "x$PURVIEW_NAME" == "x" ]; then
        echo "Purview flag is not configured, run SQL registry"
        python -m feathr_registry
    else
        echo "Purview flag is configured, run Purview registry"
        cd purview-registry && python main.py
    fi
fi
