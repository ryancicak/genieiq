# Databricks notebook source
# MAGIC %md
# MAGIC # 🧞 GenieIQ Setup & Configuration
# MAGIC 
# MAGIC This notebook configures the **GenieIQ** app to use your **Lakebase (PostgreSQL)** or **Databricks SQL** for persistent storage.
# MAGIC 
# MAGIC ### Instructions
# MAGIC 1. Fill in the database details below.
# MAGIC 2. Run the cell.
# MAGIC 3. The script will update the App's environment variables and restart it.

# COMMAND ----------

# MAGIC %pip install databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

# Initialize SDK
w = WorkspaceClient()
APP_NAME = "simpletest"  # The name of your deployed app

# =============================================================================
# 🔧 CONFIGURATION
# =============================================================================

# OPTION 1: Lakebase (PostgreSQL) Details
# Retrieve these from your Compute -> Apps -> Lakebase/DB settings
DB_HOST = "your-postgres-host.databricks.com"  # Replace this
DB_PORT = "5432"
DB_NAME = "genieiq"
DB_USER = "genieiq_user"
DB_PASS = "your-password"  # Use secrets in production: dbutils.secrets.get(...)

# =============================================================================

print(f"🚀 Configuring App: {APP_NAME}...")

try:
    # 1. Get the App
    app = w.apps.get(name=APP_NAME)
    print(f"✅ Found App: {app.name} (ID: {app.id})")

    # 2. Update Environment Variables
    print("🔄 Updating Environment Variables...")
    
    new_vars = [
        {"name": "NODE_ENV", "value": "production"},
        {"name": "PORT", "value": "8080"},
        {"name": "LAKEBASE_HOST", "value": DB_HOST},
        {"name": "LAKEBASE_PORT", "value": DB_PORT},
        {"name": "LAKEBASE_DATABASE", "value": DB_NAME},
        {"name": "LAKEBASE_USER", "value": DB_USER},
        {"name": "LAKEBASE_PASSWORD", "value": DB_PASS}
    ]

    # 3. Deploy Update
    # We trigger a deployment using the active source code path but with NEW env vars
    source_path = app.active_deployment.source_code_path
    print(f"📦 Redeploying from {source_path} with new config...")
    
    deployment = w.apps.deploy(
        app_name=APP_NAME,
        source_code_path=source_path,
        env=new_vars  # This injects the variables!
    )
    
    print("⏳ Deployment started... waiting for completion...")
    
    # Wait for success
    while True:
        status = w.apps.get_deployment(app_name=APP_NAME, deployment_id=deployment.deployment_id)
        state = status.status.state
        print(f"   Status: {state}")
        
        if state == "SUCCEEDED":
            print("✅ SUCCESS! GenieIQ is now connected to the database.")
            print(f"👉 Go to: {app.url}")
            break
        elif state == "FAILED":
            print("❌ Deployment Failed.")
            print(status.status.message)
            break
        elif state in ["STOPPED", "ERROR"]:
             print(f"❌ Deployment ended with state: {state}")
             break
        
        time.sleep(5)

except Exception as e:
    print(f"❌ Error: {e}")
