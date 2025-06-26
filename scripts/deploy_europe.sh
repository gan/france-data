#!/bin/bash

# Deploy France Data Collection Project to Europe
# This script deploys all Cloud Functions to European regions for data compliance

set -e  # Exit on error

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Default to Paris region if not set
GCP_LOCATION=${GCP_LOCATION:-europe-west9}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-""}

echo "🚀 Deploying France Data Collection Project to Europe"
echo "📍 Region: $GCP_LOCATION"
echo "🏗️ Project: $GCP_PROJECT_ID"

# Verify gcloud authentication
echo "🔐 Verifying gcloud authentication..."
gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1
if [ $? -ne 0 ]; then
    echo "❌ Please authenticate with: gcloud auth login"
    exit 1
fi

# Set project
echo "🎯 Setting project to $GCP_PROJECT_ID..."
gcloud config set project $GCP_PROJECT_ID

# Enable required APIs
echo "🔌 Enabling required APIs..."
gcloud services enable cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    cloudtasks.googleapis.com \
    secretmanager.googleapis.com \
    storage.googleapis.com

# Create GCS bucket in Europe if it doesn't exist
echo "🪣 Creating GCS bucket in Europe..."
gsutil mb -l $GCP_LOCATION gs://$GCS_BUCKET_NAME 2>/dev/null || echo "Bucket already exists"

# Deploy Cloud Functions
echo "☁️ Deploying Cloud Functions..."

# DVF Collector
echo "📊 Deploying DVF Collector..."
gcloud functions deploy dvf-collector \
    --runtime python311 \
    --trigger-http \
    --entry-point dvf_collector_main \
    --source collectors/dvf \
    --region $GCP_LOCATION \
    --memory 512MB \
    --timeout 540s \
    --set-env-vars GCS_BUCKET_NAME=$GCS_BUCKET_NAME,GCP_LOCATION=$GCP_LOCATION

# SIRENE Collector  
echo "🏢 Deploying SIRENE Collector..."
gcloud functions deploy sirene-collector \
    --runtime python311 \
    --trigger-http \
    --entry-point sirene_collector_main \
    --source collectors/sirene \
    --region $GCP_LOCATION \
    --memory 512MB \
    --timeout 540s \
    --set-env-vars GCS_BUCKET_NAME=$GCS_BUCKET_NAME,GCP_LOCATION=$GCP_LOCATION

# INSEE Contours Collector
echo "🗺️ Deploying INSEE Contours Collector..."
gcloud functions deploy insee-contours-collector \
    --runtime python311 \
    --trigger-http \
    --entry-point insee_contours_collector_main \
    --source collectors/insee_contours \
    --region $GCP_LOCATION \
    --memory 512MB \
    --timeout 540s \
    --set-env-vars GCS_BUCKET_NAME=$GCS_BUCKET_NAME,GCP_LOCATION=$GCP_LOCATION

# PLU Collector
echo "🏙️ Deploying PLU Collector..."
gcloud functions deploy plu-collector \
    --runtime python311 \
    --trigger-http \
    --entry-point plu_collector_main \
    --source collectors/plu \
    --region $GCP_LOCATION \
    --memory 512MB \
    --timeout 540s \
    --set-env-vars GCS_BUCKET_NAME=$GCS_BUCKET_NAME,GCP_LOCATION=$GCP_LOCATION

# Master Scheduler
echo "🎯 Deploying Master Scheduler..."
gcloud functions deploy master-scheduler \
    --runtime python311 \
    --trigger-http \
    --entry-point master_scheduler_main \
    --source scheduler \
    --region $GCP_LOCATION \
    --memory 512MB \
    --timeout 540s \
    --set-env-vars GCS_BUCKET_NAME=$GCS_BUCKET_NAME,GCP_LOCATION=$GCP_LOCATION

# Create Cloud Scheduler jobs for automated execution
echo "⏰ Creating Cloud Scheduler jobs..."

# Daily DVF collection (2 AM Paris time)
gcloud scheduler jobs create http dvf-daily-job \
    --location $GCP_LOCATION \
    --schedule "0 2 * * *" \
    --time-zone "Europe/Paris" \
    --uri "https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/dvf-collector" \
    --http-method POST \
    --description "Daily DVF data collection at 2 AM Paris time"

# Daily SIRENE collection (3 AM Paris time)
gcloud scheduler jobs create http sirene-daily-job \
    --location $GCP_LOCATION \
    --schedule "0 3 * * *" \
    --time-zone "Europe/Paris" \
    --uri "https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/sirene-collector" \
    --http-method POST \
    --description "Daily SIRENE data collection at 3 AM Paris time"

# Weekly INSEE Contours collection (Sunday 4 AM Paris time)
gcloud scheduler jobs create http insee-weekly-job \
    --location $GCP_LOCATION \
    --schedule "0 4 * * 0" \
    --time-zone "Europe/Paris" \
    --uri "https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/insee-contours-collector" \
    --http-method POST \
    --description "Weekly INSEE Contours collection on Sunday at 4 AM Paris time"

# Weekly PLU collection (Sunday 5 AM Paris time)
gcloud scheduler jobs create http plu-weekly-job \
    --location $GCP_LOCATION \
    --schedule "0 5 * * 0" \
    --time-zone "Europe/Paris" \
    --uri "https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/plu-collector" \
    --http-method POST \
    --description "Weekly PLU data collection on Sunday at 5 AM Paris time"

# Master scheduler (runs every Sunday at 1 AM to coordinate all collectors)
gcloud scheduler jobs create http master-scheduler-job \
    --location $GCP_LOCATION \
    --schedule "0 1 * * 0" \
    --time-zone "Europe/Paris" \
    --uri "https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/master-scheduler" \
    --http-method POST \
    --description "Master scheduler coordination job on Sunday at 1 AM Paris time"

echo "✅ Deployment complete!"
echo ""
echo "🌍 All services deployed to: $GCP_LOCATION"
echo "📊 Cloud Functions URLs:"
echo "   - DVF: https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/dvf-collector"
echo "   - SIRENE: https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/sirene-collector"
echo "   - INSEE: https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/insee-contours-collector"
echo "   - PLU: https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/plu-collector"
echo "   - Scheduler: https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/master-scheduler"
echo ""
echo "⏰ Scheduled jobs created with Paris timezone:"
echo "   - DVF: Daily at 2 AM"
echo "   - SIRENE: Daily at 3 AM"
echo "   - INSEE: Weekly Sunday at 4 AM"
echo "   - PLU: Weekly Sunday at 5 AM"
echo "   - Master: Weekly Sunday at 1 AM"
echo ""
echo "🔧 To update function URLs in config, run:"
echo "   export DVF_FUNCTION_URL=https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/dvf-collector"
echo "   export SIRENE_FUNCTION_URL=https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/sirene-collector"
echo "   export INSEE_FUNCTION_URL=https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/insee-contours-collector"
echo "   export PLU_FUNCTION_URL=https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/plu-collector"