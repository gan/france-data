#!/usr/bin/env python3
"""Validation script to ensure project infrastructure is properly configured."""

import os
import sys
import subprocess
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config_loader import get_config, ConfigError


def check_python_version():
    """Check Python version is 3.11 or higher."""
    print("Checking Python version...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 11:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"✗ Python {version.major}.{version.minor}.{version.micro} (requires 3.11+)")
        return False


def check_dependencies():
    """Check all required dependencies are installed."""
    print("\nChecking dependencies...")
    required_packages = [
        'google.cloud.storage',
        'functions_framework',
        'pandas',
        'geopandas',
        'yaml',
        'dotenv',
        'tenacity',
        'tqdm'
    ]
    
    all_good = True
    for package in required_packages:
        try:
            if package == 'yaml':
                __import__('yaml')
            elif package == 'dotenv':
                __import__('dotenv')
            else:
                __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} not installed")
            all_good = False
    
    return all_good


def check_configuration():
    """Check configuration can be loaded."""
    print("\nChecking configuration...")
    try:
        config = get_config()
        
        # Check critical configuration values
        checks = [
            ('data_sources.dvf.base_url', 'DVF URL'),
            ('data_sources.sirene.base_url', 'SIRENE URL'),
            ('data_sources.insee_contours.base_url', 'INSEE Contours URL'),
            ('data_sources.plu.wfs_endpoint', 'PLU WFS Endpoint'),
            ('processing_config.batch_size', 'Batch size'),
            ('processing_config.max_retries', 'Max retries'),
        ]
        
        all_good = True
        for key, name in checks:
            value = config.get(key)
            if value:
                print(f"✓ {name}: {value}")
            else:
                print(f"✗ {name}: not found")
                all_good = False
        
        # Check environment-dependent values
        try:
            bucket_name = config.get('gcs_config.bucket_name')
            print(f"✓ GCS Bucket: {bucket_name}")
        except ConfigError:
            print("✗ GCS Bucket: GCS_BUCKET_NAME environment variable not set")
            all_good = False
        
        return all_good
        
    except Exception as e:
        print(f"✗ Error loading configuration: {e}")
        return False


def check_directory_structure():
    """Check project directory structure."""
    print("\nChecking directory structure...")
    
    required_dirs = [
        'collectors/dvf',
        'collectors/sirene',
        'collectors/insee_contours',
        'collectors/plu',
        'config',
        'tests',
        'utils',
        'credentials'
    ]
    
    all_good = True
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"✓ {dir_path}/")
        else:
            print(f"✗ {dir_path}/ missing")
            all_good = False
    
    return all_good


def check_gcs_access():
    """Check Google Cloud Storage access."""
    print("\nChecking Google Cloud Storage access...")
    
    # Check if credentials are configured
    cred_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not cred_env:
        print("✗ GOOGLE_APPLICATION_CREDENTIALS not set")
        return False
    
    if not Path(cred_env).exists():
        print(f"✗ Credentials file not found: {cred_env}")
        return False
    
    print(f"✓ Credentials file: {cred_env}")
    
    # Try to import and use GCS client
    try:
        from utils.gcs_client import get_gcs_client
        
        client = get_gcs_client()
        print(f"✓ GCS client initialized for bucket: {client.bucket_name}")
        
        # Test bucket access
        try:
            # This will create bucket if it doesn't exist
            bucket = client.bucket
            print(f"✓ Bucket accessible: {bucket.name}")
            
            # Initialize directory structure
            print("Initializing GCS directory structure...")
            client.initialize_directory_structure()
            print("✓ Directory structure initialized")
            
            return True
            
        except Exception as e:
            print(f"✗ Error accessing bucket: {e}")
            return False
        
    except Exception as e:
        print(f"✗ Error initializing GCS client: {e}")
        return False


def check_environment_variables():
    """Check required environment variables."""
    print("\nChecking environment variables...")
    
    required_vars = [
        ('GCP_PROJECT_ID', 'Google Cloud Project ID'),
        ('GCS_BUCKET_NAME', 'GCS Bucket Name'),
        ('GOOGLE_APPLICATION_CREDENTIALS', 'Service Account Credentials Path')
    ]
    
    optional_vars = [
        ('ALERT_EMAIL', 'Alert Email'),
        ('SLACK_WEBHOOK_URL', 'Slack Webhook URL'),
        ('MAX_WORKERS', 'Max Workers'),
        ('ENABLE_DEBUG', 'Debug Mode')
    ]
    
    all_good = True
    
    print("Required:")
    for var, name in required_vars:
        value = os.environ.get(var)
        if value:
            # Mask sensitive values
            if 'CREDENTIALS' in var:
                display_value = f"{value[:20]}..." if len(value) > 20 else value
            else:
                display_value = value
            print(f"✓ {name} ({var}): {display_value}")
        else:
            print(f"✗ {name} ({var}): not set")
            all_good = False
    
    print("\nOptional:")
    for var, name in optional_vars:
        value = os.environ.get(var)
        if value:
            print(f"✓ {name} ({var}): {value}")
        else:
            print(f"- {name} ({var}): not set")
    
    return all_good


def main():
    """Run all validation checks."""
    print("=" * 60)
    print("France Data Collector - Setup Validation")
    print("=" * 60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Dependencies", check_dependencies),
        ("Configuration", check_configuration),
        ("Directory Structure", check_directory_structure),
        ("Environment Variables", check_environment_variables),
        ("Google Cloud Storage", check_gcs_access),
    ]
    
    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"\n✗ Error during {name} check: {e}")
            results[name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name}: {status}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\nAll checks passed! The project is ready for use. ✓")
        return 0
    else:
        print("\nSome checks failed. Please fix the issues above. ✗")
        return 1


if __name__ == "__main__":
    sys.exit(main())