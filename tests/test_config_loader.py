"""Test configuration loading functionality."""

import os
import sys
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config_loader import ConfigLoader, ConfigError, get_config


def test_config_loading():
    """Test basic configuration loading."""
    print("Testing configuration loading...")
    
    try:
        config = get_config()
        print("✓ Configuration loaded successfully")
        
        # Test getting values
        dvf_url = config.get('data_sources.dvf.base_url')
        print(f"✓ DVF URL: {dvf_url}")
        
        sirene_url = config.get('data_sources.sirene.base_url')
        print(f"✓ SIRENE URL: {sirene_url}")
        
        batch_size = config.get('processing_config.batch_size')
        print(f"✓ Batch size: {batch_size}")
        
        # Test default values
        non_existent = config.get('non.existent.key', 'default_value')
        assert non_existent == 'default_value'
        print("✓ Default values working")
        
    except Exception as e:
        print(f"✗ Error loading config: {e}")
        return False
    
    return True


def test_env_var_substitution():
    """Test environment variable substitution."""
    print("\nTesting environment variable substitution...")
    
    # Set a test environment variable
    os.environ['GCS_BUCKET_NAME'] = 'test-bucket-name'
    
    try:
        # Reload config to pick up new env var
        from config.config_loader import reload_config
        config = reload_config()
        
        bucket_name = config.get('gcs_config.bucket_name')
        assert bucket_name == 'test-bucket-name'
        print(f"✓ Environment variable substitution working: {bucket_name}")
        
    except ConfigError as e:
        if 'GCS_BUCKET_NAME' in str(e):
            print("✓ Environment variable validation working (expected error)")
        else:
            print(f"✗ Unexpected error: {e}")
            return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    return True


def test_required_fields():
    """Test required field validation."""
    print("\nTesting required field validation...")
    
    try:
        config = get_config()
        
        # Test getting required fields
        try:
            dvf_url = config.get_required('data_sources.dvf.base_url')
            print(f"✓ Required field found: {dvf_url}")
        except ConfigError:
            print("✗ Required field not found")
            return False
        
        # Test missing required field
        try:
            config.get_required('non.existent.required.field')
            print("✗ Should have raised ConfigError for missing field")
            return False
        except ConfigError:
            print("✓ ConfigError raised for missing required field")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    return True


def test_config_validation():
    """Test configuration validation."""
    print("\nTesting configuration validation...")
    
    # Set required env var for validation to pass
    os.environ['GCS_BUCKET_NAME'] = 'test-bucket-name'
    
    try:
        from config.config_loader import reload_config
        config = reload_config()
        config.validate()
        print("✓ Configuration validation passed")
        
    except ConfigError as e:
        print(f"✗ Configuration validation failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    return True


def main():
    """Run all tests."""
    print("=" * 50)
    print("Configuration Loader Tests")
    print("=" * 50)
    
    tests = [
        test_config_loading,
        test_env_var_substitution,
        test_required_fields,
        test_config_validation,
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print("\n" + "=" * 50)
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("All tests passed! ✓")
    else:
        print("Some tests failed! ✗")
        sys.exit(1)


if __name__ == "__main__":
    main()