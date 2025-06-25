"""Configuration loader module for France Data Collector.

This module provides functionality to load configuration from YAML files
and merge with environment variables.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv


class ConfigLoader:
    """Handles loading and merging configuration from YAML and environment variables."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the configuration loader.
        
        Args:
            config_path: Path to the configuration YAML file.
                        If None, uses default path 'config/config.yaml'
        """
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            'config', 
            'config.yaml'
        )
        self._config: Dict[str, Any] = {}
        self._load_env_vars()
        self._load_config()
    
    def _load_env_vars(self) -> None:
        """Load environment variables from .env file if it exists."""
        env_path = Path('.env')
        if env_path.exists():
            load_dotenv(env_path)
    
    def _load_config(self) -> None:
        """Load configuration from YAML file and process environment variables."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file) or {}
            
            # Process environment variable substitutions
            self._substitute_env_vars(self._config)
            
        except FileNotFoundError:
            raise ConfigError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ConfigError(f"Error parsing YAML configuration: {e}")
    
    def _substitute_env_vars(self, obj: Any) -> Any:
        """Recursively substitute environment variables in configuration.
        
        Environment variables are specified as ${VAR_NAME} in the YAML file.
        
        Args:
            obj: Configuration object to process
            
        Returns:
            Processed configuration object
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                obj[key] = self._substitute_env_vars(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = self._substitute_env_vars(item)
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            value = os.environ.get(env_var)
            if value is None:
                raise ConfigError(f"Environment variable '{env_var}' not found")
            return value
        
        return obj
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key.
        
        Supports nested keys using dot notation (e.g., 'gcs_config.bucket_name')
        
        Args:
            key: Configuration key (supports dot notation)
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_required(self, key: str) -> Any:
        """Get a required configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation)
            
        Returns:
            Configuration value
            
        Raises:
            ConfigError: If key not found
        """
        value = self.get(key)
        if value is None:
            raise ConfigError(f"Required configuration key '{key}' not found")
        return value
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get the full configuration dictionary."""
        return self._config
    
    def validate(self) -> None:
        """Validate the configuration has all required fields."""
        required_keys = [
            'data_sources.dvf.base_url',
            'data_sources.sirene.base_url',
            'data_sources.insee_contours.base_url',
            'data_sources.plu.wfs_endpoint',
            'gcs_config.bucket_name',
            'processing_config.batch_size',
            'processing_config.max_retries',
        ]
        
        for key in required_keys:
            self.get_required(key)
    
    def __repr__(self) -> str:
        """String representation of the configuration."""
        return f"ConfigLoader(config_path='{self.config_path}')"


class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass


# Singleton instance
_config_instance: Optional[ConfigLoader] = None


def get_config() -> ConfigLoader:
    """Get the singleton configuration instance.
    
    Returns:
        ConfigLoader instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigLoader()
    return _config_instance


def reload_config(config_path: Optional[str] = None) -> ConfigLoader:
    """Reload the configuration from file.
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        New ConfigLoader instance
    """
    global _config_instance
    _config_instance = ConfigLoader(config_path)
    return _config_instance