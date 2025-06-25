"""PLU (Plan Local d'Urbanisme) Data Collector.

This module collects urban planning data from the French GPU (Géoportail de l'Urbanisme)  
service using WFS (Web Feature Service) protocol. It supports spatial filtering,
multiple output formats, and incremental updates.
"""

import json
import logging
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlencode, urlparse
import requests
import geopandas as gpd
from shapely.geometry import box
from shapely import wkt

from collectors.base_collector import BaseCollector


class PLUCollector(BaseCollector):
    """Collector for PLU (Plan Local d'Urbanisme) data from GPU WFS service."""
    
    def __init__(self):
        """Initialize PLU collector."""
        super().__init__('plu')
        
        # WFS configuration
        self.wfs_endpoint = self.collector_config.get('wfs_endpoint')
        self.service_name = self.collector_config.get('service_name', 'gpu')
        self.wfs_version = self.collector_config.get('version', '2.0.0')
        self.output_format = self.collector_config.get('output_format', 'application/json')
        self.max_features = self.collector_config.get('max_features', 5000)
        
        # Layer configuration
        self.layer_types = self.collector_config.get('layer_types', ['GPU.ZONE_URBA'])
        
        # Filtering configuration
        filter_config = self.collector_config.get('filter_options', {})
        self.use_bbox = filter_config.get('use_bbox', True)
        self.use_insee_codes = filter_config.get('use_insee_codes', True)
        self.insee_codes = filter_config.get('insee_codes', [])
        self.default_bbox = filter_config.get('default_bbox', {})
        
        # Output configuration
        self.output_formats = self.collector_config.get('output_formats', ['geojson'])
        self.input_srs = self.collector_config.get('input_srs', 'EPSG:4326')
        self.output_srs = self.collector_config.get('output_srs', 'EPSG:2154')
        
        # Processing configuration
        self.enable_incremental = self.collector_config.get('enable_incremental', True)
        self.batch_by_department = self.collector_config.get('batch_by_department', True)
        self.validate_geometry = self.collector_config.get('validate_geometry', True)
        
        # Timeouts and retries
        self.wfs_timeout = self.collector_config.get('timeout_seconds', 120)
        self.retry_on_empty = self.collector_config.get('retry_on_empty', True)
        self.max_empty_retries = self.collector_config.get('max_empty_retries', 3)
        
        self.logger.info(f"Initialized PLU collector with {len(self.layer_types)} layer types")
    
    def collect(self) -> Dict[str, Any]:
        """Main collection method for PLU data.
        
        Returns:
            Dictionary with collection results and metadata
        """
        collection_results = {
            'files_collected': 0,
            'layers_processed': 0,
            'features_collected': 0,
            'errors': [],
            'layers_summary': {}
        }
        
        self.logger.info(f"Starting PLU data collection for {len(self.layer_types)} layer types")
        
        for layer_type in self.layer_types:
            try:
                self.logger.info(f"Processing layer: {layer_type}")
                layer_result = self._collect_layer_data(layer_type)
                
                collection_results['layers_processed'] += 1
                collection_results['features_collected'] += layer_result.get('features_count', 0)
                collection_results['files_collected'] += layer_result.get('files_created', 0)
                collection_results['layers_summary'][layer_type] = layer_result
                
                self.logger.info(
                    f"Completed layer {layer_type}: "
                    f"{layer_result.get('features_count', 0)} features, "
                    f"{layer_result.get('files_created', 0)} files"
                )
                
            except Exception as e:
                error_msg = f"Failed to collect layer {layer_type}: {str(e)}"
                self.logger.error(error_msg)
                collection_results['errors'].append({
                    'layer': layer_type,
                    'error': str(e),
                    'type': type(e).__name__
                })
        
        self.logger.info(
            f"PLU collection completed: {collection_results['layers_processed']} layers, "
            f"{collection_results['features_collected']} features, "
            f"{collection_results['files_collected']} files"
        )
        
        return collection_results
    
    def _collect_layer_data(self, layer_type: str) -> Dict[str, Any]:
        """Collect data for a specific layer type.
        
        Args:
            layer_type: WFS layer name (e.g., 'GPU.ZONE_URBA')
            
        Returns:
            Dictionary with layer collection results
        """
        layer_result = {
            'features_count': 0,
            'files_created': 0,
            'requests_made': 0,
            'processing_time_seconds': 0
        }
        
        start_time = datetime.now()
        
        # Determine filtering strategy
        if self.use_bbox and self.default_bbox:
            # Use bbox filtering
            bbox_filter = self._create_bbox_filter()
            features_data = self._fetch_wfs_data(layer_type, bbox_filter=bbox_filter)
        elif self.use_insee_codes and self.insee_codes:
            # Use INSEE code filtering
            features_data = self._fetch_data_by_insee_codes(layer_type, self.insee_codes)
        else:
            # Fetch all data (use with caution - can be very large)
            self.logger.warning(f"Fetching all data for {layer_type} - this may be slow")
            features_data = self._fetch_wfs_data(layer_type)
        
        layer_result['requests_made'] = len(features_data) if isinstance(features_data, list) else 1
        
        # Process and save features
        if features_data:
            processed_data = self._process_features(features_data, layer_type)
            if processed_data is not None:
                layer_result['features_count'] = len(processed_data)
                
                # Save in different formats
                for output_format in self.output_formats:
                    file_created = self._save_layer_data(processed_data, layer_type, output_format)
                    if file_created:
                        layer_result['files_created'] += 1
        
        end_time = datetime.now()
        layer_result['processing_time_seconds'] = (end_time - start_time).total_seconds()
        
        return layer_result
    
    def _create_bbox_filter(self) -> Dict[str, float]:
        """Create bounding box filter from configuration.
        
        Returns:
            Dictionary with bbox coordinates
        """
        return {
            'min_x': self.default_bbox.get('min_x', 2.0),
            'min_y': self.default_bbox.get('min_y', 48.0),
            'max_x': self.default_bbox.get('max_x', 3.0),
            'max_y': self.default_bbox.get('max_y', 49.0),
            'srs': self.default_bbox.get('srs', 'CRS:84')
        }
    
    def _fetch_wfs_data(self, layer_type: str, bbox_filter: Optional[Dict] = None, 
                       cql_filter: Optional[str] = None) -> Optional[Dict]:
        """Fetch data from WFS service with optional filtering.
        
        Args:
            layer_type: WFS layer name
            bbox_filter: Bounding box filter parameters
            cql_filter: CQL (Common Query Language) filter string
            
        Returns:
            GeoJSON-like feature collection or None if failed
        """
        params = {
            'service': 'WFS',
            'version': self.wfs_version,
            'request': 'GetFeature',
            'typename': layer_type,
            'outputFormat': self.output_format,
            'maxfeatures': self.max_features,
            'srsname': self.input_srs
        }
        
        # Add bounding box filter
        if bbox_filter:
            bbox_str = f"{bbox_filter['min_x']},{bbox_filter['min_y']},{bbox_filter['max_x']},{bbox_filter['max_y']},{bbox_filter['srs']}"
            params['bbox'] = bbox_str
        
        # Add CQL filter
        if cql_filter:
            params['cql_filter'] = cql_filter
        
        url = f"{self.wfs_endpoint}?{urlencode(params)}"
        
        self.logger.debug(f"WFS request URL: {url}")
        
        try:
            response = requests.get(url, timeout=self.wfs_timeout)
            response.raise_for_status()
            
            # Parse response based on format
            if self.output_format == 'application/json':
                return response.json()
            else:
                # Handle GML/XML response
                return self._parse_gml_response(response.text)
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"WFS request failed for {layer_type}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response for {layer_type}: {e}")
            return None
    
    def _fetch_data_by_insee_codes(self, layer_type: str, insee_codes: List[str]) -> Optional[Dict]:
        """Fetch data filtered by INSEE commune codes.
        
        Args:
            layer_type: WFS layer name  
            insee_codes: List of INSEE commune codes
            
        Returns:
            Combined feature collection or None if failed
        """
        all_features = []
        
        # Process INSEE codes in batches to avoid URL length limits
        batch_size = 10  # Adjust based on URL length limits
        
        for i in range(0, len(insee_codes), batch_size):
            batch_codes = insee_codes[i:i + batch_size]
            
            # Create CQL filter for INSEE codes
            insee_filter = "INSEE_COM IN ('" + "','".join(batch_codes) + "')"
            
            self.logger.debug(f"Fetching data for INSEE codes batch: {batch_codes}")
            
            batch_data = self._fetch_wfs_data(layer_type, cql_filter=insee_filter)
            
            if batch_data and 'features' in batch_data:
                all_features.extend(batch_data['features'])
        
        if all_features:
            # Return combined feature collection
            return {
                'type': 'FeatureCollection',
                'features': all_features
            }
        
        return None
    
    def _parse_gml_response(self, gml_content: str) -> Optional[Dict]:
        """Parse GML/XML response to GeoJSON-like structure.
        
        Args:
            gml_content: GML XML content
            
        Returns:
            GeoJSON-like feature collection or None if failed
        """
        try:
            root = ET.fromstring(gml_content)
            
            # This is a simplified GML parser - in production, consider using
            # libraries like OGR/GDAL or lxml for robust GML parsing
            features = []
            
            # Find feature members (this depends on the specific GML structure)
            for member in root.findall('.//{http://www.opengis.net/gml}featureMember'):
                # Extract properties and geometry
                # This is a basic implementation - needs expansion for full GML support
                feature = {
                    'type': 'Feature',
                    'properties': {},
                    'geometry': None
                }
                features.append(feature)
            
            return {
                'type': 'FeatureCollection',
                'features': features
            }
            
        except ET.ParseError as e:
            self.logger.error(f"Failed to parse GML response: {e}")
            return None
    
    def _process_features(self, features_data: Dict, layer_type: str) -> Optional[gpd.GeoDataFrame]:
        """Process and validate feature data.
        
        Args:
            features_data: Raw feature collection
            layer_type: Layer type for context
            
        Returns:
            Processed GeoDataFrame or None if failed
        """
        if not features_data or 'features' not in features_data:
            self.logger.warning(f"No features found for layer {layer_type}")
            return None
        
        try:
            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features(features_data['features'])
            
            if gdf.empty:
                self.logger.warning(f"Empty GeoDataFrame for layer {layer_type}")
                return None
            
            # Set coordinate system
            gdf.crs = self.input_srs
            
            # Convert to target coordinate system if different
            if self.output_srs != self.input_srs:
                gdf = gdf.to_crs(self.output_srs)
            
            # Validate geometries if enabled
            if self.validate_geometry:
                invalid_geoms = ~gdf.geometry.is_valid
                if invalid_geoms.any():
                    self.logger.warning(
                        f"Found {invalid_geoms.sum()} invalid geometries in {layer_type}, "
                        "attempting to fix with buffer(0)"
                    )
                    gdf.loc[invalid_geoms, 'geometry'] = gdf.loc[invalid_geoms, 'geometry'].buffer(0)
            
            # Add metadata columns
            gdf['collection_timestamp'] = datetime.now(timezone.utc).isoformat()
            gdf['layer_type'] = layer_type
            gdf['source'] = 'GPU_WFS'
            
            self.logger.info(f"Processed {len(gdf)} features for layer {layer_type}")
            
            return gdf
            
        except Exception as e:
            self.logger.error(f"Failed to process features for {layer_type}: {e}")
            return None
    
    def _save_layer_data(self, gdf: gpd.GeoDataFrame, layer_type: str, 
                        output_format: str) -> bool:
        """Save layer data in specified format.
        
        Args:
            gdf: GeoDataFrame to save
            layer_type: Layer type for filename
            output_format: Output format ('geojson', 'geopackage', etc.)
            
        Returns:
            True if file was saved successfully, False otherwise
        """
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        layer_safe = layer_type.replace('.', '_').lower()
        
        if output_format == 'geojson':
            filename = f"{layer_safe}_{timestamp}.geojson"
            return self._save_as_geojson(gdf, filename)
        elif output_format == 'geopackage':
            filename = f"{layer_safe}_{timestamp}.gpkg"
            return self._save_as_geopackage(gdf, filename)
        else:
            self.logger.error(f"Unsupported output format: {output_format}")
            return False
    
    def _save_as_geojson(self, gdf: gpd.GeoDataFrame, filename: str) -> bool:
        """Save GeoDataFrame as GeoJSON.
        
        Args:
            gdf: GeoDataFrame to save
            filename: Output filename
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as tmp_file:
                gdf.to_file(tmp_file.name, driver='GeoJSON')
                
                # Upload to GCS
                gcs_path = f"{self.raw_path}/{filename}"
                success = self.upload_to_gcs(tmp_file.name, gcs_path)
                
                # Cleanup
                os.unlink(tmp_file.name)
                
                if success:
                    self.logger.info(f"Saved GeoJSON: {gcs_path}")
                    return True
                else:
                    self.logger.error(f"Failed to upload GeoJSON: {gcs_path}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Failed to save GeoJSON {filename}: {e}")
            return False
    
    def _save_as_geopackage(self, gdf: gpd.GeoDataFrame, filename: str) -> bool:
        """Save GeoDataFrame as GeoPackage.
        
        Args:
            gdf: GeoDataFrame to save
            filename: Output filename
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with tempfile.NamedTemporaryFile(suffix='.gpkg', delete=False) as tmp_file:
                gdf.to_file(tmp_file.name, driver='GPKG')
                
                # Upload to GCS
                gcs_path = f"{self.raw_path}/{filename}"
                success = self.upload_to_gcs(tmp_file.name, gcs_path)
                
                # Cleanup
                os.unlink(tmp_file.name)
                
                if success:
                    self.logger.info(f"Saved GeoPackage: {gcs_path}")
                    return True
                else:
                    self.logger.error(f"Failed to upload GeoPackage: {gcs_path}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Failed to save GeoPackage {filename}: {e}")
            return False
    
    def validate_data(self, data: Any) -> bool:
        """Validate PLU data before storage.
        
        Args:
            data: Data to validate (GeoDataFrame expected)
            
        Returns:
            True if data is valid, False otherwise
        """
        if not isinstance(data, gpd.GeoDataFrame):
            self.logger.error("Data validation failed: not a GeoDataFrame")
            return False
        
        if data.empty:
            self.logger.warning("Data validation: GeoDataFrame is empty")
            return False
        
        # Check for required columns
        required_columns = ['geometry']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            self.logger.error(f"Data validation failed: missing columns {missing_columns}")
            return False
        
        # Check geometry validity
        if self.validate_geometry:
            invalid_geoms = ~data.geometry.is_valid
            if invalid_geoms.any():
                self.logger.warning(f"Found {invalid_geoms.sum()} invalid geometries")
                # Don't fail validation, but log the issue
        
        self.logger.info(f"Data validation passed: {len(data)} features")
        return True


def plu_collector_main(event=None, context=None):
    """Cloud Function entry point for PLU collector.
    
    Args:
        event: Cloud Function event (unused)
        context: Cloud Function context (unused)
        
    Returns:
        Dictionary with collection results
    """
    collector = PLUCollector()
    result = collector.run()
    
    return {
        'statusCode': 200 if result['status'] == 'completed' else 500,
        'body': json.dumps(result),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


if __name__ == "__main__":
    # Direct execution for testing
    collector = PLUCollector()
    result = collector.run()
    print(json.dumps(result, indent=2))