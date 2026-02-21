"""
This file contains the KnowledgeGraphIngestor class which handles data ingestion from various sources.
It supports multiple data formats and ensures data is normalized before adding to the knowledge graph.
"""

from typing import Dict, List, Optional
import logging
import json
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataParsingError(Exception):
    """Exception raised when data parsing fails."""
    pass

class KnowledgeGraphIngestor:
    """Handles ingestion of data into the knowledge graph from various sources."""
    
    def __init__(self, kg: 'KnowledgeGraph'):
        self.kg = kg
        
    def _parse_json(self, file_path: str) -> Dict:
        """Parse JSON formatted data."""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise DataParsingError(f"Failed to parse JSON file {file_path}: {str(e)}")
    
    def _parse_csv(self, file_path: str) -> List[Dict]:
        """Parse CSV formatted data."""
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                return [row for row in reader]
        except Exception as e:
            raise DataParsingError(f"Failed to parse CSV file {file_path}: {str(e)}")

    def _normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize data into a consistent format."""
        # Example normalization rules
        normalized = []
        for entry in raw_data:
            node = {
                'id': str(entry.get('id', '')),
                'labels': [entry.get('type', 'Unknown')],
                'properties': entry.copy()
            }
            normalized.append(node)
        return normalized

    def ingest(self, file_path: str, format_type: str) -> None:
        """Ingest data into the knowledge graph."""
        if format_type == 'json':
            raw_data = self._parse_json(file_path)
        elif format_type == 'csv':
            raw_data = self._parse_csv(file_path)
        else:
            raise ValueError("Unsupported data format. Supported formats: json, csv.")
        
        normalized_data = self._normalize_data(raw_data)
        
        for node_data in normalized_data:
            node = Node(
                id=node_data['id'],
                labels=node_data['labels'],
                properties=node_data['properties']
            )
            self.kg.add_node(node)
            
        logger.info(f"Ingested {len(normalized_data)} nodes from {file_path}")

# Example usage
if __name__ == "__main__":