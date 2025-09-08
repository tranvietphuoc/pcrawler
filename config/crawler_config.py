"""
YAML Configuration Loader for Crawler
"""

import os
import yaml
from typing import Dict, Any, List, Tuple
from pathlib import Path


class CrawlerConfig:
    """Load and manage YAML configuration files"""
    
    def __init__(self, config_name: str = "default"):
        self.config_name = config_name
        self.config_data = self._load_yaml_config(config_name)
    
    def _load_yaml_config(self, config_name: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        config_dir = Path(__file__).parent / "configs"
        config_file = config_dir / f"{config_name}.yml"
        
        if not config_file.exists():
            # Fallback to default if config doesn't exist
            config_file = config_dir / "default.yml"
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_name}.yml or default.yml")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    @property
    def website_config(self) -> Dict[str, str]:
        """Get website configuration"""
        return self.config_data.get("website", {})
    
    @property
    def xpath_config(self) -> Dict[str, str]:
        """Get xpath configuration"""
        return self.config_data.get("xpath", {})
    
    
    @property
    def crawl4ai_config(self) -> Dict[str, str]:
        """Get crawl4ai configuration"""
        return self.config_data.get("crawl4ai", {})
    
    @property
    def processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return self.config_data.get("processing", {})
    
    @property
    def output_config(self) -> Dict[str, str]:
        """Get output configuration"""
        return self.config_data.get("output", {})
    
    @property
    def fieldnames(self) -> List[str]:
        """Get CSV fieldnames"""
        return self.config_data.get("fieldnames", [])
    
    def get_xpath(self, key: str, **kwargs) -> str:
        """Get xpath with optional formatting"""
        xpath = self.xpath_config.get(key, "")
        if kwargs:
            xpath = xpath.format(**kwargs)
        return xpath
    
    def get_crawl4ai_query(self, source: str) -> str:
        """Get crawl4ai query for specific source"""
        if source == "website":
            return self.crawl4ai_config.get("website_query", "")
        elif source == "facebook":
            return self.crawl4ai_config.get("facebook_query", "")
        return ""
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return self.processing_config.copy()
    
    def get_output_config(self) -> Dict[str, Any]:
        """Get output configuration"""
        return self.output_config.copy()
    
    def get_fieldnames(self) -> List[str]:
        """Get CSV fieldnames"""
        return self.fieldnames.copy()
    
    
    def list_available_configs(self) -> List[str]:
        """List all available configuration files"""
        config_dir = Path(__file__).parent / "configs"
        configs = []
        if config_dir.exists():
            for file in config_dir.glob("*.yml"):
                configs.append(file.stem)
        return configs
    
    def validate_config(self) -> Tuple[bool, List[str]]:
        """Validate configuration and return (is_valid, errors)"""
        errors = []
        
        # Check required sections
        required_sections = ["website", "xpath", "crawl4ai", "processing", "output", "fieldnames"]
        for section in required_sections:
            if section not in self.config_data:
                errors.append(f"Missing required section: {section}")
        
        # Check required website config
        if "website" in self.config_data:
            website = self.config_data["website"]
            if "base_url" not in website:
                errors.append("Missing website.base_url")
            elif not website["base_url"].startswith(("http://", "https://")):
                errors.append("website.base_url must be a valid URL starting with http:// or https://")
        
        # Check required xpath config
        if "xpath" in self.config_data:
            xpath = self.config_data["xpath"]
            required_xpaths = [
                "company_name", "company_address", "company_website", 
                "company_phone", "company_links"
            ]
            for xpath_key in required_xpaths:
                if xpath_key not in xpath:
                    errors.append(f"Missing required xpath: {xpath_key}")
                elif not xpath[xpath_key].strip():
                    errors.append(f"xpath.{xpath_key} cannot be empty")
        
        # Check crawl4ai config
        if "crawl4ai" in self.config_data:
            crawl4ai = self.config_data["crawl4ai"]
            if "website_query" not in crawl4ai or not crawl4ai["website_query"].strip():
                errors.append("Missing or empty crawl4ai.website_query")
            if "facebook_query" not in crawl4ai or not crawl4ai["facebook_query"].strip():
                errors.append("Missing or empty crawl4ai.facebook_query")
        
        # Check processing config
        if "processing" in self.config_data:
            processing = self.config_data["processing"]
            required_processing = ["batch_size", "write_batch_size", "max_concurrent_pages"]
            for key in required_processing:
                if key not in processing:
                    errors.append(f"Missing processing.{key}")
                elif not isinstance(processing[key], int) or processing[key] <= 0:
                    errors.append(f"processing.{key} must be a positive integer")
            
            # Check optional processing configs
            if "max_retries" in processing and (not isinstance(processing["max_retries"], int) or processing["max_retries"] < 0):
                errors.append("processing.max_retries must be a non-negative integer")
            
            if "delay_range" in processing:
                delay_range = processing["delay_range"]
                if not isinstance(delay_range, list) or len(delay_range) != 2:
                    errors.append("processing.delay_range must be a list with 2 elements")
                elif not all(isinstance(x, (int, float)) and x >= 0 for x in delay_range):
                    errors.append("processing.delay_range elements must be non-negative numbers")
        
        # Check output config
        if "output" in self.config_data:
            output = self.config_data["output"]
            if "output_dir" not in output:
                errors.append("Missing output.output_dir")
            if "final_output" not in output:
                errors.append("Missing output.final_output")
        
        # Check fieldnames
        if "fieldnames" in self.config_data:
            fieldnames = self.config_data["fieldnames"]
            if not isinstance(fieldnames, list) or len(fieldnames) == 0:
                errors.append("fieldnames must be a non-empty list")
            else:
                # Check for required fields
                required_fields = ["industry_name", "name", "extracted_emails", "email_source"]
                for field in required_fields:
                    if field not in fieldnames:
                        errors.append(f"Missing required fieldname: {field}")
        
        return len(errors) == 0, errors



