import pytest
from pathlib import Path
from config.crawler_config import CrawlerConfig


class TestCrawlerConfig:
    """Test cases for CrawlerConfig class"""
    
    def test_load_default_config(self):
        """Test loading default configuration"""
        config = CrawlerConfig("default")
        assert config.config_name == "default"
        assert "website" in config.config_data
        assert "xpath" in config.config_data
    
    def test_load_1900comvn_config(self):
        """Test loading 1900comvn configuration"""
        config = CrawlerConfig("1900comvn")
        assert config.config_name == "1900comvn"
        assert config.website_config["name"] == "1900.com.vn"
        assert "1900.com.vn" in config.website_config["base_url"]
    
    def test_fallback_to_default(self):
        """Test fallback to default when config doesn't exist"""
        config = CrawlerConfig("non_existent_config")
        assert config.config_name == "non_existent_config"
        # Should fallback to default
        assert "website" in config.config_data
    
    def test_website_config_property(self):
        """Test website_config property"""
        config = CrawlerConfig("1900comvn")
        website_config = config.website_config
        assert "name" in website_config
        assert "base_url" in website_config
    
    def test_xpath_config_property(self):
        """Test xpath_config property"""
        config = CrawlerConfig("1900comvn")
        xpath_config = config.xpath_config
        assert "company_name" in xpath_config
        assert "company_address" in xpath_config
    
    def test_crawl4ai_config_property(self):
        """Test crawl4ai_config property"""
        config = CrawlerConfig("1900comvn")
        crawl4ai_config = config.crawl4ai_config
        assert "website_query" in crawl4ai_config
        assert "facebook_query" in crawl4ai_config
    
    def test_processing_config_property(self):
        """Test processing_config property"""
        config = CrawlerConfig("1900comvn")
        processing_config = config.processing_config
        assert "batch_size" in processing_config
        assert "max_concurrent_pages" in processing_config
    
    def test_output_config_property(self):
        """Test output_config property"""
        config = CrawlerConfig("1900comvn")
        output_config = config.output_config
        assert "output_dir" in output_config
        assert "final_output" in output_config
    
    def test_fieldnames_property(self):
        """Test fieldnames property"""
        config = CrawlerConfig("1900comvn")
        fieldnames = config.fieldnames
        assert isinstance(fieldnames, list)
        assert len(fieldnames) > 0
        assert "industry_name" in fieldnames
        assert "name" in fieldnames
    
    def test_get_xpath(self):
        """Test get_xpath method"""
        config = CrawlerConfig("1900comvn")
        xpath = config.get_xpath("company_name")
        assert isinstance(xpath, str)
        assert len(xpath) > 0
    
    def test_get_xpath_with_formatting(self):
        """Test get_xpath method with formatting"""
        config = CrawlerConfig("1900comvn")
        xpath = config.get_xpath("social_media_container", platform="facebook")
        assert "facebook" in xpath
    
    def test_get_crawl4ai_query(self):
        """Test get_crawl4ai_query method"""
        config = CrawlerConfig("1900comvn")
        website_query = config.get_crawl4ai_query("website")
        facebook_query = config.get_crawl4ai_query("facebook")
        
        assert isinstance(website_query, str)
        assert isinstance(facebook_query, str)
        assert len(website_query) > 0
        assert len(facebook_query) > 0
    
    def test_list_available_configs(self):
        """Test list_available_configs method"""
        config = CrawlerConfig()
        configs = config.list_available_configs()
        assert isinstance(configs, list)
        assert "default" in configs
        assert "1900comvn" in configs
    
    def test_validate_config_valid(self):
        """Test validate_config with valid configuration"""
        config = CrawlerConfig("1900comvn")
        is_valid, errors = config.validate_config()
        assert is_valid
        assert len(errors) == 0
    
    def test_validate_config_invalid(self):
        """Test validate_config with invalid configuration"""
        # Create a config with missing required fields
        config = CrawlerConfig("default")
        # Temporarily remove required field to test validation
        original_fieldnames = config.config_data.get("fieldnames", [])
        config.config_data["fieldnames"] = []
        
        is_valid, errors = config.validate_config()
        assert not is_valid
        assert len(errors) > 0
        
        # Restore original data
        config.config_data["fieldnames"] = original_fieldnames


class TestConfigValidation:
    """Test cases for configuration validation"""
    
    def test_missing_required_sections(self):
        """Test validation with missing required sections"""
        config = CrawlerConfig("default")
        # Remove a required section
        original_website = config.config_data.get("website", {})
        del config.config_data["website"]
        
        is_valid, errors = config.validate_config()
        assert not is_valid
        assert any("Missing required section: website" in error for error in errors)
        
        # Restore
        config.config_data["website"] = original_website
    
    def test_invalid_base_url(self):
        """Test validation with invalid base URL"""
        config = CrawlerConfig("default")
        original_url = config.config_data["website"]["base_url"]
        config.config_data["website"]["base_url"] = "invalid-url"
        
        is_valid, errors = config.validate_config()
        assert not is_valid
        assert any("must be a valid URL" in error for error in errors)
        
        # Restore
        config.config_data["website"]["base_url"] = original_url
    
    def test_missing_required_xpaths(self):
        """Test validation with missing required xpaths"""
        config = CrawlerConfig("default")
        original_xpath = config.config_data.get("xpath", {})
        if "company_name" in config.config_data["xpath"]:
            del config.config_data["xpath"]["company_name"]
        
        is_valid, errors = config.validate_config()
        assert not is_valid
        assert any("Missing required xpath: company_name" in error for error in errors)
        
        # Restore
        config.config_data["xpath"] = original_xpath
