#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SerenaMCP Initialization Script for Restaurant Scraper
"""

import os
import sys
import json
import logging
import subprocess
from pathlib import Path

class SerenaInit:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.config_path = self.project_root / ".serena" / "config.json"
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Set up logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def load_config(self):
        """Load SerenaMCP configuration"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Configuration file not found")
            return None
    
    def check_python_version(self):
        """Check Python version"""
        version = sys.version_info
        self.logger.info(f"Python version: {version.major}.{version.minor}.{version.micro}")
        
        if version.major < 3 or (version.major == 3 and version.minor < 8):
            self.logger.error("Python 3.8+ is required")
            return False
        return True
    
    def check_dependencies(self):
        """Check if all dependencies are installed"""
        try:
            import requests
            import bs4
            import pandas
            import openpyxl
            import lxml
            self.logger.info("✅ All dependencies are installed")
            return True
        except ImportError as e:
            self.logger.error(f"Missing dependency: {e}")
            return False
    
    def check_files(self, config):
        """Check if all required files exist"""
        all_files_exist = True
        
        # Check main file
        main_file = self.project_root / config['files']['main']
        if not main_file.exists():
            self.logger.error(f"Main file not found: {main_file}")
            all_files_exist = False
        
        # Check modules
        for module in config['files']['modules']:
            module_path = self.project_root / module
            if not module_path.exists():
                self.logger.error(f"Module not found: {module_path}")
                all_files_exist = False
        
        if all_files_exist:
            self.logger.info("✅ All required files are present")
        
        return all_files_exist
    
    def create_sample_env(self):
        """Create a sample environment file"""
        env_path = self.project_root / ".env.sample"
        env_content = """# Sample environment file for Restaurant Scraper
# Copy this file to .env and fill in your values

# HotPepper API Key (optional)
# Get your API key from: https://webservice.recruit.co.jp/
HOTPEPPER_API_KEY=

# Default settings
DEFAULT_AREA=東京都
MAX_RESTAURANTS_PER_AREA=100
OUTPUT_FILENAME=restaurant_list.xlsx

# Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
"""
        
        if not env_path.exists():
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(env_content)
            self.logger.info("✅ Created sample environment file: .env.sample")
    
    def run_test(self):
        """Run a simple test"""
        self.logger.info("Running basic functionality test...")
        
        try:
            # Test import
            sys.path.insert(0, str(self.project_root))
            from restaurant_scraper_app import RestaurantScraperApp
            
            # Create instance
            app = RestaurantScraperApp()
            self.logger.info("✅ Application initialized successfully")
            
            return True
        except Exception as e:
            self.logger.error(f"Test failed: {e}")
            return False
    
    def initialize(self):
        """Main initialization process"""
        self.logger.info("🚀 Starting SerenaMCP initialization...")
        
        # Load configuration
        config = self.load_config()
        if not config:
            return False
        
        self.logger.info(f"Project: {config['project']['name']} v{config['project']['version']}")
        
        # Check Python version
        if not self.check_python_version():
            return False
        
        # Check dependencies
        if not self.check_dependencies():
            self.logger.info("Run 'python3 setup.py' to install dependencies")
            return False
        
        # Check files
        if not self.check_files(config):
            return False
        
        # Create sample env file
        self.create_sample_env()
        
        # Run test
        if not self.run_test():
            return False
        
        self.logger.info("✨ SerenaMCP initialization completed successfully!")
        self.logger.info("\nAvailable commands:")
        for script_name, command in config['scripts'].items():
            self.logger.info(f"  {script_name}: {command}")
        
        return True

if __name__ == "__main__":
    init = SerenaInit()
    success = init.initialize()
    sys.exit(0 if success else 1)