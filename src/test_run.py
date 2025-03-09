import pytest
from unittest import mock
from pyspark.sql import SparkSession
from run import (
    setup_logger,
    initialize_output_directory,
    process_data,
    process_data_all,
    load_config
)
import os
import logging
import shutil
import yaml

# Initialize Spark for testing
spark = SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()

# Sample data for testing
sample_data = [{"description": "The president visited Asia"}, {"description": "Asia is a continent"}]
sample_df = spark.createDataFrame(sample_data)

# Test the logger setup
def test_setup_logger(tmp_path):
    log_file = tmp_path / "test.log"
    logger = setup_logger(str(log_file))
    assert isinstance(logger, logging.Logger)
    logger.info("Test log message")
    with open(log_file, "r") as f:
        logs = f.read()
    assert "Test log message" in logs

# Test output directory initialization
def test_initialize_output_directory(tmp_path):
    output_dir = str(tmp_path / "output")
    result = initialize_output_directory(output_dir, logging.getLogger("test_logger"))
    assert os.path.exists(result)

# Test processing specific word counts
def test_process_data(tmp_path):
    output_dir = str(tmp_path / "output")
    os.makedirs(output_dir, exist_ok=True)
    logger = logging.getLogger("test_logger")
    specific_words = ["president", "Asia"]
    process_data(sample_df, output_dir + "/", specific_words, logger)
    parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
    assert len(parquet_files) > 0

# Test processing all word counts
def test_process_data_all(tmp_path):
    output_dir = str(tmp_path / "output")
    os.makedirs(output_dir, exist_ok=True)
    logger = logging.getLogger("test_logger")
    process_data_all(sample_df, output_dir + "/", logger)
    parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
    assert len(parquet_files) > 0

# Test loading config
def test_load_config(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_data = {"specific_words": ["president", "Asia"]}
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)
    config = load_config(str(config_path))
    assert config["specific_words"] == ["president", "Asia"]

