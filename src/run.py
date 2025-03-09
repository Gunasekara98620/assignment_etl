import argparse
import logging
import os
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower
from datasets import load_dataset
import yaml
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark
sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")

spark = SparkSession.builder.appName("AGNewsProcessing").getOrCreate()
spark.sparkContext._jsc.hadoopConfiguration().set("fs.permissions.umask-mode", "022")

logger.info("Spark session initialized successfully.")

# Function to setup logger to write to a text file
def setup_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def load_dataset_to_spark(dataset_name: str, logger: logging.Logger):
    """
    Loads a dataset from Hugging Face and converts it to a PySpark DataFrame.

    Args:
        dataset_name (str): The name of the dataset to load.
        logger (logging.Logger): Logger for logging information.

    Returns:
        DataFrame: PySpark DataFrame containing the loaded dataset.

    Raises:
        Exception: If loading the dataset or converting it to PySpark DataFrame fails.
    """
    try:
        logger.info(f"Loading dataset {dataset_name}...")
        dataset = load_dataset(dataset_name, split="test")  # use test dataset only
        logger.info("Dataset loaded successfully.")
        df = dataset.to_pandas()

        logger.info("Converting to PySpark DataFrame...")
        spark_df = spark.createDataFrame(df)
        logger.info("PySpark DataFrame created successfully.")
        return spark_df

    except Exception as e:
        logger.error("Failed to load or convert dataset.", exc_info=True)
        spark.stop()
        exit(1)



def initialize_output_directory(output_dir: str, logger: logging.Logger) -> str:
    """
    Initializes the output directory where the processed files will be saved.

    Args:
        output_dir (str): Directory path for saving the output files.
        logger (logging.Logger): Logger for logging information.

    Returns:
        str: The path of the initialized output directory.

    Raises:
        Exception: If the directory creation fails.
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory '{output_dir}' is ready.")
        return output_dir
    except Exception as e:
        logger.error("Failed to initialize output directory.", exc_info=True)
        spark.stop()
        exit(1)



def process_data(spark_df, output_dir: str, specific_words: List[str], logger: logging.Logger) -> None:
    """
    Processes specific word counts from a PySpark DataFrame and saves the result.

    Args:
        spark_df (DataFrame): The PySpark DataFrame to process.
        output_dir (str): Directory path to save the output.
        specific_words (List[str]): List of words to count in the dataset.
        logger (logging.Logger): Logger for logging information.

    Raises:
        Exception: If processing specific word counts fails.
    """
    # Add file handler to save logs to Data_processed.txt

    try:
        logger.info("Processing specific word counts...")
        current_date = datetime.now().strftime("%Y%m%d")

        words_df = spark_df.select(explode(split(col("description"), r"\s+")).alias("word"))

        # Count specific words
        specific_word_counts_nocasefilter = words_df.filter(
            lower(col("word")).isin([w.lower() for w in specific_words])
        ).groupBy("word").count()

        specific_word_counts = specific_word_counts_nocasefilter.filter(
            col("word").isin(specific_words)
        )

        specific_word_counts.show()

        # Save specific word counts
        output_path = f"{output_dir}word_count_{current_date}.parquet"
        specific_word_counts.write.mode("overwrite").parquet(output_path)
        logger.info(f"Specific word counts saved to {output_path}.")
    except Exception as e:
        logger.error("Failed to process specific_word_counts.", exc_info=True)
        spark.stop()
        exit(1)



def process_data_all(spark_df, output_dir: str, logger: logging.Logger) -> None:
    """
    Processes all word counts from a PySpark DataFrame and saves the result.

    Args:
        spark_df (DataFrame): The PySpark DataFrame to process.
        output_dir (str): Directory path to save the output.
        logger (logging.Logger): Logger for logging information.

    Raises:
        Exception: If processing all word counts fails.
    """
    try:
        logger.info("Processing all word counts...")
        current_date = datetime.now().strftime("%Y%m%d")

        words_df = spark_df.select(explode(split(col("description"), r"\s+")).alias("word"))
        all_word_counts = words_df.groupBy("word").count().orderBy(col("count").desc())

        all_word_counts.show()

        # Save all unique word counts
        output_path = f"{output_dir}word_count_all_{current_date}.parquet"
        all_word_counts.write.mode("overwrite").parquet(output_path)
        logger.info(f"All word counts saved to {output_path}.")
    except Exception as e:
        logger.error("Failed to process all_word_counts.", exc_info=True)
        spark.stop()
        exit(1)



def load_config(config_path: str) -> Dict[str, any]:
    """
    Loads configuration data from a YAML file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: The loaded configuration data.

    Raises:
        Exception: If loading the configuration file fails.
    """
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logger.info("Configuration file loaded successfully.")
        return config
    except Exception as e:
        logger.error("Failed to load config file.", exc_info=True)
        exit(1)



# Main execution with argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process AG News dataset with PySpark.")
    parser.add_argument("function", choices=["process_data", "process_data_all"], help="Function to run")
    parser.add_argument("-cfg", "--config", required=True, help="Path to the config file.")
    parser.add_argument("-dataset", "--dataset", required=True, help="Dataset name to load.")
    parser.add_argument("-dirout", "--output_dir", required=True, help="Directory for output files.")

    args = parser.parse_args()

    # Initialize logger based on function
    os.makedirs("../log", exist_ok=True)       # Ensure the 'log' directory exists
    log_file = os.path.join("../log", "Data_processed.txt" if args.function == "process_data" else "Data_processed_all.txt")
    logger = setup_logger(log_file)

    logger.info("Starting the data processing pipeline.")

    # Load configuration and dataset
    config = load_config(args.config)
    specific_words = config.get("specific_words", ["president", "the", "Asia"])

    spark_df = load_dataset_to_spark(args.dataset, logger)
    output_dir = initialize_output_directory(args.output_dir, logger)

    # Call the appropriate function based on input
    if args.function == "process_data":
        process_data(spark_df, output_dir, specific_words, logger)
    elif args.function == "process_data_all":
        process_data_all(spark_df, output_dir, logger)

    spark.stop()
    logger.info("Spark session stopped.")
